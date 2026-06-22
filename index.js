"use strict";
require("dotenv").config();

/**
 * ECL Profile Stats Refresh Worker
 * ──────────────────────────────────────────────────────────────────────────
 * Standalone Railway service. Connects directly to the same Postgres DB as
 * the ECL website and re-fetches lzyumi / ecl.gg stats for every verified
 * player whose data is older than REFRESH_STALE_HOURS hours.
 *
 * Runs a batch every POLL_INTERVAL_MINUTES minutes so it never hammers
 * lzyumi and never hits Vercel's serverless timeout limit.
 *
 * Required env vars (set in Railway):
 *   DATABASE_URL       – same Postgres connection string as the website
 *   ECL_SITE_URL       – website base URL (e.g. https://eclchina.lol)
 *   ECL_JOB_SECRET     – must match ECL_JOB_SECRET on Vercel
 *
 * Optional env vars:
 *   POLL_INTERVAL_MINUTES    – how often to check for stale profiles (default 20)
 *   BATCH_SIZE               – profiles per poll cycle (default 5)
 *   REFRESH_STALE_HOURS      – hours before a profile is considered stale (default 12)
 *   PORT                     – HTTP port for Railway health checks (default 3000)
 *   DB_SSL                   – set to "false" to disable Postgres SSL
 */

const http = require("node:http");
const axios = require("axios");
const { Pool } = require("pg");

// ─────────────────────────────────────────────────────────────────────────────
// Config
// ─────────────────────────────────────────────────────────────────────────────

const DATABASE_URL   = process.env.DATABASE_URL;
const ECL_SITE_URL   = (process.env.ECL_SITE_URL || "https://eclchina.lol").replace(/\/$/, "");
const ECL_JOB_SECRET = process.env.ECL_JOB_SECRET;

const POLL_INTERVAL_MS =
  parseFloat(process.env.POLL_INTERVAL_MINUTES || "20") * 60 * 1000;
const BATCH_SIZE = parseInt(process.env.BATCH_SIZE || "5", 10);
const REFRESH_STALE_MS =
  parseFloat(process.env.REFRESH_STALE_HOURS || "12") * 60 * 60 * 1000;
const LZYUMI_TIMEOUT_MS = 20000;     // 20 s per proxy call (Vercel → lzyumi)
const DELAY_BETWEEN_MS = 2000;       // rate-limit pause between profiles
const HTTP_PORT = parseInt(process.env.PORT || "3000", 10);

// ─────────────────────────────────────────────────────────────────────────────
// Proxy helpers – all lzyumi calls go through the Vercel website's
// /api/lzyumi-proxy endpoint so they originate from a Vercel IP.
// ─────────────────────────────────────────────────────────────────────────────

async function proxyGet(action, params) {
  const url = new URL(`${ECL_SITE_URL}/api/lzyumi-proxy`);
  url.searchParams.set("action", action);
  for (const [k, v] of Object.entries(params)) {
    if (v != null && v !== "") url.searchParams.set(k, String(v));
  }
  const res = await axios.get(url.toString(), {
    timeout: LZYUMI_TIMEOUT_MS,
    headers: { "x-ecl-job-secret": ECL_JOB_SECRET },
  });
  if (res.data?.error) throw new Error(`Proxy error: ${res.data.error}`);
  return res.data;
}

async function lookupProfile(riotName, areaId) {
  return proxyGet("lookup", { nickname: riotName, areaId });
}

async function fetchRankedGames(riotName, areaId) {
  const result = await proxyGet("ranked", { nickname: riotName, areaId });
  return {
    soloGames: Array.isArray(result?.soloGames) ? result.soloGames : [],
    flexGames: Array.isArray(result?.flexGames) ? result.flexGames : [],
  };
}

async function fetchRecentStat(openId, areaId) {
  return proxyGet("recent", { openId, areaId });
}

// Strip Unicode bidirectional control characters that can be embedded in user-supplied
// Riot names/tags. Keeps intentional chars like U+FFA0 (Halfwidth Hangul Filler).
function sanitize(str) {
  if (!str) return str;
  return str.replace(/[\u2066\u2067\u2068\u2069\u202A-\u202E\u200B\u200C\u200D\uFEFF]/g, "").trim();
}

function normalizeRiotId(name, tag) {
  return `${sanitize(name)}#${sanitize(tag)}`.toLowerCase();
}

async function lookupProfileWithFallback(riotName, riotTag, areaId) {
  // Sanitize first – strip any invisible control chars from stored values
  // Also strip any leading # from the tag (some DB entries store e.g. "#36614")
  const cleanName = sanitize(riotName);
  const cleanTag  = sanitize(riotTag)?.replace(/^#+/, "");

  // Mirror the website's lookupLzyumiIdentity: try plain name first, then name#tag.
  // lzyumi often resolves on the plain name but not the full Riot ID.
  const candidates = Array.from(
    new Set([cleanName, cleanTag ? `${cleanName}#${cleanTag}` : null].filter(Boolean))
  );

  let fallback = null;
  for (const candidate of candidates) {
    const raw = await lookupProfile(candidate, areaId);
    const resolvedName = raw?.battleInfo?.nameInfoNew;
    const openId = raw?.battleInfo?.openId;

    if (!raw?.battleInfo || !openId || !resolvedName) {
      console.warn(`[refresh] ⚠ lookup "${candidate}" → no battleInfo. Full response:`, JSON.stringify(raw));
      fallback = fallback ?? { found: false, raw };
      continue;
    }

    // If we have a tag, verify it matches
    if (cleanTag) {
      const resolved = resolvedName.trim().toLowerCase();
      const expected = normalizeRiotId(cleanName, cleanTag);
      if (resolved !== expected) {
        fallback = fallback ?? { found: false, raw, mismatch: `expected "${expected}", got "${resolved}"` };
        continue;
      }
    }

    return { found: true, raw };
  }

  return fallback ?? { found: false, raw: null };
}

// ─────────────────────────────────────────────────────────────────────────────
// Refresh a single profile
// ─────────────────────────────────────────────────────────────────────────────

async function refreshProfile(pool, profile) {
  const { id, displayName, riotName, riotTag, openId: storedOpenId, chinaServerId } = profile;
  const cleanRiotTag = riotTag?.replace(/^#+/, "");
  const lookupName = cleanRiotTag ? `${riotName}#${cleanRiotTag}` : riotName;

  // Mirror website's lookupLzyumiIdentity: try plain name first, then name#tag
  const [lookupResult, rankedResult] = await Promise.allSettled([
    lookupProfileWithFallback(riotName, riotTag, chinaServerId),
    fetchRankedGames(lookupName, chinaServerId),
  ]);

  // Both hard-failed – bump timestamp only so we don't retry in the next poll
  if (lookupResult.status === "rejected" && rankedResult.status === "rejected") {
    await pool.query(
      `UPDATE "AccountProfile" SET "lzyumiLastLookupAt" = NOW() WHERE id = $1`,
      [id],
    );
    return { ok: false, reason: "all_lzyumi_calls_failed", detail: lookupResult.reason?.message };
  }

  // ── Resolve openId + validate identity ────────────────────────────────────
  let openId = storedOpenId || null;
  let validRawProfile = null;

  if (lookupResult.status === "fulfilled") {
    const { found, raw, mismatch } = lookupResult.value;
    if (found && raw) {
      validRawProfile = raw;
      const freshOpenId = raw?.battleInfo?.openId;
      if (freshOpenId) openId = freshOpenId;
    } else if (mismatch) {
      console.warn(`[refresh] ⚠ ${displayName}: identity mismatch – ${mismatch}`);
    } else {
      console.warn(`[refresh] ⚠ ${displayName}: not found on lzyumi (code=${raw?.code})`);
    }
  } else {
    console.warn(`[refresh] ⚠ ${displayName}: lookup threw – ${lookupResult.reason?.message}`);
  }

  if (rankedResult.status === "fulfilled") {
    const { soloGames, flexGames } = rankedResult.value;
    if (soloGames.length === 0 && flexGames.length === 0) {
      console.warn(`[refresh] ⚠ ${displayName}: ranked games returned empty arrays`);
    }
  } else {
    console.warn(`[refresh] ⚠ ${displayName}: ranked games rejected – ${rankedResult.reason?.message}`);
  }

  // ── Fetch recent stat (needs openId) ──────────────────────────────────────
  let recentStat = null;
  if (openId) {
    try {
      const rs = await fetchRecentStat(openId, chinaServerId);
      if (rs?.data) recentStat = rs;
    } catch (err) {
      console.warn(`[refresh] Recent stat failed for ${displayName}: ${err.message}`);
    }
  }

  // ── Build UPDATE ──────────────────────────────────────────────────────────
  const sets = ['"lzyumiLastLookupAt" = NOW()'];
  const values = [];
  let p = 1;

  if (openId && openId !== storedOpenId) {
    sets.push(`"openId" = $${p++}`);
    values.push(openId);
  }

  if (validRawProfile) {
    sets.push(`"lzyumiRawProfile" = $${p++}`);
    values.push(JSON.stringify(validRawProfile));

    // Extract rank tier from mapOneInfoList
    const mapRows = Array.isArray(validRawProfile.battleInfo?.mapOneInfoList)
      ? validRawProfile.battleInfo.mapOneInfoList.filter((r) => r.tier && r.tier !== "-")
      : [];
    if (mapRows.length > 0) {
      // Prefer Solo/Duo (单双排)
      const rankRow = mapRows.find((r) => r.type?.includes("\u5355\u53cc\u6392")) ?? mapRows[0];
      sets.push(`"currentRank" = $${p++}`);
      values.push(rankRow?.tier ?? null);
    }
  }

  if (
    rankedResult.status === "fulfilled" &&
    (rankedResult.value.soloGames.length > 0 || rankedResult.value.flexGames.length > 0)
  ) {
    sets.push(`"lzyumiRankedGames" = $${p++}`);
    values.push(JSON.stringify(rankedResult.value));
  }

  if (recentStat) {
    sets.push(`"lzyumiRecentStat" = $${p++}`);
    values.push(JSON.stringify(recentStat));
  }

  values.push(id);
  await pool.query(
    `UPDATE "AccountProfile" SET ${sets.join(", ")} WHERE id = $${p}`,
    values,
  );

  return {
    ok: true,
    gotProfile: !!validRawProfile,
    gotRecentStat: !!recentStat,
    rankedGames:
      rankedResult.status === "fulfilled"
        ? rankedResult.value.soloGames.length + rankedResult.value.flexGames.length
        : 0,
  };
}

// ─────────────────────────────────────────────────────────────────────────────
// Batch runner
// ─────────────────────────────────────────────────────────────────────────────

async function runBatch(pool) {
  const staleBefore = new Date(Date.now() - REFRESH_STALE_MS);

  const { rows: profiles } = await pool.query(
    `SELECT id, "displayName", "riotName", "riotTag", "openId", "chinaServerId"
       FROM "AccountProfile"
      WHERE "accountStatus"      = 'ACTIVE'
        AND "verificationStatus" = 'VERIFIED'
        AND "riotName"           != ''
        AND "chinaServerId"       IS NOT NULL
        AND (
          "lzyumiLastLookupAt" IS NULL
          OR "lzyumiLastLookupAt" < $1
          OR "lzyumiRawProfile"  IS NULL
        )
      ORDER BY
        CASE WHEN "lzyumiRawProfile" IS NULL THEN 0 ELSE 1 END ASC,
        "lzyumiLastLookupAt" ASC NULLS FIRST
      LIMIT $2`,
    [staleBefore, BATCH_SIZE],
  );

  if (profiles.length === 0) {
    console.log("[refresh] All profiles are fresh – nothing to do.");
    return { checked: 0, refreshed: 0, failed: 0 };
  }

  console.log(`[refresh] Processing ${profiles.length} stale profile(s)...`);
  let refreshed = 0;
  let failed = 0;

  for (const profile of profiles) {
    try {
      const result = await refreshProfile(pool, profile);
      if (result.ok) {
        console.log(
          `[refresh] ✓ ${profile.displayName} ` +
            `(${profile.riotName}#${profile.riotTag || ""}) ` +
            `profile=${result.gotProfile} stat=${result.gotRecentStat} ranked=${result.rankedGames}`,
        );
        refreshed++;
      } else {
        console.log(`[refresh] ✗ ${profile.displayName}: ${result.reason} ${result.detail || ""}`);
        failed++;
      }
    } catch (err) {
      console.error(`[refresh] ✗ ${profile.displayName} threw:`, err.message);
      failed++;
      // Bump timestamp so we don't retry this in the very next poll
      try {
        await pool.query(
          `UPDATE "AccountProfile" SET "lzyumiLastLookupAt" = NOW() WHERE id = $1`,
          [profile.id],
        );
      } catch {
        // suppress secondary error
      }
    }

    // Rate-limit: don't hammer lzyumi
    await new Promise((r) => setTimeout(r, DELAY_BETWEEN_MS));
  }

  console.log(`[refresh] Batch done – refreshed=${refreshed} failed=${failed}`);
  return { checked: profiles.length, refreshed, failed };
}

// ─────────────────────────────────────────────────────────────────────────────
// Lock-guard (prevents overlapping batches if a poll fires while one is running)
// ─────────────────────────────────────────────────────────────────────────────

let isRunning = false;
let lastRunAt = null;
let lastResult = null;

async function runWithLock(pool) {
  if (isRunning) {
    console.log("[refresh] Previous batch still running – skipping this tick.");
    return { skipped: true };
  }
  isRunning = true;
  try {
    lastRunAt = new Date().toISOString();
    lastResult = await runBatch(pool);
    return lastResult;
  } catch (err) {
    console.error("[refresh] Unhandled batch error:", err.message);
    lastResult = { error: err.message };
    return lastResult;
  } finally {
    isRunning = false;
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// HTTP server – Railway health check + manual trigger
// ─────────────────────────────────────────────────────────────────────────────

function startHttpServer(pool) {
  const server = http.createServer(async (req, res) => {
    res.setHeader("Content-Type", "application/json");

    // POST /refresh → manual trigger
    if (req.method === "POST" && req.url === "/refresh") {
      console.log("[refresh] Manual trigger via HTTP POST /refresh");
      runWithLock(pool).then((r) => console.log("[refresh] Manual run complete:", r));
      res.writeHead(202);
      res.end(JSON.stringify({ ok: true, message: "Refresh triggered." }));
      return;
    }

    // GET / → status / health check
    res.writeHead(200);
    res.end(
      JSON.stringify({
        ok: true,
        service: "ecl-refresh-worker",
        isRunning,
        lastRunAt,
        lastResult,
        config: {
          pollIntervalMinutes: POLL_INTERVAL_MS / 60000,
          batchSize: BATCH_SIZE,
          staleWindowHours: REFRESH_STALE_MS / 3600000,
        },
      }),
    );
  });

  server.listen(HTTP_PORT, () => {
    console.log(`[refresh] HTTP server listening on port ${HTTP_PORT}`);
    console.log(`[refresh]   Health check : GET  http://localhost:${HTTP_PORT}/`);
    console.log(`[refresh]   Manual run   : POST http://localhost:${HTTP_PORT}/refresh`);
  });
}

// ─────────────────────────────────────────────────────────────────────────────
// Boot
// ─────────────────────────────────────────────────────────────────────────────

if (!DATABASE_URL) {
  console.error("[refresh] ✗ DATABASE_URL is not set. Add it to Railway env vars.");
  process.exit(1);
}

if (!ECL_JOB_SECRET) {
  console.error("[refresh] ✗ ECL_JOB_SECRET is not set. Add it to Railway env vars (must match Vercel).");
  process.exit(1);
}

console.log("[refresh] ECL Refresh Worker starting...");
console.log(`[refresh]   Proxy          : ${ECL_SITE_URL}/api/lzyumi-proxy`);
console.log(`[refresh]   Poll interval  : every ${POLL_INTERVAL_MS / 60000} minutes`);
console.log(`[refresh]   Batch size     : ${BATCH_SIZE} profiles`);
console.log(`[refresh]   Stale window   : ${REFRESH_STALE_MS / 3600000} hours`);
console.log(`[refresh]   lzyumi timeout : ${LZYUMI_TIMEOUT_MS / 1000} seconds`);

const sslEnabled = process.env.DB_SSL !== "false";
const pool = new Pool({
  connectionString: DATABASE_URL,
  ssl: sslEnabled ? { rejectUnauthorized: false } : false,
  max: 3,
  idleTimeoutMillis: 30000,
});

pool.on("error", (err) => {
  console.error("[refresh] Postgres pool error:", err.message);
});

// Test DB connection on startup
pool.query("SELECT 1").then(() => {
  console.log("[refresh] ✓ Postgres connected.");
}).catch((err) => {
  console.error("[refresh] ✗ Postgres connection failed:", err.message);
  process.exit(1);
});

startHttpServer(pool);

// Initial run – wait 20 s so Postgres connection fully settles
setTimeout(() => {
  console.log("[refresh] Running initial batch...");
  runWithLock(pool);
}, 20000);

// Recurring poll
setInterval(() => runWithLock(pool), POLL_INTERVAL_MS);
