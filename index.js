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
 *
 * Optional env vars:
 *   LZYUMI_BASE_URL          – ecl.gg API base (has a default)
 *   POLL_INTERVAL_MINUTES    – how often to check for stale profiles (default 20)
 *   BATCH_SIZE               – profiles per poll cycle (default 5)
 *   REFRESH_STALE_HOURS      – hours before a profile is considered stale (default 12)
 *   PORT                     – HTTP port for Railway health checks (default 3000)
 *   DB_SSL                   – set to "false" to disable Postgres SSL
 */

const crypto = require("node:crypto");
const http = require("node:http");
const axios = require("axios");
const { Pool } = require("pg");

// ─────────────────────────────────────────────────────────────────────────────
// Config
// ─────────────────────────────────────────────────────────────────────────────

const DATABASE_URL = process.env.DATABASE_URL;
const LZYUMI_BASE_URL =
  process.env.LZYUMI_BASE_URL || "https://a.2025lol.top/lzyumi/lol/info";

const POLL_INTERVAL_MS =
  parseFloat(process.env.POLL_INTERVAL_MINUTES || "20") * 60 * 1000;
const BATCH_SIZE = parseInt(process.env.BATCH_SIZE || "5", 10);
const REFRESH_STALE_MS =
  parseFloat(process.env.REFRESH_STALE_HOURS || "12") * 60 * 60 * 1000;
const LZYUMI_TIMEOUT_MS = 15000;     // 15 s per lzyumi call – hard abort
const DELAY_BETWEEN_MS = 3500;       // rate-limit pause between profiles
const HTTP_PORT = parseInt(process.env.PORT || "3000", 10);

// China servers – must match ecl-split-one/lib/lzyumi.ts exactly
const CHINA_SERVERS = [
  { id: 1,  name: "\u827e\u6b27\u5c3c\u4e9a" },
  { id: 14, name: "\u9ed1\u8272\u73ab\u7470" },
  { id: 31, name: "\u5ce1\u8c37\u4e4b\u5dc5" },
  { id: 30, name: "\u7537\u7235\u9886\u57df" },
  { id: 3,  name: "\u7956\u5b89" },
  { id: 4,  name: "\u8bfa\u514b\u8428\u65af" },
  { id: 16, name: "\u6055\u745e\u739b" },
];

// ─────────────────────────────────────────────────────────────────────────────
// lzyumi API helpers (ported from ecl-split-one/lib/lzyumi.ts)
// ─────────────────────────────────────────────────────────────────────────────

function getChinaServer(areaId) {
  return CHINA_SERVERS.find((s) => s.id === Number(areaId)) ?? CHINA_SERVERS[0];
}

function createSignature() {
  const now = new Date();
  const month   = String(now.getMonth() + 1);
  const day     = String(now.getDate());
  const hours   = String(now.getHours());
  const minutes = String(now.getMinutes());
  const seconds = String(now.getSeconds());

  const signSource =
    `dld${month.padStart(2,"0")}o${day.padStart(2,"0")}` +
    `u${hours.padStart(2,"0")}d${minutes.padStart(2,"0")}` +
    `o${seconds.padStart(2,"0")}dld`;

  const lzyumiSign = crypto.createHash("md5").update(signSource).digest("hex");
  const signStr =
    `${month}${day}${hours}${minutes}${seconds}` +
    `${month.length * 3}${day.length * 3}${hours.length * 3}` +
    `${minutes.length * 3}${seconds.length * 3}`;

  return { lzyumiSign, signStr };
}

async function lzyumiGet(url) {
  const res = await axios.get(url, {
    timeout: LZYUMI_TIMEOUT_MS,
    headers: {
      Accept: "application/json, text/plain, */*",
      Referer: "https://a.2025lol.top/",
      "User-Agent":
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    },
  });
  return res.data;
}

function lookupUrl(riotName, areaId, filter = 1) {
  const server = getChinaServer(areaId);
  const { lzyumiSign, signStr } = createSignature();
  const p = new URLSearchParams({
    nickname: riotName.trim(),
    allCount: "10",
    areaId: String(server.id),
    areaName: server.name,
    seleMe: "1",
    filter: String(filter),
    openId: "",
    lzyumiSign,
    signStr,
  });
  return `${LZYUMI_BASE_URL}?${p}`;
}

async function lookupProfile(riotName, areaId) {
  return lzyumiGet(lookupUrl(riotName, areaId, 1));
}

async function fetchRankedGames(riotName, areaId) {
  const [solo, flex] = await Promise.allSettled([
    lzyumiGet(lookupUrl(riotName, areaId, 2)), // Solo/Duo
    lzyumiGet(lookupUrl(riotName, areaId, 3)), // Flex
  ]);
  return {
    soloGames: solo.status === "fulfilled" && Array.isArray(solo.value?.data) ? solo.value.data : [],
    flexGames: flex.status === "fulfilled" && Array.isArray(flex.value?.data) ? flex.value.data : [],
  };
}

async function fetchRecentStat(openId, areaId) {
  const { lzyumiSign, signStr } = createSignature();
  const p = new URLSearchParams({ openId, areaId: String(areaId), lzyumiSign, signStr });
  return lzyumiGet(`${LZYUMI_BASE_URL}/getPlayerRecentStat?${p}`);
}

function normalizeRiotId(name, tag) {
  return `${name.trim()}#${tag.trim()}`.toLowerCase();
}

// ─────────────────────────────────────────────────────────────────────────────
// Refresh a single profile
// ─────────────────────────────────────────────────────────────────────────────

async function refreshProfile(pool, profile) {
  const { id, displayName, riotName, riotTag, openId: storedOpenId, chinaServerId } = profile;
  const lookupName = riotTag ? `${riotName}#${riotTag}` : riotName;

  // Fire the two most important calls in parallel
  const [rawResult, rankedResult] = await Promise.allSettled([
    lookupProfile(lookupName, chinaServerId),
    fetchRankedGames(lookupName, chinaServerId),
  ]);

  // Both hard-failed – bump timestamp only so we don't retry in the next poll
  if (rawResult.status === "rejected" && rankedResult.status === "rejected") {
    await pool.query(
      `UPDATE "AccountProfile" SET "lzyumiLastLookupAt" = NOW() WHERE id = $1`,
      [id],
    );
    return { ok: false, reason: "all_lzyumi_calls_failed", detail: rawResult.reason?.message };
  }

  // ── Resolve openId + validate identity ────────────────────────────────────
  let openId = storedOpenId || null;
  let validRawProfile = null;

  if (rawResult.status === "fulfilled") {
    const raw = rawResult.value;
    const freshOpenId = raw?.battleInfo?.openId;
    if (freshOpenId) openId = freshOpenId;

    if (riotTag && raw?.battleInfo?.nameInfoNew) {
      // Verify lzyumi returned the correct account
      const resolved = raw.battleInfo.nameInfoNew.trim().toLowerCase();
      const expected = normalizeRiotId(riotName, riotTag);
      if (resolved === expected) {
        validRawProfile = raw;
      } else {
        console.warn(`[refresh] ⚠ Mismatch for ${displayName}: expected "${expected}", got "${resolved}"`);
      }
    } else if (raw?.battleInfo?.openId) {
      // No tag to verify against – accept as-is
      validRawProfile = raw;
    }
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
      WHERE "accountStatus"    = 'ACTIVE'
        AND "verificationStatus" = 'VERIFIED'
        AND "riotName"          != ''
        AND "chinaServerId"      IS NOT NULL
        AND ("lzyumiLastLookupAt" IS NULL OR "lzyumiLastLookupAt" < $1)
      ORDER BY "lzyumiLastLookupAt" ASC NULLS FIRST
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
  console.error(
    "[refresh] ✗ DATABASE_URL is not set.\n" +
      "          Add it to the Railway environment variables and redeploy.",
  );
  process.exit(1);
}

console.log("[refresh] ECL Refresh Worker starting...");
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
