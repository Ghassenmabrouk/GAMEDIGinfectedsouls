'use strict';

const { GameDig } = require('gamedig');
const { createClient } = require('@supabase/supabase-js');

// ── Config ────────────────────────────────────────────────────────────────────
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const SERVER_HOST = process.env.CS_HOST || '151.247.205.250';
const SERVER_PORT = parseInt(process.env.CS_PORT || '27042', 10);
const POLL_INTERVAL_MS = parseInt(process.env.POLL_INTERVAL_MS || '60000', 10);

if (!SUPABASE_URL || !SUPABASE_SERVICE_KEY) {
  console.error('Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY env vars');
  process.exit(1);
}

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

// ── Poll ──────────────────────────────────────────────────────────────────────
async function poll() {
  let state;
  try {
    state = await GameDig.query({
      type: 'counterstrike16',
      host: SERVER_HOST,
      port: SERVER_PORT,
      socketTimeout: 5000,
    });
  } catch (err) {
    console.error(`[${new Date().toISOString()}] Query failed:`, err.message);
    return;
  }

  const now = new Date().toISOString();
  console.log(`[${now}] ${state.name} | ${state.map} | ${state.players.length} players`);

  // Fetch existing stats for all players seen this poll
  const names = state.players.map(p => p.name).filter(Boolean);
  if (names.length === 0) return;

  const { data: existing, error: fetchErr } = await supabase
    .from('player_stats')
    .select('*')
    .in('name', names);

  if (fetchErr) {
    console.error('Supabase fetch error:', fetchErr.message);
    return;
  }

  const existingMap = {};
  (existing || []).forEach(row => { existingMap[row.name] = row; });

  const upserts = [];
  const sessionRows = [];

  for (const player of state.players) {
    const name = player.name;
    if (!name) continue;

    const rawScore = Math.round(player.raw?.score ?? 0);
    const rawTime  = Math.round(player.raw?.time  ?? 0);  // seconds in current map/session
    const prev     = existingMap[name];

    let timeDelta  = 0;
    let scoreDelta = 0;

    if (prev) {
      const lastSeenMs = prev.last_seen ? new Date(prev.last_seen).getTime() : 0;
      const gapMs      = Date.now() - lastSeenMs;
      const freshSession = gapMs < 90 * 1000; // seen within last 1.5 poll cycles

      if (freshSession) {
        // Same continuous session — take the increase since last poll
        timeDelta  = Math.max(0, rawTime  - prev.last_raw_time);
        scoreDelta = Math.max(0, rawScore - prev.last_raw_score);
      } else {
        // Player rejoined or server restarted — treat current values as a new segment
        timeDelta  = rawTime;
        scoreDelta = rawScore;
      }
    } else {
      // First time we've ever seen this player
      timeDelta  = rawTime;
      scoreDelta = rawScore;
    }

    upserts.push({
      name,
      total_time:      (prev?.total_time  || 0) + timeDelta,
      total_score:     (prev?.total_score || 0) + scoreDelta,
      last_seen:       now,
      last_raw_time:   rawTime,
      last_raw_score:  rawScore,
      updated_at:      now,
    });

    if (timeDelta > 0 || scoreDelta > 0) {
      sessionRows.push({
        player_name:  name,
        time_delta:   timeDelta,
        score_delta:  scoreDelta,
        recorded_at:  now,
      });
    }

    console.log(`  ${name}: +${timeDelta}s / +${scoreDelta} score`);
  }

  // Upsert player stats
  const { error: upsertErr } = await supabase
    .from('player_stats')
    .upsert(upserts, { onConflict: 'name' });

  if (upsertErr) console.error('Upsert error:', upsertErr.message);

  // Insert session rows
  if (sessionRows.length > 0) {
    const { error: sessionErr } = await supabase
      .from('player_sessions')
      .insert(sessionRows);

    if (sessionErr) console.error('Session insert error:', sessionErr.message);
  }

  // Prune sessions older than 30 days
  const thirtyDaysAgo = new Date(Date.now() - 30 * 24 * 60 * 60 * 1000).toISOString();
  await supabase.from('player_sessions').delete().lt('recorded_at', thirtyDaysAgo);
}

// ── Start ─────────────────────────────────────────────────────────────────────
console.log(`Poller starting — ${SERVER_HOST}:${SERVER_PORT} every ${POLL_INTERVAL_MS / 1000}s`);
poll();
setInterval(poll, POLL_INTERVAL_MS);
