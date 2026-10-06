import {EXTRACTION_INSTRUCTIONS, REPORT_SCHEMA, validateExtraction} from './schema.mjs';

const json = (value, status = 200) => new Response(JSON.stringify(value), {
  status, headers: {'Content-Type': 'application/json', 'Cache-Control': 'no-store'},
});
const timestamp = value => typeof value === 'string' && /^\d{10,12}\.\d{6}$/.test(value);
const localDate = ts => new Intl.DateTimeFormat('en-CA', {
  timeZone: 'America/Chicago', year: 'numeric', month: '2-digit', day: '2-digit',
}).format(new Date(Number(ts) * 1000));

async function digestHex(value) {
  return Array.from(new Uint8Array(await crypto.subtle.digest('SHA-256',
    new TextEncoder().encode(value))), b => b.toString(16).padStart(2, '0')).join('');
}
function equal(a, b) {
  if (a.length !== b.length) return false;
  let difference = 0;
  for (let i = 0; i < a.length; i++) difference |= a.charCodeAt(i) ^ b.charCodeAt(i);
  return difference === 0;
}
export async function verifySlack(request, raw, secret, now = Date.now()) {
  const ts = request.headers.get('x-slack-request-timestamp') || '';
  const signature = request.headers.get('x-slack-signature') || '';
  if (!secret || !/^\d+$/.test(ts) || Math.abs(now / 1000 - Number(ts)) > 300) return false;
  const key = await crypto.subtle.importKey('raw', new TextEncoder().encode(secret),
    {name: 'HMAC', hash: 'SHA-256'}, false, ['sign']);
  const signed = await crypto.subtle.sign('HMAC', key,
    new TextEncoder().encode(`v0:${ts}:${raw}`));
  const expected = 'v0=' + Array.from(new Uint8Array(signed),
    b => b.toString(16).padStart(2, '0')).join('');
  return equal(signature, expected);
}

// Do not send author IDs, attachments, or arbitrary channel history to the LLM.
export function normalizeEvent(payload, env) {
  if (payload.type !== 'event_callback' || payload.team_id !== env.SLACK_TEAM_ID ||
      typeof payload.event_id !== 'string' || payload.event_id.length > 100) return null;
  const e = payload.event;
  if (!e || e.type !== 'message' || e.channel !== env.SLACK_CHANNEL_ID) return null;
  const subtype = e.subtype || null;
  if (subtype && !['message_changed', 'message_deleted'].includes(subtype)) return null;
  const m = subtype === 'message_changed' ? e.message : e;
  if (!m || m.bot_id || m.app_id || m.subtype === 'bot_message' ||
      e.previous_message?.bot_id || e.previous_message?.app_id) return null;
  const deleted = subtype === 'message_deleted';
  const ts = deleted ? e.deleted_ts : m.ts;
  const revision = deleted ? e.event_ts : (m.edited?.ts || m.ts);
  if (!timestamp(ts) || !timestamp(revision) || (!deleted &&
      (typeof m.user !== 'string' || typeof m.text !== 'string' || m.text.length > 12000))) return null;
  const thread = m.thread_ts || e.previous_message?.thread_ts || ts;
  if (!timestamp(thread)) return null;
  return {event_id: payload.event_id, channel_id: e.channel, message_ts: ts,
    thread_ts: thread, revision_ts: revision, source_text: deleted ? '' : m.text,
    deleted: deleted ? 1 : 0, message_key: `${payload.team_id}:${e.channel}:${ts}`};
}

export function validateSnapshot(value) {
  if (!value || !/^\d{4}-\d{2}-\d{2}$/.test(value.target_date) ||
      new Date(value.target_date).toISOString().slice(0, 10) !== value.target_date ||
      !Array.isArray(value.events) || value.events.length > 1000) throw new Error('Invalid event snapshot');
  const ids = new Set();
  for (const e of value.events) {
    if (!e || typeof e.event_id !== 'string' || !/^\d+$/.test(e.event_id) || ids.has(e.event_id) ||
        typeof e.in_digest !== 'boolean' ||
        ['title', 'organizer_name', 'time_text', 'event_url'].some(k => typeof e[k] !== 'string' || e[k].length > 2000) ||
        (e.room_text !== null && (typeof e.room_text !== 'string' || e.room_text.length > 2000)))
      throw new Error('Invalid snapshot event');
    const url = new URL(e.event_url);
    if (url.protocol !== 'https:' || url.hostname !== 'kellogg.campusgroups.com') throw new Error('Invalid event URL');
    ids.add(e.event_id);
  }
  return value;
}

async function authorized(request, secret) {
  const provided = request.headers.get('Authorization') || '';
  return !!secret && equal(await digestHex(provided), await digestHex(`Bearer ${secret}`));
}

export async function handleRequest(request, env) {
  const path = new URL(request.url).pathname;
  if (path === '/health' && request.method === 'GET') return json({status: 'ok'});
  if (path.startsWith('/api/')) {
    if (!await authorized(request, env.FEEDBACK_API_TOKEN)) return json({error: 'Unauthorized'}, 401);
    if (path === '/api/events' && request.method === 'POST') {
      const raw = await request.text();
      if (raw.length > 1000000) return json({error: 'Snapshot too large'}, 413);
      let snapshot;
      try { snapshot = validateSnapshot(JSON.parse(raw)); }
      catch { return json({error: 'Invalid snapshot'}, 400); }
      // Atomic replacement also removes events cancelled between daily collections.
      await env.DB.batch([
        env.DB.prepare('DELETE FROM event_catalog WHERE target_date = ?').bind(snapshot.target_date),
        ...snapshot.events.map(e => env.DB.prepare(`INSERT INTO event_catalog
          (target_date, event_id, organizer_name, in_digest, event_json) VALUES (?, ?, ?, ?, ?)`)
          .bind(snapshot.target_date, e.event_id, e.organizer_name, Number(e.in_digest),
            JSON.stringify({...e, target_date: snapshot.target_date}))),
      ]);
      return json({status: 'saved', event_count: snapshot.events.length});
    }
    if (path === '/api/reports' && request.method === 'GET') {
      const day = new URL(request.url).searchParams.get('date');
      if (!day || !/^\d{4}-\d{2}-\d{2}$/.test(day)) return json({error: 'date required'}, 400);
      const result = await env.DB.prepare(`SELECT r.report_json, m.source_text, m.channel_id,
        m.message_ts, m.thread_ts, m.received_at FROM reports r JOIN messages m USING(message_key)
        WHERE r.event_date = ? OR (r.event_id IS NULL AND m.reported_date = ?)
        ORDER BY m.message_ts DESC LIMIT 500`).bind(day, day).all();
      return json({reports: result.results.map(row => ({...row, report: JSON.parse(row.report_json), report_json: undefined}))});
    }
    if (path === '/api/clubs' && request.method === 'GET') {
      const result = await env.DB.prepare('SELECT * FROM club_observations ORDER BY reported_events DESC').all();
      const food = await env.DB.prepare('SELECT * FROM club_food_observations ORDER BY reported_events DESC').all();
      return json({clubs: result.results, food_patterns: food.results, note: 'Reported events only; not a representative food probability.'});
    }
    return json({error: 'Not found'}, 404);
  }
  if (path !== '/slack/events' || request.method !== 'POST') return json({error: 'Not found'}, 404);
  const raw = await request.text();
  if (raw.length > 64000) return json({error: 'Payload too large'}, 413);
  if (!await verifySlack(request, raw, env.SLACK_SIGNING_SECRET)) return json({error: 'Invalid signature'}, 401);
  let payload;
  try { payload = JSON.parse(raw); } catch { return json({error: 'Invalid JSON'}, 400); }
  if (payload.type === 'url_verification' && typeof payload.challenge === 'string')
    return json({challenge: payload.challenge});
  const event = normalizeEvent(payload, env);
  if (!event) return json({status: 'ignored'});
  // Await durable enqueue, NOT LLM work. A queue failure returns 503 so Slack retries.
  await env.FEEDBACK_QUEUE.send(event);
  return json({status: 'queued'});
}

export async function extractReports(event, candidates, context, env, fetcher = fetch) {
  if (!env.OPENAI_API_KEY || !env.OPENAI_MODEL) throw new Error('Missing LLM configuration');
  const response = await fetcher('https://api.openai.com/v1/responses', {
    method: 'POST', signal: AbortSignal.timeout(45000),
    headers: {'Authorization': `Bearer ${env.OPENAI_API_KEY}`, 'Content-Type': 'application/json'},
    body: JSON.stringify({model: env.OPENAI_MODEL, store: false, max_output_tokens: 3500,
      instructions: EXTRACTION_INSTRUCTIONS,
      input: JSON.stringify({current_message: event.source_text,
        reported_at: new Date(Number(event.message_ts) * 1000).toISOString(),
        timezone: 'America/Chicago', event_candidates: candidates, prior_thread_messages: context}),
      text: {format: {type: 'json_schema', name: 'food_observations', strict: true, schema: REPORT_SCHEMA}},
    }),
  });
  if (!response.ok) throw new Error(`LLM HTTP ${response.status}`);
  const body = await response.json();
  if (body.status !== 'completed') throw new Error('LLM response incomplete');
  const content = (body.output || []).filter(o => o.type === 'message').flatMap(o => o.content || []);
  if (content.some(c => c.type === 'refusal')) throw new Error('LLM refused extraction');
  const text = content.filter(c => c.type === 'output_text').map(c => c.text).join('');
  return {reports: validateExtraction(JSON.parse(text), event.source_text, candidates),
    model: body.model || env.OPENAI_MODEL, usage: body.usage || null};
}

export async function processEvent(event, env, fetcher = fetch) {
  if (await env.DB.prepare('SELECT event_id FROM processed_events WHERE event_id = ?').bind(event.event_id).first())
    return {status: 'duplicate'};
  const existing = await env.DB.prepare('SELECT revision_ts FROM messages WHERE message_key = ?').bind(event.message_key).first();
  if (existing && Number(existing.revision_ts) >= Number(event.revision_ts)) {
    await env.DB.prepare('INSERT OR IGNORE INTO processed_events(event_id) VALUES (?)').bind(event.event_id).run();
    return {status: 'stale'};
  }
  let extraction = {reports: [], model: null, usage: null};
  if (!event.deleted) {
    const day = localDate(event.message_ts);
    const rows = await env.DB.prepare(`SELECT event_json FROM event_catalog
      WHERE target_date BETWEEN date(?, '-7 days') AND date(?, '+1 day')
      ORDER BY abs(julianday(target_date) - julianday(?)), target_date DESC LIMIT 200`).bind(day, day, day).all();
    const candidates = rows.results.map(row => JSON.parse(row.event_json));
    const prior = await env.DB.prepare(`SELECT source_text, message_ts FROM messages
      WHERE channel_id = ? AND thread_ts = ? AND message_key != ? AND deleted = 0
      AND message_ts < ? ORDER BY message_ts DESC LIMIT 8`)
      .bind(event.channel_id, event.thread_ts, event.message_key, event.message_ts).all();
    const context = prior.results.reverse();
    extraction = await extractReports(event, candidates, context, env, fetcher);
    extraction.context = context;
  }
  // Queue is configured with one consumer. All writes commit atomically before ack.
  await env.DB.batch([
    env.DB.prepare(`INSERT INTO messages
      (message_key, channel_id, message_ts, thread_ts, revision_ts, source_text, reported_date, deleted)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT(message_key) DO UPDATE SET
      revision_ts=excluded.revision_ts, source_text=excluded.source_text, deleted=excluded.deleted`)
      .bind(event.message_key, event.channel_id, event.message_ts, event.thread_ts,
        event.revision_ts, event.source_text, localDate(event.message_ts), event.deleted),
    env.DB.prepare('DELETE FROM reports WHERE message_key = ?').bind(event.message_key),
    // Revoke derived thread observations when a source message is edited/deleted.
    ...(existing ? [env.DB.prepare(`DELETE FROM reports WHERE message_key IN (
      SELECT r.message_key FROM reports r, json_each(r.report_json, '$.source_context') c
      JOIN messages m ON m.message_key = r.message_key
      WHERE m.channel_id = ? AND json_extract(c.value, '$.message_ts') = ?
    )`).bind(event.channel_id, event.message_ts)] : []),
    ...extraction.reports.map((report, i) => env.DB.prepare(`INSERT INTO reports
      (message_key, report_index, event_id, event_date, organizer_name, food_status, missed_by_digest, report_json)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?)`).bind(event.message_key, i, report.event_id,
        report.event_date, report.organizer_name, report.food_status,
        report.missed_by_digest === null ? null : Number(report.missed_by_digest),
        JSON.stringify({...report, source_context: (extraction.context || []).map(c => ({message_ts: c.message_ts})), model: extraction.model, reported_at: new Date(Number(event.message_ts) * 1000).toISOString()}))),
    env.DB.prepare('INSERT INTO processed_events(event_id) VALUES (?)').bind(event.event_id),
  ]);
  // Optional UX is best effort after storage. Do not repeat prompts on queue redelivery.
  if (extraction.reports.length && env.SLACK_ACKNOWLEDGEMENTS === 'true') {
    try {
      const unresolved = extraction.reports.some(r => !r.event_id);
      await slack('reactions.add', {channel: event.channel_id, timestamp: event.message_ts,
        name: unresolved ? 'eyes' : 'white_check_mark'}, env, fetcher);
      if (unresolved && !existing) await slack('chat.postMessage', {
        channel: event.channel_id, thread_ts: event.thread_ts,
        text: 'I saved your food report. Which event was this for? Reply with the event name or CampusGroups link so I can match it.',
        unfurl_links: false, unfurl_media: false,
      }, env, fetcher);
    } catch { console.log(JSON.stringify({status: 'slack-ack-failed', event_id: event.event_id})); }
  }
  return {status: event.deleted ? 'deleted' : 'processed', report_count: extraction.reports.length,
    unmatched_count: extraction.reports.filter(r => !r.event_id).length,
    model: extraction.model, usage: extraction.usage};
}

async function slack(method, payload, env, fetcher) {
  if (!env.SLACK_BOT_TOKEN) throw new Error('Missing Slack bot token');
  const response = await fetcher(`https://slack.com/api/${method}`, {
    method: 'POST', signal: AbortSignal.timeout(10000),
    headers: {'Authorization': `Bearer ${env.SLACK_BOT_TOKEN}`, 'Content-Type': 'application/json'},
    body: JSON.stringify(payload),
  });
  if (!response.ok) throw new Error('Slack acknowledgement failed');
  const result = await response.json();
  if (!result.ok && result.error !== 'already_reacted') throw new Error('Slack acknowledgement failed');
}

export default {
  async fetch(request, env) {
    try { return await handleRequest(request, env); }
    catch { return json({error: 'Service unavailable'}, 503); }
  },
  async queue(batch, env) {
    for (const message of batch.messages) {
      try {
        const result = await processEvent(message.body, env);
        console.log(JSON.stringify({event_id: message.body.event_id, ...result}));
        message.ack();
      } catch (error) {
        // Only status/type messages from our code, never source text/API error bodies.
        console.log(JSON.stringify({status: 'processing-failed', event_id: message.body.event_id,
          error_type: error.name}));
        message.retry({delaySeconds: 60});
      }
    }
  },
};
