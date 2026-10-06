import test from 'node:test';
import assert from 'node:assert/strict';
import {createHmac, webcrypto} from 'node:crypto';
import {mkdtempSync, readFileSync, rmSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {spawnSync} from 'node:child_process';
import worker, {handleRequest, normalizeEvent, verifySlack, processEvent, extractReports} from './worker.mjs';
import {validateExtraction} from './schema.mjs';

globalThis.crypto ??= webcrypto;
const ts = '1791306000.000001'; // Oct 6, 2026, 11:00 Chicago
const baseEvent = {type: 'message', channel: 'CFOOD', user: 'USTUDENT', ts,
  text: 'VC club has a bunch of pizza outside 243'};
const payload = (event = baseEvent, id = 'Ev1') => ({type: 'event_callback', team_id: 'TTEAM', event_id: id, event});
const environment = () => ({SLACK_TEAM_ID: 'TTEAM', SLACK_CHANNEL_ID: 'CFOOD', SLACK_SIGNING_SECRET: 'test-secret',
  OPENAI_API_KEY: 'test-api-key', OPENAI_MODEL: 'test-model', FEEDBACK_API_TOKEN: 'test-feedback-token'});
const candidate = {event_id: '123', target_date: '2026-10-06', title: 'VC Lunch',
  organizer_name: 'VC Club', room_text: '243', time_text: '11 AM',
  event_url: 'https://kellogg.campusgroups.com/rsvp_boot?id=123', in_digest: false};
const report = overrides => ({evidence_quote: 'pizza outside 243', event_id: '123', event_date: '2026-10-06',
  match_confidence: 'high', reported_event_name: 'VC lunch', reported_club: 'VC club', food_status: 'available',
  location: 'outside', location_text: '243', quantity_text: 'a bunch', food_items: ['pizza'],
  vendor_reported: null, vendor_inferred: null, observation_time_text: null, access_text: null, ...overrides});
const llm = reports => async () => new Response(JSON.stringify({status: 'completed', model: 'test-model',
  usage: {input_tokens: 100, output_tokens: 40}, output: [{type: 'message',
    content: [{type: 'output_text', text: JSON.stringify({reports})}]}]}), {status: 200});
function signed(value, secret = 'test-secret', time = Math.floor(Date.now() / 1000)) {
  const body = JSON.stringify(value);
  return new Request('https://feedback.example/slack/events', {method: 'POST', body,
    headers: {'x-slack-request-timestamp': String(time),
      'x-slack-signature': 'v0=' + createHmac('sha256', secret).update(`v0:${time}:${body}`).digest('hex')}});
}

// Run worker SQL against real SQLite, using the same atomic batch contract as D1.
function database(t) {
  const dir = mkdtempSync(join(tmpdir(), 'food-feedback-'));
  t.after(() => rmSync(dir, {recursive: true, force: true}));
  const path = join(dir, 'test.sqlite');
  const bridge = `import sys,json,sqlite3
p=json.load(sys.stdin)
c=sqlite3.connect(sys.argv[1]); c.row_factory=sqlite3.Row
if 'script' in p: c.executescript(p['script']); c.commit(); print('[]'); sys.exit()
r=[]
try:
 for s in p['statements']:
  cur=c.execute(s['sql'],s['args']); r.append([dict(x) for x in cur.fetchall()])
 c.commit()
except: c.rollback(); raise
print(json.dumps(r))`;
  function run(statements, script) {
    const result = spawnSync('python3', ['-c', bridge, path], {
      input: JSON.stringify({statements, ...(script ? {script} : {})}), encoding: 'utf8'});
    assert.equal(result.status, 0, result.stderr);
    return JSON.parse(result.stdout);
  }
  run([], readFileSync(new URL('./migrations/0001_feedback.sql', import.meta.url), 'utf8'));
  return {prepare(sql) {return {sql, args: [], bind(...args) {this.args = args; return this;},
    async first() {return run([this])[0][0] || null;},
    async all() {return {results: run([this])[0]};}, async run() {run([this]); return {};}};},
    async batch(statements) {return run(statements).map(results => ({results}));}};
}
async function seed(env) {
  return handleRequest(new Request('https://feedback.example/api/events', {method: 'POST',
    headers: {Authorization: 'Bearer test-feedback-token'},
    body: JSON.stringify({target_date: candidate.target_date, events: [candidate]})}), env);
}

test('signature authenticates raw body and rejects tampering, expired and future requests', async () => {
  const request = signed(payload());
  const raw = await request.text();
  assert.equal(await verifySlack(request, raw, 'test-secret'), true);
  assert.equal(await verifySlack(request, raw + ' ', 'test-secret'), false);
  for (const offset of [-301, 301]) {
    const r = signed(payload(), 'test-secret', Math.floor(Date.now() / 1000) + offset);
    assert.equal(await verifySlack(r, await r.text(), 'test-secret'), false);
  }
});

test('Slack handshake is signed; enqueue is awaited without making an LLM call', async () => {
  let queued;
  const env = {...environment(), FEEDBACK_QUEUE: {send: async e => {queued = e;}}};
  const challenge = await handleRequest(signed({type: 'url_verification', challenge: 'hello'}), env);
  assert.deepEqual(await challenge.json(), {challenge: 'hello'});
  assert.equal((await handleRequest(signed(payload()), env)).status, 200);
  assert.equal(queued.source_text, baseEvent.text);
  assert.equal(queued.user, undefined);
  env.FEEDBACK_QUEUE.send = async () => {throw new Error('offline');};
  assert.equal((await worker.fetch(signed(payload()), env)).status, 503);
  assert.equal((await handleRequest(signed(payload(), 'wrong'), env)).status, 401);
});

test('only configured workspace/channel human messages, edits and deletes are ingested', () => {
  const env = environment();
  assert.ok(normalizeEvent(payload(), env));
  for (const change of [{bot_id: 'B'}, {app_id: 'A'}, {channel: 'COTHER'}, {subtype: 'message_replied'},
    {ts: 'bad'}, {text: 'x'.repeat(12001)}]) assert.equal(normalizeEvent(payload({...baseEvent, ...change}), env), null);
  assert.equal(normalizeEvent({...payload(), team_id: 'OTHER'}, env), null);
  const edit = normalizeEvent(payload({type: 'message', channel: 'CFOOD', subtype: 'message_changed',
    message: {...baseEvent, text: 'gone', edited: {ts: '1791306100.000001'}}}), env);
  assert.equal(edit.revision_ts, '1791306100.000001');
  const deletion = normalizeEvent(payload({type: 'message', channel: 'CFOOD', subtype: 'message_deleted',
    deleted_ts: ts, event_ts: '1791306200.000001'}), env);
  assert.equal(deletion.deleted, 1);
});

test('strict extraction preserves uncertainty, rejects invented IDs, missing evidence and bad shapes', () => {
  const result = validateExtraction({reports: [report()]}, baseEvent.text, [candidate])[0];
  assert.equal(result.missed_by_digest, true);
  assert.equal(result.quantity_text, 'a bunch');
  assert.equal(result.observation_time_text, null);
  assert.equal(validateExtraction({reports: [report({match_confidence: 'medium'})]}, baseEvent.text, [candidate])[0].event_id, null);
  for (const change of [{event_id: '999'}, {evidence_quote: 'invented quote'}, {location: 'rooftop'}, {food_items: 'pizza'}, {access_text: 3}])
    assert.throws(() => validateExtraction({reports: [report(change)]}, baseEvent.text, [candidate]));
  assert.deepEqual(validateExtraction({reports: []}, 'Any food?', [candidate]), []);
});

test('LLM request uses strict schema, no tools, bounded context and store:false', async () => {
  const e = normalizeEvent(payload(), environment());
  await extractReports(e, [candidate], [], environment(), async (url, options) => {
    assert.equal(url, 'https://api.openai.com/v1/responses');
    const body = JSON.parse(options.body);
    assert.equal(body.store, false);
    assert.equal(body.text.format.strict, true);
    assert.equal(body.tools, undefined);
    assert.match(body.instructions, /untrusted data/);
    assert.equal(JSON.parse(body.input).current_message, baseEvent.text);
    return llm([report()])();
  });
  for (const body of [{status: 'incomplete'}, {status: 'completed', output: [{type: 'message', content: [{type: 'refusal'}]}]}])
    await assert.rejects(extractReports(e, [candidate], [], environment(), async () => new Response(JSON.stringify(body))));
});

test('snapshot upload is protected and replaces cancelled events atomically', async t => {
  const env = {...environment(), DB: database(t)};
  assert.equal((await handleRequest(new Request('https://feedback.example/api/clubs'), env)).status, 401);
  assert.equal((await seed(env)).status, 200);
  const invalid = await handleRequest(new Request('https://feedback.example/api/events', {method: 'POST',
    headers: {Authorization: 'Bearer test-feedback-token'}, body: JSON.stringify({target_date: '2026-10-06', events: [candidate, candidate]})}), env);
  assert.equal(invalid.status, 400);
  assert.equal((await env.DB.prepare('SELECT * FROM event_catalog').all()).results.length, 1);
  await handleRequest(new Request('https://feedback.example/api/events', {method: 'POST',
    headers: {Authorization: 'Bearer test-feedback-token'}, body: JSON.stringify({target_date: '2026-10-06', events: []})}), env);
  assert.equal((await env.DB.prepare('SELECT * FROM event_catalog').all()).results.length, 0);
});

test('pipeline stores missed-event observation once, handles retries, edits and deletion', async t => {
  const env = {...environment(), DB: database(t)};
  await seed(env);
  const e = normalizeEvent(payload(), env);
  assert.equal((await processEvent(e, env, llm([report()]))).status, 'processed');
  assert.equal((await processEvent(e, env, () => {throw new Error('must not call');})).status, 'duplicate');
  let row = await env.DB.prepare('SELECT * FROM reports').first();
  assert.equal(row.missed_by_digest, 1);
  const edit = {...e, event_id: 'Ev2', revision_ts: '1791306100.000001', source_text: 'food already gone'};
  await processEvent(edit, env, llm([report({evidence_quote: 'food already gone', food_status: 'ran_out'})]));
  row = await env.DB.prepare('SELECT * FROM reports').first();
  assert.equal(row.food_status, 'ran_out');
  assert.equal((await env.DB.prepare('SELECT * FROM reports').all()).results.length, 1);
  assert.equal((await processEvent({...e, event_id: 'Ev3'}, env, llm([]))).status, 'stale');
  const deletion = {...edit, event_id: 'Ev4', revision_ts: '1791306200.000001', source_text: '', deleted: 1};
  assert.equal((await processEvent(deletion, env, () => {throw new Error('must not call');})).status, 'deleted');
  assert.equal((await env.DB.prepare('SELECT * FROM reports').all()).results.length, 0);
  assert.equal((await env.DB.prepare('SELECT * FROM messages').first()).source_text, '');
});

test('LLM failure leaves event eligible for retry and does not erase an existing report', async t => {
  const env = {...environment(), DB: database(t)};
  await seed(env);
  const e = normalizeEvent(payload(), env);
  await processEvent(e, env, llm([report()]));
  const edit = {...e, event_id: 'Ev2', revision_ts: '1791306100.000001'};
  await assert.rejects(processEvent(edit, env, async () => new Response('', {status: 429})));
  assert.equal(await env.DB.prepare('SELECT * FROM processed_events WHERE event_id = ?').bind('Ev2').first(), null);
  assert.equal((await env.DB.prepare('SELECT * FROM reports').first()).food_status, 'available');
  assert.equal((await processEvent(edit, env, llm([]))).status, 'processed');
});

test('club counts use distinct events, preserve conflicting reports, and exclude unresolved matches', async t => {
  const env = {...environment(), DB: database(t)};
  await seed(env);
  for (let i = 0; i < 3; i++) {
    const e = normalizeEvent(payload({...baseEvent, ts: `17913060${i}0.000001`}, `Ev${i}`), env);
    await processEvent(e, env, llm([report(i === 2 ? {food_status: 'none_seen'} : {})]));
  }
  const row = await env.DB.prepare('SELECT * FROM club_observations').first();
  assert.equal(row.reported_events, 1);
  assert.equal(row.events_with_food_seen, 1);
  assert.equal(row.events_with_none_seen, 1);
  assert.equal(row.missed_food_events, 1);
  const e = normalizeEvent(payload({...baseEvent, ts: '1791306300.000001'}, 'EvUnmatched'), env);
  await processEvent(e, env, llm([report({event_id: null, event_date: null, match_confidence: 'low'})]));
  assert.equal((await env.DB.prepare('SELECT * FROM club_observations').first()).reported_events, 1);
  const response = await handleRequest(new Request('https://feedback.example/api/reports?date=2026-10-06',
    {headers: {Authorization: 'Bearer test-feedback-token'}}), env);
  assert.equal((await response.json()).reports.length, 4);
});

test('thread replies receive prior observations, notifications cannot create a bot loop', async t => {
  const env = {...environment(), DB: database(t), SLACK_ACKNOWLEDGEMENTS: 'true', SLACK_BOT_TOKEN: 'test-bot'};
  await seed(env);
  const e = normalizeEvent(payload(), env);
  const calls = [];
  const fetcher = async (url, options) => {
    if (url.includes('openai')) return llm([report({event_id: null, event_date: null})])();
    calls.push({url, body: JSON.parse(options.body)});
    return new Response(JSON.stringify({ok: true}));
  };
  await processEvent(e, env, fetcher);
  assert.equal(calls.length, 2);
  assert.equal(calls[1].body.thread_ts, ts);
  await processEvent(e, env, fetcher);
  assert.equal(calls.length, 2);
  const reply = normalizeEvent(payload({...baseEvent, ts: '1791306400.000001', thread_ts: ts, text: 'VC Lunch'}, 'EvReply'), env);
  await processEvent(reply, env, async (url, options) => {
    if (url.includes('openai')) {
      assert.equal(JSON.parse(JSON.parse(options.body).input).prior_thread_messages[0].source_text, baseEvent.text);
      return llm([report({evidence_quote: 'VC Lunch'})])();
    }
    return new Response(JSON.stringify({ok: true}));
  });
  assert.equal(JSON.parse((await env.DB.prepare('SELECT report_json FROM reports WHERE message_key = ?').bind(reply.message_key).first()).report_json).source_context.length, 1);
  assert.equal(normalizeEvent(payload({...baseEvent, bot_id: 'BAPP'}), env), null);
  await processEvent({...e, event_id: 'EvDeleteRoot', revision_ts: '1791306500.000001', source_text: '', deleted: 1}, env);
  assert.equal((await env.DB.prepare('SELECT * FROM reports').all()).results.length, 0);
});

test('consumer acknowledges completed work and retries failure', async () => {
  const messages = [];
  const message = {body: {event_id: 'EvRetry'}, ack: () => messages.push('ack'), retry: () => messages.push('retry')};
  const original = console.log; console.log = () => {};
  try {
    await worker.queue({messages: [message]}, {DB: {prepare: () => {throw new Error('offline');}}});
    assert.deepEqual(messages, ['retry']);
    messages.length = 0;
    await worker.queue({messages: [message]}, {DB: {prepare: () => ({bind() {return this;}, first: async () => ({event_id: 'EvRetry'})})}});
    assert.deepEqual(messages, ['ack']);
  } finally {console.log = original;}
});
