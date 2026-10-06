import test from 'node:test';
import assert from 'node:assert/strict';
import {checkDelivery, chicagoTime} from './worker.mjs';

const env = {GITHUB_TOKEN: 'test', REPOSITORY: 'owner/repo', REF: 'main'};
const now = Date.parse('2026-10-06T13:00:00Z');
function mockApi(responses) {
  const calls = [];
  return {calls, fetcher: async (url, options) => {
    calls.push({url, options});
    assert.ok(responses.length, 'unexpected API call');
    const [status, body] = responses.shift();
    return new Response(status === 204 ? null : JSON.stringify(body ?? {}), {status});
  }};
}

test('Chicago weekday window handles summer and winter DST', async () => {
  assert.equal(chicagoTime(now).minutes, 480);
  assert.equal(chicagoTime(Date.parse('2026-12-08T14:00:00Z')).minutes, 480);
  for (const time of ['2026-10-10T13:00:00Z', '2026-10-06T12:45:00Z', '2026-10-06T16:00:00Z', '2026-12-08T13:45:00Z']) {
    const {status} = await checkDelivery(env, Date.parse(time), () => {throw new Error('should not fetch');});
    assert.equal(status, 'outside-window');
  }
});
test('confirmed delivery skips dispatch', async () => {
  const api = mockApi([[200, {}]]);
  assert.equal((await checkDelivery(env, now, api.fetcher)).status, 'delivered');
  assert.equal(api.calls.length, 1);
});
test('uncertain delivery raises instead of dispatching', async () => {
  const api = mockApi([[404], [200, {}]]);
  await assert.rejects(checkDelivery(env, now, api.fetcher), /reserved without confirmation/);
  assert.equal(api.calls.length, 2);
});
test('missing delivery dispatches explicit Chicago date', async () => {
  const api = mockApi([[404], [404], [200, {workflow_runs: []}], [204]]);
  assert.equal((await checkDelivery(env, now, api.fetcher)).status, 'dispatched');
  assert.deepEqual(JSON.parse(api.calls.at(-1).options.body), {ref: 'main', inputs: {digest_date: '2026-10-06'}});
});
test('recent active run is given time but failed or stale runs permit fallback', async () => {
  for (const [status, age, branch, expected] of [
    ['in_progress', 5, 'main', 'active-run'], ['queued', 25, 'main', 'dispatched'],
    ['completed', 5, 'main', 'dispatched'], ['in_progress', 5, 'feature', 'dispatched'],
  ]) {
    const api = mockApi([[404], [404], [200, {workflow_runs: [
      {status, head_branch: branch, created_at: new Date(now - age * 60000).toISOString()},
    ]}], [204]]);
    assert.equal((await checkDelivery(env, now, api.fetcher)).status, expected);
  }
});
test('API and dispatch permission errors fail visibly', async () => {
  for (const status of [401, 403, 429, 500]) {
    await assert.rejects(checkDelivery(env, now, mockApi([[status]]).fetcher), new RegExp(String(status)));
  }
  await assert.rejects(checkDelivery(env, now, mockApi([[404], [404], [200, {workflow_runs: []}], [403]]).fetcher), /403/);
});
