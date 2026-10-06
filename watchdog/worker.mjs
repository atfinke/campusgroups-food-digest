// Independent of GitHub's scheduled-event service. No public dispatch endpoint.
export function chicagoTime(timestamp) {
  const parts = Object.fromEntries(new Intl.DateTimeFormat('en-US', {
    timeZone: 'America/Chicago', year: 'numeric', month: '2-digit', day: '2-digit',
    weekday: 'short', hour: '2-digit', minute: '2-digit', hourCycle: 'h23',
  }).formatToParts(new Date(timestamp)).map(p => [p.type, p.value]));
  return {date: `${parts.year}-${parts.month}-${parts.day}`,
    weekday: parts.weekday, minutes: Number(parts.hour) * 60 + Number(parts.minute)};
}

export async function checkDelivery(env, timestamp, fetcher = fetch) {
  const local = chicagoTime(timestamp);
  if (['Sat', 'Sun'].includes(local.weekday) || local.minutes < 480 || local.minutes >= 660)
    return {status: 'outside-window'};
  if (!env.GITHUB_TOKEN || !env.REPOSITORY || !env.REF)
    throw new Error('Missing watchdog configuration');
  const base = `https://api.github.com/repos/${env.REPOSITORY}`;
  async function api(path, options = {}) {
    const response = await fetcher(base + path, {
      ...options, signal: AbortSignal.timeout(15000),
      headers: {'Authorization': `Bearer ${env.GITHUB_TOKEN}`,
        'Accept': 'application/vnd.github+json', 'X-GitHub-Api-Version': '2022-11-28',
        'User-Agent': 'campusgroups-food-digest-watchdog', 'Content-Type': 'application/json'},
    });
    // Only a ref lookup's 404 means no receipt. Auth/dispatch failures must surface.
    if (response.status === 404 && path.startsWith('/git/ref/')) return null;
    if (!response.ok) throw new Error(`GitHub ${options.method || 'GET'} failed: ${response.status}`);
    return response.status === 204 ? {} : response.json();
  }
  const prefix = `/git/ref/tags/food-digest/${local.date}/`;
  if (await api(prefix + 'delivered')) return {status: 'delivered', date: local.date};
  if (await api(prefix + 'reserved'))
    throw new Error(`Delivery ${local.date} is reserved without confirmation; inspect Slack and the Actions run`);
  // Give an existing runner up to the workflow's 20-minute timeout before dispatching.
  const runs = await api('/actions/workflows/daily-food-digest.yml/runs?per_page=100');
  const active = runs.workflow_runs.some(run =>
    run.head_branch === env.REF && ['queued', 'in_progress', 'waiting', 'pending', 'requested'].includes(run.status) &&
    timestamp - Date.parse(run.created_at) >= 0 && timestamp - Date.parse(run.created_at) < 20 * 60 * 1000);
  if (active) return {status: 'active-run', date: local.date};
  await api('/actions/workflows/daily-food-digest.yml/dispatches', {
    method: 'POST', body: JSON.stringify({ref: env.REF, inputs: {digest_date: local.date}}),
  });
  return {status: 'dispatched', date: local.date};
}

export default {
  async scheduled(controller, env) {
    // Let failures propagate so Cloudflare marks the invocation failed.
    const result = await checkDelivery(env, controller.scheduledTime);
    console.log(JSON.stringify(result));
  },
};
