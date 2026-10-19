# Proposal: reliable morning food digest delivery

Status: proposal for review, October 19, 2026. This document does not change runtime behavior or deploy a scheduler.

## Problem and evidence

The default-branch workflow requests a weekday run at 06:25 in America/Chicago.
Native scheduled runs have repeatedly appeared hours later. Guarded manual recovery
has delivered the digest around 07:00, with Slack acceptance and a durable delivered
receipt verified for each date.

All times below are Chicago daylight time. Native run times are rounded to the
minute shown in Actions; they are not measurements of GitHub's internal queue.

| Date | Native scheduled run | Observed start | Approximate lateness from 06:25 |
| --- | --- | --- | --- |
| Oct. 9 | [#69](https://github.com/atfinke/campusgroups-food-digest/actions/runs/37966535377) | 12:28 | 6h 03m |
| Oct. 12 | [#71](https://github.com/atfinke/campusgroups-food-digest/actions/runs/38303559121) | 15:20 | 8h 55m |
| Oct. 13 | [#73](https://github.com/atfinke/campusgroups-food-digest/actions/runs/38446323104) | 13:23 | 6h 58m |
| Oct. 14 | [#75](https://github.com/atfinke/campusgroups-food-digest/actions/runs/38593049287) | 10:51 | 4h 26m |
| Oct. 15 | [#78](https://github.com/atfinke/campusgroups-food-digest/actions/runs/38762163556) | 10:38 | 4h 13m |
| Oct. 16 | [#80](https://github.com/atfinke/campusgroups-food-digest/actions/runs/38923690495) | 10:24 | 3h 59m |

On Oct. 19, no native run or delivered receipt was present at approximately 06:57.
[Recovery #81](https://github.com/atfinke/campusgroups-food-digest/actions/runs/39248430901)
started promptly, found four food events among 22 checked, and logged Slack webhook
acceptance at 06:59:32. The delivered tag for 2026-10-19 was then confirmed.
Friday's #80 logged "Daily digest already delivered; skipping," confirming that
the late native run did not send a second digest.

Diagnosis: high confidence that native scheduled execution is late; the exact
internal GitHub cause is unconfirmed. A missing run has no job logs to explain why
it was not created on time. The observations alone cannot distinguish all internal
scheduler and queue causes. Recent recovery runs do not show authentication or
Slack failures. GitHub [documents schedule delays and possible dropped jobs under
load](https://docs.github.com/en/actions/reference/workflows-and-actions/events-that-trigger-workflows#schedule);
this is a possible explanation, not proof of the cause in this repository.

## Reuse the existing watchdog

An external scheduler implementation already exists in
[`watchdog/worker.mjs`](../watchdog/worker.mjs), with configuration in
[`watchdog/wrangler.toml`](../watchdog/wrangler.toml) and activation instructions
in the [README](../README.md#activate-the-fallback-after-merging).
Its Cloudflare deployment, active cron triggers, and configured token have not
been verified. Source code in GitHub does not prove that it is running.

The current watchdog checks every 15 minutes from 08:00 through 10:45 on Chicago
weekdays. Even if deployed successfully, that window cannot recover delivery
before 07:00. The proposed fix is to activate and move this existing watchdog
earlier, rather than build another scheduler.

## Proposed behavior and implementation scope

1. Keep the native 06:25 GitHub schedule as one delivery path.
2. Move the watchdog's Chicago window to 06:40 through 10:40 on weekdays, checking
   every 15 minutes from the first check: 06:40, 06:55, 07:10, and so on.
   A first check at 06:40 gives the native schedule 15 minutes to start.
3. Update both the UTC cron coverage and the handler's local-time filter together.
   A proposed UTC trigger is `10,25,40,55 11-16 * * *`, with the handler allowing
   only Chicago weekday minutes from 400 through 640 inclusive. That covers both
   daylight and standard time; the local filter must reject the extra invocations.
4. Preserve the existing behavior: check the delivered receipt first; stop and
   surface an error on an unconfirmed reservation; allow a recent active main
   run time to finish; dispatch `daily-food-digest.yml` on `main` with an explicit
   Chicago `digest_date` only when appropriate.
5. Preserve workflow concurrency and the atomic delivery guard. Never expire or
   automatically clear a reservation, and never remove a delivered receipt.
   A stale active run may lead to another queued dispatch under the current
   watchdog policy, but concurrency and atomic claims must still prevent a
   second Slack post.
6. Keep the independent morning verification while validating the deployment.
   A green Actions run or a successful dispatch response alone is not delivery.

Target: confirmed delivery by approximately 07:00 on weekdays under normal
conditions. This is an operational target, not a guarantee: the watchdog still
depends on GitHub's API, runners, CampusGroups authentication, and Slack.
An active run's existing 20-minute grace period can push recovery past that target;
report this explicitly rather than declaring an on-time success.

## Deployment prerequisites

- Verify whether `campusgroups-food-digest-watchdog` is already deployed in the
  intended Cloudflare account; inspect its version, cron triggers, and logs.
- Configure a repository-scoped fine-grained GitHub token with Contents read and
  Actions write, expiry, and a rotation owner, as described in the README.
- Store the token as a Cloudflare Worker secret. The watchdog needs neither the
  Northwestern credentials nor the Slack webhook.
- Deploy the reviewed window change and enable observation of failed invocations.
  Retain the existing no-public-HTTP-trigger design.
- Record the deployment version, deployment date, and owner without recording
  secret values. No new credentials or permission changes are made by this proposal.

## Validation and next-day acceptance

Before deployment, extend `watchdog/worker.test.mjs` to cover the new first/last
checks and rejected times just outside the window in both summer and winter.
Keep coverage for weekends, delivered skips, unconfirmed reservations, active and
stale runs, explicit-date dispatch, and API failures. Run:

```bash
node --test watchdog/worker.test.mjs
python -m unittest discover -s tests -v
```

The existing tests describe the current 08:00 window and do not validate the
proposed 06:40 window until updated. This documentation PR does not claim those
future implementation tests have passed.

On the first weekday after deployment:

1. Capture the native run's creation/start times separately from watchdog
   invocation and recovery times.
2. If no receipt exists, confirm that the watchdog dispatches with that day's
   Chicago date. Measure dispatch-to-runner-start and dispatch-to-Slack-acceptance.
3. Verify Slack acceptance and `refs/tags/food-digest/YYYY-MM-DD/delivered`,
   including a legitimate zero-event digest.
4. Confirm the next watchdog invocation returns `delivered`, and that any late
   native run logs a duplicate skip without another Slack post.
5. Mark native scheduling, external recovery, and duplicate suppression as
   separate pass/fail results. Continue observation for five weekdays.

Until deployment, the next Oct. 20 morning check can retest the existing guarded
manual fallback and inspect Oct. 19's eventual native run. It cannot establish
that an undeployed Cloudflare change works.

For every late or failed morning, record the date, expected and observed times,
run links, sanitized error or log evidence, diagnosis/confidence, recovery result,
proposed corrective action, and the next test with its success criteria.

## Failure handling and rollback

For an uncertain Slack send or a reserved-but-unconfirmed date, wait for active
senders to finish and use the README's manual inspection procedure. Do not
automatically dispatch through an uncertain reservation.

If the new watchdog deployment misbehaves, restore its previous deployment or
pause its cron triggers while keeping the independent verification and guarded
manual recovery. Keep delivery receipts intact. A scheduler outage should produce
a visible incident, not an unverified success.
