# Reverify a delivered date without posting again

In Actions → Daily Food Digest → Run workflow, select `main`, enter the
Chicago date (for example `2026-11-02`), and enable `dry_run`.
The workflow still authenticates and recollects event details even if a delivery
receipt exists. It does not post to Slack, upload feedback, or read/write delivery
guard refs. Regular scheduled/manual delivery keeps its existing duplicate guard.

The equivalent local command is:

```
python campusgroups_food_digest.py --date 2026-11-02 --dry-run
```

Inspect sanitized response host/path, content type, byte count, parsed details,
and skipped-event reasons in the logs. No response bodies, cookies, or new
authentication URLs are recorded by these diagnostics. Existing logs still contain
SSO navigation URLs; treat logs as private and redact before sharing.

A detail page without a parseable event start time now fails collection before
Slack reservation/delivery. This deliberately treats inaccessible or changed pages
as a data-quality failure rather than a trustworthy empty digest. Valid dated events
without food can still produce a genuine empty result.

## November 2 incident and next test

SSO and list access succeeded, but all 16 detail responses were 1,016 bytes and
parsed without dates, locations, or food. Root cause is not confirmed: a login
redirect, interstitial, or changed page format remains possible. This PR adds
diagnostics and prevents false-empty delivery; it does not claim to fix access.

After merge, dispatch `dry_run=true` for `2026-11-02`. Success means the job either
parses real dated event details or explicitly fails with a useful diagnostic;
there must be no additional Slack message and the existing receipt must remain.
Investigate the returned host/path and parser before sending any correction.
Repeat the same collection-quality check on the next weekday's morning run.
Scheduler/watchdog deployment changes are separate and not implemented here.

## New event UI fallback

Live browser inspection confirmed that legacy `rsvp_boot` links redirect to a
JavaScript-rendered `/w/.../events/...` UI. A known November 2 event displays
Food Provided and a noon start in the rendered DOM, despite the static 1,016-byte
shell containing neither. If static metadata has no date, collection now renders
the event using Playwright with the full in-memory authenticated storage state.
The state is excluded from model serialization and representation and is never
saved as an artifact. At most two fallback browsers run concurrently.

The fallback reads the event title, date icon row, location, description, and
explicit Food Provided heading. Its context uses America/Chicago for date parsing.
Missing title/date or browser errors still fail collection before delivery.
This covers the observed UI; future markup changes may require another adapter.
