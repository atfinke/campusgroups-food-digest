# CampusGroups Food Digest

Small Python CLI that reads authenticated Kellogg CampusGroups events, keeps the lunch ones with `Food Provided`, and prints or posts a Slack digest.

Built entirely by OpenAI GPT-5.4 via Codex.

## Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
cp .env.example .env
```

Required:

- `NORTHWESTERN_NETID`
- `NORTHWESTERN_PASSWORD`
- `SLACK_WEBHOOK_URL` when using `--send-slack`

## Run

```bash
python campusgroups_food_digest.py
python campusgroups_food_digest.py --date 2026-04-09
python campusgroups_food_digest.py --date 2026-04-09 --send-slack
python campusgroups_food_digest.py --json
```

## GitHub Actions

The daily workflow is [`.github/workflows/daily-food-digest.yml`](.github/workflows/daily-food-digest.yml). It runs on weekdays at `6:25 AM` `America/Chicago`. Add these repository secrets before enabling it:

- `NORTHWESTERN_NETID`
- `NORTHWESTERN_PASSWORD`
- `SLACK_WEBHOOK_URL`

If Northwestern inserts an additional verification step, the workflow will fail instead of silently falling back to partial room data.


## Independent morning fallback and duplicate protection

The daily workflow now uses `--github-delivery-guard`. A successful delivery,
including a legitimate "No Lunch Events" digest, writes a permanent receipt for
that Chicago date. Scheduled, manual, and fallback workflow runs all use the
same guard. Authentication or collection failures fail the workflow and remain
eligible for another attempt; exit code 2 is no longer treated as success.

The independent scheduler is a Cloudflare Worker in [`watchdog/`](watchdog/).
Every 15 minutes on Chicago weekdays from **8:00 a.m. through 10:45 a.m.**, it:

1. Checks GitHub for a confirmed delivery receipt.
2. Stops with an error if delivery was reserved but never confirmed.
3. Gives a queued/running workflow up to 20 minutes before dispatching a fallback.
4. Dispatches `daily-food-digest.yml` on `main` with an explicit `digest_date`.

This avoids GitHub cron delays but still depends on GitHub's API and hosted
runners. It cannot guarantee delivery during a GitHub outage. Repeated checks
allow recovery after transient API, runner, authentication, or collection
failures. Workflow concurrency and atomic delivery claims protect against races
with late scheduled runs. Cloudflare invocation logs show `delivered`,
`active-run`, `dispatched`, or errors; Actions logs show execution failures.
Configure your monitoring for failed Worker invocations and failed Actions runs.

### Activate the fallback after merging

The PR supplies the code, **not an active Cloudflare deployment**.

1. Merge the guarded workflow first. Confirm repository policies permit its
   `GITHUB_TOKEN` to use `contents: write` and create the delivery tags.
2. Create a fine-grained GitHub token restricted to this repository, with
   **Contents: read** and **Actions: write**. Set an expiry and arrange rotation.
   The Worker gets no Northwestern credentials or Slack webhook.
3. With Node.js and Wrangler installed, sign in to your Cloudflare account and
   deploy from the `watchdog` directory:

   ```bash
   cd watchdog
   npx wrangler login
   npx wrangler secret put GITHUB_TOKEN
   npx wrangler deploy
   npx wrangler tail
   ```

   Enter the token at the secret prompt, never in source control. Review
   `REPOSITORY` and `REF` in `wrangler.toml` if deploying to another repository.
   The cron is UTC; the handler filters the Chicago window in both daylight
   saving and standard time. There is no public HTTP trigger endpoint.
4. Start the Worker on the next weekday after any unguarded delivery, or seed a
   receipt for today's already-posted digest before activation (see below).
   Historic deliveries have no receipts and cannot be inferred from old
   "success" runs, since those runs could mask an authentication failure.
5. Run the guarded workflow once through GitHub's **Run workflow** control.
   Verify its Slack message and both delivery tags, then run it again for the
   same date: it should skip without authenticating or posting. Check Cloudflare
   logs during the next morning window to confirm it sees `delivered`.

### Ambiguous delivery and manual recovery

Slack incoming webhooks do not provide an atomic transaction with GitHub.
Immediately before posting, the guard atomically creates
`refs/tags/food-digest/YYYY-MM-DD/reserved`, pointing to the executing commit.
Only after Slack accepts the message does it create
`refs/tags/food-digest/YYYY-MM-DD/delivered`. No event or credential data is
stored in these tags. The reservation intentionally never expires: a timeout,
killed runner, or receipt-write failure can leave a message posted without a
receipt. Automatically reclaiming that reservation could duplicate the post.

For a reserved-but-unconfirmed date, wait until all runs for that date have
finished, then inspect Slack and the Actions logs. Using `gh` authenticated with
Contents write permission, choose **one** recovery action:

- If the digest is already in Slack, create the delivered tag (also use this to
  seed a receipt for an unguarded delivery during migration):

  ```bash
  gh api repos/atfinke/campusgroups-food-digest/git/refs \
    -f ref=refs/tags/food-digest/YYYY-MM-DD/delivered -f sha=FULL_COMMIT_SHA
  ```

- If you have confirmed it was not posted, delete **only** the reserved tag and
  dispatch a new run with that `digest_date`:

  ```bash
  gh api --method DELETE \
    repos/atfinke/campusgroups-food-digest/git/refs/tags/food-digest/YYYY-MM-DD/reserved
  ```

Do not clear state while a sender is active. Keep delivered receipts permanently;
removing them permits duplicates. Existing CLI use without
`--github-delivery-guard` still posts normally and does not participate in this
protection. All automated workflow paths enable the guard.

## Tests

No real CampusGroups login, Slack post, or dispatch is needed for these tests:

```bash
python -m pip install -r requirements.txt
python -m unittest discover -s tests -v
node --test watchdog/worker.test.mjs
```

The test workflow runs on pushes and pull requests without delivery secrets.
Tests cover duplicate skips, claim conflicts, API failures, authentication and
collection failures, uncertain Slack outcomes, receipt-write failures, fallback
dispatches, active runs, weekends, and the Chicago daylight-saving offset.
