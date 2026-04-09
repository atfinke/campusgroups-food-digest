# CampusGroups Food Digest

Small Python CLI that reads authenticated Kellogg CampusGroups events, keeps the ones with `Food Provided`, and prints or posts a Slack digest.

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

The daily workflow is [`.github/workflows/daily-food-digest.yml`](.github/workflows/daily-food-digest.yml). It runs at `6:25 AM` `America/Chicago`. Add these repository secrets before enabling it:

- `NORTHWESTERN_NETID`
- `NORTHWESTERN_PASSWORD`
- `SLACK_WEBHOOK_URL`

If Northwestern inserts an additional verification step, the workflow will fail instead of silently falling back to partial room data.
