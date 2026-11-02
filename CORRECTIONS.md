# One-time digest corrections

After reviewing/merging the correction feature, dispatch Daily Food Digest on
`main` with an explicit Chicago `digest_date`, `correction=true`, and
`dry_run=false`. This recollects current event data and posts a prominently labeled
correction to the existing Slack webhook. It does not edit the earlier message.

The CLI equivalent is:

```
python campusgroups_food_digest.py --date 2026-11-02 --send-slack --github-delivery-guard --correction
```

An original `food-digest/DATE/delivered` receipt must exist. Correction claims use
`food-digest/DATE/correction-v1/reserved` and `.../delivered`. The original refs are
never written or removed. There is intentionally no arbitrary revision input or
force option: repeats skip an already confirmed correction, while an unconfirmed
reservation fails for human inspection. Authentication/collection failures occur
before reservation. Uncertain Slack sends keep their reservation and must not be
blindly retried. The normal weekday schedule does not enable correction mode.

Verify Slack acceptance in logs and the correction delivered receipt before
claiming success. A green duplicate-suppression run is not a new send. Do not
delete any receipt to force another post.
