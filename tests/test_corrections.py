import unittest
from contextlib import ExitStack
from datetime import date
from unittest.mock import Mock, patch

import campusgroups_food_digest as digest
from delivery_guard import CorrectionGuard, DeliveryGuard

DAY = date(2026, 11, 2)
ARGS = ["--date", DAY.isoformat(), "--send-slack", "--github-delivery-guard", "--correction"]


class CorrectionTests(unittest.TestCase):
    def test_refs_are_separate_and_fixed(self):
        self.assertEqual(CorrectionGuard.ref(DAY, "delivered"), "tags/food-digest/2026-11-02/correction-v1/delivered")
        self.assertNotEqual(CorrectionGuard.ref(DAY, "reserved"), DeliveryGuard.ref(DAY, "reserved"))

    def test_requires_date_guard_and_send(self):
        for args in (["--correction"], ARGS + ["--dry-run"], ["--send-slack", "--github-delivery-guard", "--correction"]):
            with self.subTest(args=args), self.assertRaises(SystemExit):
                digest.parse_args(args)

    def test_correction_delivery_and_failure_paths(self):
        for scenario in ("success", "missing_original", "already_corrected", "reserved", "uncertain"):
            with self.subTest(scenario=scenario), ExitStack() as stack:
                original = Mock()
                original.delivered.return_value = scenario != "missing_original"
                correction = Mock()
                correction.delivered.return_value = scenario == "already_corrected"
                correction.reserve.return_value = True
                if scenario == "reserved":
                    correction.ensure_available.side_effect = RuntimeError("reserved")
                stack.enter_context(patch.object(DeliveryGuard, "from_environment", return_value=original))
                stack.enter_context(patch.object(CorrectionGuard, "from_environment", return_value=correction))
                stack.enter_context(patch.object(digest, "load_runtime_config"))
                stack.enter_context(patch.object(digest, "create_authenticated_runtime_config", return_value=Mock(slack_webhook_url="https://example.invalid")))
                stack.enter_context(patch.object(digest, "run", return_value=Mock(session_valid=True, slack_text="digest")))
                stack.enter_context(patch.object(digest, "publish_event_snapshot"))
                stack.enter_context(patch.object(digest, "build_slack_payload", return_value=Mock(model_dump=Mock(return_value={"text": "digest", "blocks": []}))))
                post = stack.enter_context(patch.object(digest, "post_json"))
                if scenario == "uncertain":
                    post.side_effect = TimeoutError("unknown send outcome")
                result = digest.main(ARGS)
                original.reserve.assert_not_called()
                original.confirm.assert_not_called()
                if scenario in ("success", "uncertain"):
                    post.assert_called_once()
                    self.assertIn("Correction for 2026-11-02", post.call_args.args[1]["text"])
                    self.assertIn("Correction", post.call_args.args[1]["blocks"][0]["text"]["text"])
                    correction.reserve.assert_called_once_with(DAY)
                else:
                    post.assert_not_called()
                if scenario == "success":
                    self.assertEqual(result, 0)
                    correction.confirm.assert_called_once_with(DAY)
                else:
                    correction.confirm.assert_not_called()
                    self.assertEqual(result, 0 if scenario == "already_corrected" else 1)
