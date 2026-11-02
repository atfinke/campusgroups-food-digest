import unittest
from datetime import date
from unittest.mock import Mock, patch

import campusgroups_food_digest as digest


class DiagnosticTests(unittest.TestCase):
    def test_dry_run_recollects_without_external_writes(self):
        result = Mock(session_valid=True, slack_text="No lunch events")
        with patch.object(digest, "create_authenticated_runtime_config") as auth, \
             patch.object(digest, "load_runtime_config"), \
             patch.object(digest, "run", return_value=result) as collect, \
             patch.object(digest.DeliveryGuard, "from_environment") as guard, \
             patch.object(digest, "post_json") as post, \
             patch.object(digest, "publish_event_snapshot") as publish:
            self.assertEqual(digest.main(["--date", "2026-11-02", "--dry-run"]), 0)
            collect.assert_called_once_with(config=auth.return_value, target_date=date(2026, 11, 2))
            guard.assert_not_called()
            post.assert_not_called()
            publish.assert_not_called()

    def test_dry_run_rejects_write_flags(self):
        for flag in ("--send-slack", "--github-delivery-guard", "--publish-feedback-context"):
            with self.subTest(flag=flag), self.assertRaises(SystemExit):
                digest.parse_args(["--dry-run", flag])

    def test_200_shell_is_rejected_instead_of_no_food(self):
        config = digest.RuntimeConfig(netid="test", password="test", session_id="test")
        with patch.object(digest, "fetch_text", return_value="<html>Sign in</html>"):
            with self.assertRaisesRegex(RuntimeError, "missing start datetime"):
                digest.fetch_event_detail("https://example.invalid/event", config)

    def test_valid_event_without_food_is_not_a_fetch_failure(self):
        config = digest.RuntimeConfig(netid="test", password="test", session_id="test")
        html = '''<script type="application/ld+json">{"@type":"Event","name":"Talk",
        "startDate":"2026-11-02T12:00:00-06:00"}</script>'''
        with patch.object(digest, "fetch_text", return_value=html):
            detail = digest.fetch_event_detail("https://example.invalid/event", config)
        self.assertFalse(detail.food_provided)
        self.assertIsNotNone(detail.start_datetime)
