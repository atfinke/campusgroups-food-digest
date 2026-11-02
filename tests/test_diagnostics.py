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
        with patch.object(digest, "fetch_text", return_value="<html>Sign in</html>"), \
             patch.object(digest, "fetch_rendered_event_detail", return_value=digest.EventDetail(food_provided=False)):
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

    def test_new_ui_food_provided_and_chicago_time(self):
        detail = digest.parse_rendered_event_fields({
            "title": "Lunch and Learn with Kevin Charlton",
            "date_text": "date_range Mon, Nov 02, 2026, 12:00 PM - 1:30 PM CST",
            "food_provided": True,
            "location": "Location available after registration",
        })
        self.assertTrue(detail.food_provided)
        self.assertEqual(detail.start_datetime.hour, 12)
        self.assertEqual(detail.start_datetime.utcoffset().total_seconds(), -21600)
        self.assertEqual(detail.location_name, "Private Location (register to display)")

    def test_new_ui_no_food_and_description_fallback(self):
        fields = {"title": "Talk", "date_text": "Tue, Jun 02, 2026, 12:00 PM - 1:00 PM CDT"}
        detail = digest.parse_rendered_event_fields(fields)
        self.assertFalse(detail.food_provided)
        self.assertIsNone(detail.description_food_match)
        self.assertEqual(detail.start_datetime.utcoffset().total_seconds(), -18000)
        fields["description"] = "Lunch will be provided."
        self.assertIsNotNone(digest.parse_rendered_event_fields(fields).description_food_match)

    def test_rendered_shell_or_login_still_fails(self):
        for fields in ({}, {"title": "Sign in"}, {"date_text": "Mon, Nov 02, 2026, 12:00 PM"}):
            with self.subTest(fields=fields), self.assertRaises(RuntimeError):
                digest.parse_rendered_event_fields(fields)

    def test_static_shell_uses_rendered_fallback(self):
        config = digest.RuntimeConfig(netid="test", password="test", session_id="test")
        expected = digest.parse_rendered_event_fields({
            "title": "Talk", "date_text": "Mon, Nov 02, 2026, 12:00 PM", "food_provided": True,
        })
        with patch.object(digest, "fetch_text", return_value="<div id='root'></div>"), \
             patch.object(digest, "fetch_rendered_event_detail", return_value=expected) as rendered:
            self.assertEqual(digest.fetch_event_detail("https://example.invalid/event", config), expected)
            rendered.assert_called_once()

    def test_authenticated_browser_state_is_not_serialized(self):
        config = digest.RuntimeConfig(netid="test", password="test", browser_storage_state={"cookies": [{"value": "secret"}]})
        self.assertNotIn("browser_storage_state", config.model_dump())
        self.assertNotIn("secret", repr(config))
