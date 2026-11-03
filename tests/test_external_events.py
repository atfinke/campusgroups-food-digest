import unittest
from datetime import date
from unittest.mock import patch
from urllib.request import Request

import campusgroups_food_digest as d

DAY = date(2026, 11, 3)


class ExternalEventsTests(unittest.TestCase):
    def setUp(self):
        self.config = d.RuntimeConfig(netid="test", password="test")
        self.event = d.PublicEvent(event_id=1, title="External event", organizer_name="Club",
                                   start_date=DAY, end_date=DAY, time_text="Noon",
                                   event_url="https://kellogg.campusgroups.com/rsvp_boot?id=1")

    def test_external_http_redirect_is_blocked_before_following(self):
        handler = d.EventRedirectHandler()
        for url in ("https://partiful.com/e/example", "https://kellogg.campusgroups.com.evil.invalid/", "http://kellogg.campusgroups.com/", "https://kellogg.campusgroups.com:8443/"):
            with self.subTest(url=url), self.assertRaises(d.ExternalEventRedirect):
                handler.redirect_request(Request(self.event.event_url), None, 302, "Found", {}, url)
        redirected = handler.redirect_request(Request(self.event.event_url), None, 302, "Found", {}, "https://kellogg.campusgroups.com/w/event")
        self.assertEqual(redirected.full_url, "https://kellogg.campusgroups.com/w/event")

    def collect(self, error):
        with patch.object(d, "fetch_list_payload"), patch.object(d, "parse_list_entries", return_value=[]), \
             patch.object(d, "select_events_for_date", return_value=[self.event]), \
             patch.object(d, "build_food_event", side_effect=error):
            return d.collect_food_events(self.config, DAY)

    def test_external_event_becomes_explicit_unverified_warning(self):
        result = self.collect(d.ExternalEventRedirect("blocked"))
        self.assertEqual(result.unverified_events, [self.event])
        self.assertEqual(result.food_events, [])
        payload = d.build_slack_payload(result, True).model_dump(mode="json")
        self.assertIn("Coverage incomplete", payload["text"])
        self.assertIn("External event", str(payload["blocks"]))
        self.assertIn("No verified lunch", payload["text"])

    def test_internal_failure_still_stops_collection(self):
        for error in (RuntimeError("bad page"), d.PlaywrightTimeoutError("timeout")):
            with self.subTest(error=error), self.assertRaises(type(error)):
                self.collect(error)

    def test_normal_food_and_unverified_event_coexist(self):
        food = d.FoodEvent(title="Verified lunch", organizer_name="Club", event_url=self.event.event_url,
                           food_detection_source="food_provided", room_text="KGH 1420")
        result = d.DigestResult(base_url=d.DEFAULT_BASE_URL, target_date=DAY, total_entries=2,
                               matching_event_count=2, food_events=[food], unverified_events=[self.event])
        payload = d.build_slack_payload(result, True).model_dump(mode="json")
        self.assertIn("Verified lunch", payload["text"])
        self.assertIn("Coverage incomplete", payload["text"])
