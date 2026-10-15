import json
import unittest
from datetime import date
from unittest.mock import patch

import campusgroups_food_digest as digest


DAY = date(2026, 10, 15)


def detail_html(label, start="2026-10-15T12:15:00-05:00"):
    return '<script type="application/ld+json">' + json.dumps({
        "name": "Example event",
        "description": "A discussion",
        "startDate": start,
        "location": {"name": "Example room"},
    }) + '</script>' + label


class FoodLabelTests(unittest.TestCase):
    def test_visible_labels_include_suffixes_and_entities(self):
        for label in (
            "Food Provided", "Food Provided (lunch)",
            "Food Provided: sandwiches", "food\n provided (pizza)",
            "Food&nbsp;Provided (lunch)",
        ):
            with self.subTest(label=label):
                detail = digest.parse_event_detail_html(detail_html(f"<div>{label}</div>"))
                self.assertTrue(detail.food_provided)
                self.assertIsNone(detail.description_food_match)

    def test_absent_labels_do_not_match_comments_attributes_or_code(self):
        for markup in (
            "<div>A discussion</div>",
            "<!-- <div>Food Provided</div> -->",
            "<!-- <div>Food Provided (lunch)</div> -->",
            '<div title="Food Provided">A discussion</div>',
            '<script>const example = ">Food Provided<";</script>',
            '<style>/* >Food Provided< */</style>',
            '<template><div>Food Provided</div></template>',
            '<div>No Food Provided</div>',
            '<div>Food Providedness</div>',
        ):
            with self.subTest(markup=markup):
                self.assertFalse(digest.parse_event_detail_html(detail_html(markup)).food_provided)

    def test_description_food_signal_still_works_without_label(self):
        detail = digest.parse_event_detail_html(
            '<script type="application/ld+json">'
            '{"name":"Example event","description":"Free food at the discussion"}'
            '</script>'
        )
        self.assertFalse(detail.food_provided)
        self.assertEqual(detail.description_food_match, "free food")


class EventUrlTests(unittest.TestCase):
    def setUp(self):
        self.config = digest.RuntimeConfig(netid="test", password="test", session_id="test")

    def entry(self, path, event_id="2261347"):
        return digest.CampusGroupsListEntry(
            fields="", p0="false", p1=event_id, p3="Example event", p18=path,
        )

    def test_event_feed_links_use_canonical_rsvp_route(self):
        for path in (
            "/SB2211/feeds?type=event&amp;type_id=2261347&amp;tab=details",
            "/SB2211/feeds?type=event&type_id=2261347&tab=details",
            "https://kellogg.campusgroups.com/SB2211/feeds?type=event&type_id=2261347",
            "/feeds?type=event&type_id=2261347",
        ):
            with self.subTest(path=path):
                self.assertEqual(digest.build_event_url(self.config, self.entry(path)),
                                 "https://kellogg.campusgroups.com/rsvp_boot?id=2261347")

    def test_canonical_routes_remain_unchanged(self):
        for path in (
            "/sb2211/rsvp_boot?id=2261347",
            "/healthcare/rsvp_boot.aspx?id=2260929",
        ):
            with self.subTest(path=path):
                self.assertEqual(digest.build_event_url(self.config, self.entry(path)),
                                 "https://kellogg.campusgroups.com" + path)

    def test_unrecognized_or_mismatched_feed_links_are_not_rewritten(self):
        for path in (
            "/SB2211/feeds?type=post&type_id=2261347",
            "/SB2211/feeds?type=event&type_id=999",
            "/SB2211/feeds?type=event",
            "/SB2211/feeds?type=event&type_id=bad",
            "/SB2211/other?type=event&type_id=2261347",
            "https://example.invalid/feeds?type=event&type_id=2261347",
        ):
            with self.subTest(path=path):
                expected = path if path.startswith("https:") else digest.DEFAULT_BASE_URL + path
                self.assertEqual(digest.build_event_url(self.config, self.entry(path)), expected)

    def test_missing_path_uses_numeric_event_id_only(self):
        self.assertEqual(digest.build_event_url(self.config, self.entry(None)),
                         "https://kellogg.campusgroups.com/rsvp_boot?id=2261347")
        self.assertIsNone(digest.build_event_url(self.config, self.entry(None, "bad")))

    def test_feed_discovery_fetches_rsvp_and_retains_lunch_validation(self):
        entry = self.entry("/SB2211/feeds?type=event&amp;type_id=2261347&amp;tab=details")
        event = digest.PublicEvent(
            event_id=2261347, title="Example event", organizer_name="Example club",
            start_date=DAY, end_date=DAY, time_text="12:15 PM",
            event_url=digest.build_event_url(self.config, entry),
        )

        def fetch(url, **kwargs):
            if url != "https://kellogg.campusgroups.com/rsvp_boot?id=2261347":
                return "<div>Activity feed</div>"
            return detail_html("<div>Food Provided</div>")

        with patch.object(digest, "fetch_text", side_effect=fetch):
            result = digest.build_food_event(event, self.config, DAY)
        self.assertIsNotNone(result)
        self.assertEqual(result.room_text, "Example room")

        for start in (None, "invalid", "2026-10-15T10:59:00-05:00", "2026-10-15T15:00:00-05:00", "2026-10-16T12:15:00-05:00"):
            with self.subTest(start=start), patch.object(
                digest, "fetch_text", return_value=detail_html("<div>Food Provided (lunch)</div>", start)
            ):
                self.assertIsNone(digest.build_food_event(event, self.config, DAY))

        with patch.object(digest, "fetch_text", return_value=detail_html("<!-- <div>Food Provided</div> -->")):
            self.assertIsNone(digest.build_food_event(event, self.config, DAY))


class RoomClassificationTests(unittest.TestCase):
    def test_registration_gate_does_not_claim_session_is_missing(self):
        self.assertEqual(digest.normalize_room_label("Private Location (register to display)"),
                         "Private Location (register to display)")
        self.assertEqual(digest.normalize_room_label("Private Location (sign in to display)"),
                         "Requires Session ID")

    def test_room_suffixes_ranges_and_combined_rooms(self):
        cases = (
            ("KGH - 2410A&amp;B", "2nd Floor"),
            ("2410A-2410B", "2nd Floor"),
            ("KGH 1420–1430", "1st Floor"),
        )
        for room, expected in cases:
            with self.subTest(room=room):
                self.assertEqual(digest.classify_section_label(room), expected)

    def test_street_address_does_not_override_or_invent_room_floor(self):
        cases = (
            ("KGH 1420&amp;1430 2211 Campus Dr., Evanston, Illi 60208, United States", "1st Floor"),
            ("2211 Campus Dr., Evanston, IL 60208", None),
            ("White Auditorium, 2211 Campus Dr.", "White Auditorium"),
        )
        for room, expected in cases:
            with self.subTest(room=room):
                self.assertEqual(digest.classify_section_label(room), expected)

    def test_ambiguous_or_unknown_rooms_remain_unclassified(self):
        for room in (
            "Private Location (register to display)", "TBD",
            "KGH 1420 & 2410", "KGH 1420 & 3100",
        ):
            with self.subTest(room=room):
                self.assertIsNone(digest.classify_section_label(room))


if __name__ == "__main__":
    unittest.main()
