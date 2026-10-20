import unittest
from datetime import date

import campusgroups_food_digest as digest


class RoomFormattingTests(unittest.TestCase):
    def setUp(self):
        # Synthetic URL only; never retain a real meeting credential in fixtures.
        self.meeting_url = "https://teams.microsoft.com/l/meetup-join/example"
        self.rsvp_url = "https://kellogg.campusgroups.com/rsvp_boot?id=123"
        event = digest.FoodEvent(
            title="Example event", organizer_name="Example club",
            room_text=f"KGH - L110; {self.meeting_url}",
            event_url=self.rsvp_url, food_detection_source="food_provided",
        )
        self.result = digest.DigestResult(
            base_url=digest.DEFAULT_BASE_URL, target_date=date(2026, 10, 19),
            total_entries=1, matching_event_count=1, food_events=[event],
        )

    def test_fallback_text_preserves_room_and_rsvp_without_meeting_url(self):
        text = digest.build_slack_text(self.result, session_valid=True)
        self.assertIn("- *KGH - L110*", text)
        self.assertNotIn(self.meeting_url, text)
        self.assertIn(f"<{self.rsvp_url}|Example event>", text)

    def test_rich_blocks_preserve_room_and_rsvp_without_meeting_url(self):
        payload = digest.build_slack_payload(self.result, session_valid=True)
        inline_elements = []
        for block in payload.blocks:
            if isinstance(block, digest.SlackRichTextBlock):
                for part in block.elements:
                    sections = part.elements if isinstance(part, digest.SlackRichTextList) else [part]
                    for section in sections:
                        inline_elements.extend(section.elements)
        texts = [e.text for e in inline_elements if isinstance(e, digest.SlackTextElement)]
        links = [e.url for e in inline_elements if isinstance(e, digest.SlackLinkElement)]
        self.assertIn("KGH - L110", texts)
        self.assertNotIn(self.meeting_url, payload.model_dump_json())
        self.assertEqual(links, [self.rsvp_url])


if __name__ == "__main__":
    unittest.main()
