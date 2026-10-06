import json
import os
import unittest
from datetime import date
from unittest.mock import Mock, patch
from urllib.error import HTTPError

import campusgroups_food_digest as digest
from feedback_client import publish_event_snapshot


class FeedbackTests(unittest.TestCase):
    def setUp(self):
        self.event = digest.PublicEvent(event_id=123, title='VC Lunch', organizer_name='VC Club',
            start_date=date(2026, 10, 6), end_date=date(2026, 10, 6), time_text='11 AM',
            room_text='243', event_url='https://kellogg.campusgroups.com/rsvp_boot?id=123')
        self.result = digest.DigestResult(base_url=digest.DEFAULT_BASE_URL, target_date=date(2026, 10, 6),
            total_entries=1, matching_event_count=1, matching_events=[self.event], food_events=[])

    def test_disabled_feedback_makes_no_network_call(self):
        with patch.dict(os.environ, {}, clear=True), patch('feedback_client.urlopen') as send:
            self.assertFalse(publish_event_snapshot(self.result))
            send.assert_not_called()

    def test_snapshot_includes_events_missed_by_food_filter(self):
        with patch.dict(os.environ, {'FEEDBACK_API_URL': 'https://feedback.example', 'FEEDBACK_API_TOKEN': 'test'}), \
                patch('feedback_client.urlopen') as send:
            send.return_value.__enter__.return_value.status = 200
            self.assertTrue(publish_event_snapshot(self.result))
            request = send.call_args.args[0]
            body = json.loads(request.data)
            self.assertEqual(body['events'][0]['event_id'], '123')
            self.assertFalse(body['events'][0]['in_digest'])
            self.assertEqual(request.full_url, 'https://feedback.example/api/events')

    def test_upload_failure_does_not_block_delivery(self):
        with patch.dict(os.environ, {'FEEDBACK_API_URL': 'https://feedback.example', 'FEEDBACK_API_TOKEN': 'test'}), \
                patch('feedback_client.urlopen', side_effect=HTTPError('', 503, '', {}, None)):
            self.assertFalse(publish_event_snapshot(self.result))

    def test_reject_insecure_endpoint_before_sending_credentials(self):
        with patch.dict(os.environ, {'FEEDBACK_API_URL': 'http://feedback.example', 'FEEDBACK_API_TOKEN': 'test'}), \
                patch('feedback_client.urlopen') as send:
            self.assertFalse(publish_event_snapshot(self.result))
            send.assert_not_called()

    def test_daily_collection_retains_all_candidates(self):
        with patch.object(digest, 'fetch_list_payload'), patch.object(digest, 'parse_list_entries', return_value=[]), \
                patch.object(digest, 'select_events_for_date', return_value=[self.event]), \
                patch.object(digest, 'build_food_event', return_value=None):
            result = digest.collect_food_events(Mock(), self.result.target_date)
            self.assertEqual(result.matching_events, [self.event])
            self.assertEqual(result.food_events, [])


class ContextOnlyTests(unittest.TestCase):
    def test_context_only_does_not_post_to_slack(self):
        config = Mock(slack_webhook_url='https://example.invalid')
        result = Mock(session_valid=True, slack_text='digest', digest_result=Mock())
        with patch.object(digest, 'create_authenticated_runtime_config', return_value=config), \
                patch.object(digest, 'load_runtime_config'), patch.object(digest, 'run', return_value=result), \
                patch.object(digest, 'publish_event_snapshot', return_value=True) as publish, \
                patch.object(digest, 'post_json') as post:
            self.assertEqual(digest.main(['--date', '2026-10-06', '--publish-feedback-context']), 0)
            publish.assert_called_once_with(result.digest_result)
            post.assert_not_called()

    def test_requested_context_failure_is_visible(self):
        result = Mock(session_valid=True, slack_text='digest', digest_result=Mock())
        with patch.object(digest, 'create_authenticated_runtime_config'), \
                patch.object(digest, 'load_runtime_config'), patch.object(digest, 'run', return_value=result), \
                patch.object(digest, 'publish_event_snapshot', return_value=False), \
                patch.object(digest, 'post_json') as post:
            self.assertEqual(digest.main(['--publish-feedback-context']), 1)
            post.assert_not_called()
