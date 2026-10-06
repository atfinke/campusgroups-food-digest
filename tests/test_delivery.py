import unittest
from datetime import date
from unittest.mock import Mock, patch
from urllib.error import HTTPError

from delivery_guard import DeliveryGuard
import campusgroups_food_digest as digest

DAY = date(2026, 10, 6)


class GuardTests(unittest.TestCase):
    def setUp(self):
        self.guard = DeliveryGuard('owner/repo', 'test-token', 'a' * 40)

    def test_missing_receipt_is_not_delivered(self):
        with patch.object(self.guard, 'request', side_effect=HTTPError('', 404, '', {}, None)):
            self.assertFalse(self.guard.delivered(DAY))

    def test_api_failure_does_not_look_like_missing_receipt(self):
        for status in (401, 403, 429, 500):
            with self.subTest(status=status), patch.object(self.guard, 'request', side_effect=HTTPError('', status, '', {}, None)):
                with self.assertRaises(HTTPError):
                    self.guard.delivered(DAY)

    def test_claim_is_atomic_and_collision_does_not_send(self):
        with patch.object(self.guard, 'request', side_effect=[HTTPError('', 422, '', {}, None), {}]):
            self.assertFalse(self.guard.reserve(DAY))

    def test_other_validation_errors_are_not_claim_collisions(self):
        with patch.object(self.guard, 'request', side_effect=[HTTPError('', 422, '', {}, None), HTTPError('', 404, '', {}, None)]):
            with self.assertRaises(HTTPError):
                self.guard.reserve(DAY)

    def test_claim_contains_date_and_commit(self):
        with patch.object(self.guard, 'request') as request:
            self.assertTrue(self.guard.reserve(DAY))
            request.assert_called_once_with('/refs', {'ref': 'refs/tags/food-digest/2026-10-06/reserved', 'sha': 'a' * 40})


class DeliveryTests(unittest.TestCase):
    def setUp(self):
        self.guard = Mock()
        self.guard.delivered.return_value = False
        self.guard.reserve.return_value = True
        self.config = Mock(slack_webhook_url='https://example.invalid/webhook')
        self.result = Mock(session_valid=True, slack_text='digest')
        self.patches = [
            patch.object(digest.DeliveryGuard, 'from_environment', return_value=self.guard),
            patch.object(digest, 'configure_logging'),
            patch.object(digest, 'create_authenticated_runtime_config', return_value=self.config),
            patch.object(digest, 'load_runtime_config'),
            patch.object(digest, 'run', return_value=self.result),
            patch.object(digest, 'build_slack_payload'),
            patch.object(digest, 'post_json'),
        ]
        self.mocks = [p.start() for p in self.patches]
        for p in self.patches:
            self.addCleanup(p.stop)

    def execute(self):
        return digest.main(['--date', DAY.isoformat(), '--send-slack', '--github-delivery-guard'])

    def test_delivered_day_skips_auth_and_slack(self):
        self.guard.delivered.return_value = True
        self.assertEqual(self.execute(), 0)
        self.mocks[2].assert_not_called()
        self.mocks[-1].assert_not_called()

    def test_success_reserves_before_post_and_confirms_after(self):
        trace = Mock()
        trace.attach_mock(self.guard.reserve, 'reserve')
        trace.attach_mock(self.mocks[-1], 'post')
        trace.attach_mock(self.guard.confirm, 'confirm')
        self.assertEqual(self.execute(), 0)
        self.assertEqual([c[0] for c in trace.mock_calls], ['reserve', 'post', 'confirm'])

    def test_auth_failure_does_not_reserve_or_post(self):
        self.result.session_valid = False
        self.assertEqual(self.execute(), 1)
        self.guard.reserve.assert_not_called()
        self.mocks[-1].assert_not_called()

    def test_collection_failure_can_be_retried(self):
        self.mocks[4].side_effect = RuntimeError('collection failed')
        self.assertEqual(self.execute(), 1)
        self.guard.reserve.assert_not_called()

    def test_uncertain_slack_send_keeps_reservation_unconfirmed(self):
        self.mocks[-1].side_effect = TimeoutError('unknown delivery outcome')
        self.assertEqual(self.execute(), 1)
        self.guard.reserve.assert_called_once()
        self.guard.confirm.assert_not_called()

    def test_reserved_day_blocks_post(self):
        self.guard.ensure_available.side_effect = RuntimeError('reserved')
        self.assertEqual(self.execute(), 1)
        self.mocks[-1].assert_not_called()

    def test_lost_claim_blocks_post(self):
        self.guard.reserve.return_value = False
        self.assertEqual(self.execute(), 1)
        self.mocks[-1].assert_not_called()

    def test_receipt_failure_after_post_fails_without_second_post(self):
        self.guard.confirm.side_effect = RuntimeError('GitHub unavailable')
        self.assertEqual(self.execute(), 1)
        self.mocks[-1].assert_called_once()


if __name__ == '__main__':
    unittest.main()
