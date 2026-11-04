import unittest
from unittest.mock import Mock, patch

import campusgroups_food_digest as d


class LoginChoiceTests(unittest.TestCase):
    def test_login_choice_does_not_count_as_authenticated_success(self):
        page, choice = self.page()
        context = Mock()
        context.new_page.return_value = page
        context.storage_state.return_value = {"cookies": []}
        browser = Mock()
        browser.new_context.return_value = context
        runtime = Mock()
        runtime.chromium.launch.return_value = browser
        config = d.RuntimeConfig(netid="test", password="test")
        with patch.object(d, "sync_playwright") as playwright, \
             patch.object(d, "wait_for_authenticated_cookies", side_effect=[None, {"CG.SessionID": "test-session", "cg_uid": "test-user"}]) as cookies, \
             patch.object(d, "page_text", return_value="Choose login"), \
             patch.object(d, "maybe_complete_login_step") as fill:
            playwright.return_value.__enter__.return_value = runtime
            result = d.create_authenticated_runtime_config(config)
        self.assertEqual(result.session_id, "test-session")
        self.assertEqual(cookies.call_count, 2)
        choice.click.assert_called_once()
        fill.assert_not_called()
        browser.close.assert_called_once()

    def test_persistent_choice_fails_closed_and_closes_browser(self):
        page, choice = self.page()
        browser = Mock()
        browser.new_context.return_value.new_page.return_value = page
        runtime = Mock()
        runtime.chromium.launch.return_value = browser
        with patch.object(d, "sync_playwright") as playwright, \
             patch.object(d, "wait_for_authenticated_cookies", return_value=None), \
             patch.object(d, "page_text", return_value="Choose login"):
            playwright.return_value.__enter__.return_value = runtime
            with self.assertRaisesRegex(RuntimeError, "redirect limit"):
                d.create_authenticated_runtime_config(d.RuntimeConfig(netid="test", password="test"))
        self.assertEqual(choice.click.call_count, 2)
        browser.close.assert_called_once()

    def page(self, url="https://kellogg.campusgroups.com/webapp/auth/login"):
        page = Mock(url=url)
        choice = page.locator.return_value
        choice.count.return_value = 1
        choice.is_visible.return_value = True
        choice.is_enabled.return_value = True
        return page, choice

    def test_observed_choice_follows_only_known_school_link(self):
        page, choice = self.page()
        with patch.object(d, "page_text", return_value="Login Choose your login method Kellogg Login JV Accounts"):
            self.assertTrue(d.follow_kellogg_login_choice(page, 0))
        page.locator.assert_called_once_with(f'a[href="{d.DEFAULT_SSO_URL}"]')
        choice.click.assert_called_once()

    def test_nonmatching_origins_and_routes_are_ignored(self):
        for url in ("https://example.invalid/webapp/auth/login", "http://kellogg.campusgroups.com/webapp/auth/login",
                    "https://kellogg.campusgroups.com/webapp/auth/login/password", "https://kellogg.campusgroups.com/web_app"):
            page, choice = self.page(url)
            self.assertFalse(d.follow_kellogg_login_choice(page, 0))
            choice.click.assert_not_called()

    def test_redirect_limit_stops_without_click(self):
        page, choice = self.page()
        with patch.object(d, "page_text", return_value="Choose login"), self.assertRaisesRegex(RuntimeError, "redirect limit"):
            d.follow_kellogg_login_choice(page, 2)
        choice.click.assert_not_called()

    def test_mfa_is_not_bypassed(self):
        page, choice = self.page()
        with patch.object(d, "page_text", return_value=d.MFA_KEYWORDS[0]), self.assertRaisesRegex(RuntimeError, "verification"):
            d.follow_kellogg_login_choice(page, 0)
        choice.click.assert_not_called()

    def test_missing_hidden_ambiguous_or_disabled_link_is_not_clicked(self):
        for attr, value in (("count", 0), ("count", 2), ("is_visible", False), ("is_enabled", False)):
            page, choice = self.page()
            getattr(choice, attr).return_value = value
            with patch.object(d, "page_text", return_value="Choose login"):
                self.assertFalse(d.follow_kellogg_login_choice(page, 0))
            choice.click.assert_not_called()
