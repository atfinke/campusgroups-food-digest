import io
import logging
import time
import unittest
from datetime import date
from email.message import Message
from unittest.mock import Mock, patch
from urllib.error import HTTPError
from urllib.request import HTTPSHandler, Request, build_opener, HTTPCookieProcessor
from urllib.response import addinfourl

import campusgroups_food_digest as d


BASE = d.DEFAULT_BASE_URL


def cookie(name, value="test", **kwargs):
    return dict(name=name, value=value, domain=d.AUTHENTICATED_HOST,
                path="/", secure=True, expires=-1) | kwargs


def config(cookies=None):
    return d.RuntimeConfig(netid="test", password="test", session_id="session",
                           browser_storage_state={"cookies": cookies or [
                               cookie("CG.SessionID", "session"),
                               cookie("cg_uid", "uid"), cookie("cg_auth", "auth"),
                           ]})


class FakeHTTPS(HTTPSHandler):
    """Exercise real urllib redirect and cookie processing without network access."""
    def __init__(self, responses):
        super().__init__()
        self.responses = iter(responses)
        self.requests = []

    def https_open(self, request):
        self.requests.append(request)
        code, header_values = next(self.responses)
        headers = Message()
        for name, value in header_values.items():
            headers[name] = value
        response = addinfourl(io.BytesIO(b"groups"), headers, request.full_url, code)
        response.msg = "test response"
        return response


class SessionValidationTests(unittest.TestCase):
    def test_companion_auth_cookies_are_preserved(self):
        header = d.session_cookie_header(config(), BASE + "/groups")
        self.assertEqual(set(header.split("; ")), {
            "CG.SessionID=session", "cg_uid=uid", "cg_auth=auth",
        })

    def test_constructed_client_has_only_the_custom_redirect_handler(self):
        handler = d.SessionRedirectHandler()
        opener = d.build_opener_with_defaults(handler)
        redirect_handlers = [item for item in opener.handlers if isinstance(item, d.HTTPRedirectHandler)]
        self.assertEqual(redirect_handlers, [handler])

    def test_event_client_still_blocks_external_redirect_before_following(self):
        transport = FakeHTTPS([(302, {"Location": "https://partiful.com/e/example"})])
        with patch.object(d, "build_opener_with_defaults", side_effect=lambda *handlers: build_opener(transport, *handlers)):
            with self.assertRaises(d.ExternalEventRedirect):
                d.fetch_text(BASE + "/rsvp_boot?id=1", {"Cookie": "CG.SessionID=test"}, 30)
        self.assertEqual(len(transport.requests), 1)

    def test_cookies_respect_domain_path_and_expiry(self):
        cookies = [cookie("CG.SessionID"),
                   cookie("parent", domain=".campusgroups.com"),
                   cookie("other", domain="idp.example.invalid"),
                   cookie("other_host", domain="www.campusgroups.com"),
                   cookie("wrong_path", path="/private"),
                   cookie("expired", expires=time.time() - 100)]
        header = d.session_cookie_header(config(cookies), BASE + "/groups")
        self.assertEqual(set(header.split("; ")), {"CG.SessionID=test", "parent=test"})
        jar = d.session_cookie_jar(config(cookies))
        for url in ("https://evil.invalid/", "http://kellogg.campusgroups.com/"):
            request = Request(url)
            jar.add_cookie_header(request)
            self.assertIsNone(request.get_header("Cookie"))

    def test_legacy_session_only_config_remains_supported(self):
        cfg = d.RuntimeConfig(netid="test", password="test", session_id="legacy")
        self.assertEqual(d.session_cookie_header(cfg), "CG.SessionID=legacy")

    def validate(self, responses):
        transport = FakeHTTPS(responses)
        with patch.object(d, "build_opener_with_defaults", side_effect=lambda *handlers: build_opener(transport, *handlers)):
            result = d.validate_session(config())
        return result, transport

    def test_redirect_refreshes_cookies_and_validates(self):
        result, transport = self.validate([
            (302, {"Location": "/groups/", "Set-Cookie": "cg_auth=renewed; Path=/; Secure"}),
            (200, {}),
        ])
        self.assertTrue(result)
        self.assertIn("cg_auth=auth", transport.requests[0].get_header("Cookie"))
        self.assertIn("cg_auth=renewed", transport.requests[1].get_header("Cookie"))
        self.assertIn("cg_uid=uid", transport.requests[1].get_header("Cookie"))

    def test_redirect_loop_is_bounded_and_safe_to_log(self):
        stream = io.StringIO()
        handler = logging.StreamHandler(stream)
        handler.setFormatter(d.StructuredLogFormatter())
        d.LOGGER.addHandler(handler)
        old_level = d.LOGGER.level
        d.LOGGER.setLevel(logging.INFO)
        self.addCleanup(d.LOGGER.removeHandler, handler)
        self.addCleanup(d.LOGGER.setLevel, old_level)
        transport = FakeHTTPS([(302, {"Location": f"/groups?token=secret-{i}"})
                               for i in range(10)])
        with patch.object(d, "build_opener_with_defaults", side_effect=lambda *handlers: build_opener(transport, *handlers)):
            with self.assertRaisesRegex(d.SessionValidationError, "redirect limit") as caught:
                d.validate_session(config())
        self.assertEqual(len(transport.requests), d.MAX_SESSION_REDIRECTS + 1)
        self.assertNotIn("secret", stream.getvalue() + str(caught.exception))
        self.assertNotIn("cg_auth", stream.getvalue())

    def test_login_or_external_redirect_never_followed(self):
        for destination in ("/webapp/auth/login?token=secret", "https://evil.invalid/?token=secret",
                            "https://kellogg.campusgroups.com.evil.invalid/",
                            "https://user:secret@kellogg.campusgroups.com/",
                            "http://kellogg.campusgroups.com/", "https://kellogg.campusgroups.com:8443/"):
            transport = FakeHTTPS([(302, {"Location": destination})])
            with self.subTest(destination=destination), patch.object(
                d, "build_opener_with_defaults", side_effect=lambda *handlers: build_opener(transport, *handlers)
            ):
                with self.assertRaises(d.SessionValidationError) as caught:
                    d.validate_session(config())
                self.assertEqual(len(transport.requests), 1)
                self.assertNotIn("secret", str(caught.exception))

    def test_http_error_is_sanitized(self):
        opener = Mock()
        opener.open.side_effect = HTTPError(BASE + "/?secret=token", 503, "secret", {}, None)
        with patch.object(d, "build_opener_with_defaults", return_value=opener):
            with self.assertRaises(d.SessionValidationError) as caught:
                d.validate_session(config())
        self.assertNotIn("secret", str(caught.exception))
        self.assertTrue(caught.exception.__suppress_context__)

    def test_invalid_session_stops_before_collection(self):
        with patch.object(d, "validate_session", return_value=False), \
             patch.object(d, "collect_food_events") as collect:
            with self.assertRaises(d.SessionValidationError):
                d.run(config(), date(2026, 11, 17))
            collect.assert_not_called()

    def test_validation_failure_does_not_reserve_post_or_confirm(self):
        guard = Mock()
        guard.delivered.return_value = False
        with patch.object(d.DeliveryGuard, "from_environment", return_value=guard), \
             patch.object(d, "load_runtime_config", return_value=config()), \
             patch.object(d, "create_authenticated_runtime_config", return_value=config()), \
             patch.object(d, "validate_session", side_effect=d.SessionValidationError("not verified")), \
             patch.object(d, "collect_food_events") as collect, \
             patch.object(d, "post_json") as post:
            self.assertEqual(d.main(["--date", "2026-11-17", "--send-slack", "--github-delivery-guard"]), 1)
            collect.assert_not_called()
            guard.reserve.assert_not_called()
            guard.confirm.assert_not_called()
            post.assert_not_called()


if __name__ == "__main__":
    unittest.main()
