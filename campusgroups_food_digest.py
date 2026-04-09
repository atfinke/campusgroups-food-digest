from __future__ import annotations

import argparse
import json
import logging
import os
import re
import ssl
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import date, datetime
from html import unescape
from pathlib import Path
from typing import Any, Literal, Sequence
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urljoin, urlparse
from urllib.request import HTTPSHandler, ProxyHandler, Request, build_opener
from zoneinfo import ZoneInfo

from playwright.sync_api import (
    BrowserContext,
    Error as PlaywrightError,
    Locator,
    Page,
    TimeoutError as PlaywrightTimeoutError,
    sync_playwright,
)
from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator

DEFAULT_BASE_URL = "https://kellogg.campusgroups.com"
DEFAULT_SSO_URL = "https://www.campusgroups.com/shibboleth/login?idp=kellogg"
DEFAULT_EVENTS_PATH = "/mobile_ws/v17/mobile_events_list"
DEFAULT_AUTH_CHECK_PATH = "/groups"
DEFAULT_EVENT_PATH_TEMPLATE = "/rsvp_boot?id={event_id}"
DEFAULT_LIMIT = 500
DEFAULT_MAX_WORKERS = 8
DEFAULT_REQUEST_TIMEOUT_SECONDS = 30
DEFAULT_REQUEST_RETRIES = 3
DEFAULT_REQUEST_RETRY_BACKOFF_SECONDS = 1.0
DEFAULT_TIMEZONE = ZoneInfo("America/Chicago")
DEFAULT_LOGIN_TIMEOUT_MILLISECONDS = 30_000
DEFAULT_LOGIN_TOTAL_TIMEOUT_SECONDS = 75
DEFAULT_LOGIN_MAX_STEPS = 12
LUNCH_WINDOW_START_MINUTES = 11 * 60
LUNCH_WINDOW_END_MINUTES = 15 * 60
SHORT_DATE_PATTERN = re.compile(
    r"(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun), [A-Z][a-z]{2} \d{1,2}, \d{4}"
)
LONG_DATE_PATTERN = re.compile(
    r"(?:Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday), \d{1,2} [A-Z][a-z]+ \d{4}"
)
USER_AGENT = "campusgroups-food-digest/1.0"
PRIVATE_LOCATION_TEXT = "Private Location (sign in to display)"
AUTHENTICATED_HOST = "kellogg.campusgroups.com"
FLOOR_ORDER = ["2nd Floor", "1st Floor", "Lower Level"]
DEFAULT_SPOTS_STATUS = "Unlimited"
DEFAULT_TITLE_MAX_LENGTH = 110
REVIEW_LABEL = "Review"
SESSION_WARNING_TEXT = "🔸 CampusGroups login failed. Check `NORTHWESTERN_NETID` and `NORTHWESTERN_PASSWORD`."
NETID_SELECTORS = [
    "input[name='IDToken1']",
    "input[name*='netid' i]",
    "input[id*='netid' i]",
    "input[type='email']",
    "input[type='text']",
]
PASSWORD_SELECTORS = [
    "input[name='IDToken2']",
    "input[name='password']",
    "input[name='passwd']",
    "input[name*='pass' i]",
    "input[id*='pass' i]",
    "input[type='password']",
]
SUBMIT_SELECTORS = [
    "button[type='submit']",
    "input[type='submit']",
    "button:has-text('Sign in')",
    "button:has-text('Log in')",
    "button:has-text('Login')",
    "button:has-text('Continue')",
    "button:has-text('Next')",
    "button:has-text('Verify')",
]
MFA_KEYWORDS = (
    "verification code",
    "one-time code",
    "passcode",
    "multi-factor",
    "multi factor",
    "authenticator",
    "duo",
    "approve sign in",
    "verify it",
)
LOGGER = logging.getLogger("campusgroups_food_digest")
RESERVED_LOG_RECORD_FIELDS = frozenset(logging.makeLogRecord({}).__dict__.keys())


@dataclass(frozen=True)
class LoginStepResult:
    action: str
    netid_selector: str | None = None
    password_selector: str | None = None
    submit_selector: str | None = None


class StructuredLogFormatter(logging.Formatter):
    default_time_format = "%Y-%m-%d %H:%M:%S"

    def format(self, record: logging.LogRecord) -> str:
        timestamp = self.formatTime(record, self.datefmt)
        extras = []
        for key, value in sorted(record.__dict__.items()):
            if key in RESERVED_LOG_RECORD_FIELDS or key.startswith("_"):
                continue
            extras.append(f"{key}={self._format_value(value)}")

        base = f"{timestamp} {record.levelname} {record.getMessage()}"
        if extras:
            base = f"{base} {' '.join(extras)}"

        if record.exc_info:
            return f"{base}\n{self.formatException(record.exc_info)}"
        return base

    @staticmethod
    def _format_value(value: Any) -> str:
        return json.dumps(value, sort_keys=True, default=str)


class RuntimeConfig(BaseModel):
    session_id: str | None = None
    slack_webhook_url: str | None = None
    netid: str | None = None
    password: str | None = None

    @model_validator(mode="after")
    def validate_auth_settings(self) -> "RuntimeConfig":
        if self.netid and self.password:
            return self
        raise ValueError("Set NORTHWESTERN_NETID/NORTHWESTERN_PASSWORD.")


class CampusGroupsListEntry(BaseModel):
    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    fields: str
    listing_separator: str | None = Field(default=None, alias="listingSeparator")
    is_separator_flag: str = Field(alias="p0")
    primary_value: str | None = Field(default=None, alias="p1")
    title: str | None = Field(default=None, alias="p3")
    event_dates_html: str | None = Field(default=None, alias="p4")
    event_location_html: str | None = Field(default=None, alias="p6")
    organizer_name: str | None = Field(default=None, alias="p9")
    event_url_path: str | None = Field(default=None, alias="p18")
    registration_status: str | None = Field(default=None, alias="p26")
    aria_details: str | None = Field(default=None, alias="p29")
    aria_details_with_location: str | None = Field(default=None, alias="p30")

    @property
    def is_separator(self) -> bool:
        return self.is_separator_flag.lower() == "true"


class PublicEvent(BaseModel):
    event_id: int
    title: str
    organizer_name: str
    start_date: date
    end_date: date
    time_text: str
    room_text: str | None = None
    event_url: str
    spots_status: str | None = None


class JsonLdLocation(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    type_name: str | None = Field(default=None, alias="@type")
    name: str | None = None
    address: str | None = None


class JsonLdEvent(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    name: str
    start_date: str | None = Field(default=None, alias="startDate")
    end_date: str | None = Field(default=None, alias="endDate")
    location: JsonLdLocation | None = None
    description: str | None = None


class EventDetail(BaseModel):
    description: str | None = None
    food_provided: bool
    location_name: str | None = None
    address: str | None = None
    start_datetime: datetime | None = None


class FoodEvent(BaseModel):
    title: str
    organizer_name: str
    room_text: str | None = None
    event_url: str
    spots_status: str | None = None


class DigestResult(BaseModel):
    base_url: str
    target_date: date
    total_entries: int
    matching_event_count: int
    food_events: list[FoodEvent]


class ScriptResult(BaseModel):
    session_valid: bool
    slack_text: str
    digest_result: DigestResult | None = None


class SlackTextStyle(BaseModel):
    bold: bool | None = None
    code: bool | None = None


class SlackTextElement(BaseModel):
    type: Literal["text"] = "text"
    text: str
    style: SlackTextStyle | None = None


class SlackLinkElement(BaseModel):
    type: Literal["link"] = "link"
    url: str
    text: str | None = None


SlackInlineElement = SlackTextElement | SlackLinkElement


class SlackRichTextSection(BaseModel):
    type: Literal["rich_text_section"] = "rich_text_section"
    elements: list[SlackInlineElement]


class SlackRichTextList(BaseModel):
    type: Literal["rich_text_list"] = "rich_text_list"
    style: Literal["bullet"] = "bullet"
    elements: list[SlackRichTextSection]
    indent: int | None = None


class SlackRichTextBlock(BaseModel):
    type: Literal["rich_text"] = "rich_text"
    elements: list[SlackRichTextSection | SlackRichTextList]


class SlackDividerBlock(BaseModel):
    type: Literal["divider"] = "divider"


class SlackPayload(BaseModel):
    text: str
    blocks: list[SlackRichTextBlock | SlackDividerBlock] | None = None


def configure_logging() -> None:
    if LOGGER.handlers:
        return

    handler = logging.StreamHandler()
    handler.setFormatter(StructuredLogFormatter())
    LOGGER.addHandler(handler)
    LOGGER.setLevel(logging.INFO)
    LOGGER.propagate = False


def normalize_whitespace(value: str) -> str:
    return re.sub(r"\s+", " ", value).strip()


def html_fragment_to_text(fragment: str | None) -> str | None:
    if fragment is None:
        return None

    expanded = re.sub(r"(?i)<br\s*/?>", "\n", fragment)
    expanded = re.sub(r"(?i)</p>", "\n", expanded)
    expanded = re.sub(r"(?i)<p[^>]*>", "", expanded)
    without_tags = re.sub(r"<[^>]+>", "", expanded)
    decoded = unescape(without_tags)
    lines = [normalize_whitespace(line) for line in decoded.splitlines()]
    meaningful_lines = [line for line in lines if line]
    if not meaningful_lines:
        return None
    return "\n".join(meaningful_lines)


def clean_room_text(value: str | None) -> str | None:
    if value is None:
        return None
    cleaned = value.rstrip(" ,")
    return cleaned or None


def is_private_location(value: str | None) -> bool:
    if value is None:
        return False
    return "private location" in value.casefold()


def parse_iso_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("Date must be in YYYY-MM-DD format.") from exc


def resolve_target_date(explicit_date: date | None) -> date:
    if explicit_date is not None:
        return explicit_date
    return datetime.now(DEFAULT_TIMEZONE).date()


def parse_abbreviated_date(value: str) -> date:
    return datetime.strptime(value, "%a, %b %d, %Y").date()


def parse_long_date(value: str) -> date:
    return datetime.strptime(value, "%A, %d %B %Y").date()


def extract_calendar_dates(*sources: str | None) -> list[date]:
    seen: set[date] = set()
    dates: list[date] = []

    for source in sources:
        if source is None:
            continue

        normalized = normalize_whitespace(source.replace("\n", " "))

        for match in SHORT_DATE_PATTERN.finditer(normalized):
            parsed = parse_abbreviated_date(match.group(0))
            if parsed not in seen:
                seen.add(parsed)
                dates.append(parsed)

        for match in LONG_DATE_PATTERN.finditer(normalized):
            parsed = parse_long_date(match.group(0))
            if parsed not in seen:
                seen.add(parsed)
                dates.append(parsed)

    return dates


def extract_event_date_range(entry: CampusGroupsListEntry) -> tuple[date, date] | None:
    dates = extract_calendar_dates(
        html_fragment_to_text(entry.event_dates_html),
        entry.aria_details,
        entry.aria_details_with_location,
    )
    if not dates:
        return None
    return dates[0], dates[-1]


def load_dotenv_if_present(path: Path) -> None:
    if not path.exists():
        return

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        parsed_value = value.strip().strip("'").strip('"')
        os.environ.setdefault(key.strip(), parsed_value)

    LOGGER.info(
        "Loaded environment file",
        extra={"path": str(path)},
    )


def load_runtime_config() -> RuntimeConfig:
    base_path = Path(__file__).resolve()
    load_dotenv_if_present(base_path.with_name(".env"))
    load_dotenv_if_present(base_path.with_name(".env.probe"))
    env = os.environ
    raw_config = {
        "slack_webhook_url": env.get("SLACK_WEBHOOK_URL"),
        "netid": env.get("NORTHWESTERN_NETID"),
        "password": env.get("NORTHWESTERN_PASSWORD"),
    }
    config = RuntimeConfig.model_validate(raw_config)
    LOGGER.info(
        "Loaded runtime configuration",
        extra={
            "has_netid": bool(config.netid),
            "has_password": bool(config.password),
            "has_slack_webhook": bool(config.slack_webhook_url),
        },
    )
    return config


def campusgroups_cookie_map(context: BrowserContext) -> dict[str, str]:
    cookie_map: dict[str, str] = {}
    for cookie in context.cookies([DEFAULT_BASE_URL]):
        cookie_map[cookie["name"]] = cookie["value"]
    return cookie_map


def wait_for_authenticated_cookies(
    context: BrowserContext, timeout_milliseconds: int
) -> dict[str, str] | None:
    deadline = time.monotonic() + timeout_milliseconds / 1000
    while time.monotonic() < deadline:
        cookie_map = campusgroups_cookie_map(context)
        if cookie_map.get("CG.SessionID") and cookie_map.get("cg_uid"):
            return cookie_map
        time.sleep(0.5)
    return None


def first_visible_locator(
    page: Page, selectors: list[str], timeout_milliseconds: int = 750
) -> tuple[Locator, str] | None:
    for selector in selectors:
        locator = page.locator(selector).first
        try:
            locator.wait_for(state="visible", timeout=timeout_milliseconds)
            return locator, selector
        except PlaywrightTimeoutError:
            continue
    return None


def click_submit(page: Page) -> str | None:
    submit_match = first_visible_locator(page, SUBMIT_SELECTORS)
    if submit_match is None:
        return None
    submit, selector = submit_match
    submit.click()
    return selector


def fill_locator(locator: Locator, value: str) -> None:
    locator.click()
    locator.fill(value)


def page_text(page: Page) -> str:
    try:
        body_text = page.locator("body").inner_text(timeout=5_000)
    except PlaywrightError:
        body_text = page.content()
    return normalize_whitespace(body_text)


def maybe_complete_login_step(page: Page, config: RuntimeConfig) -> LoginStepResult:
    body_text = page_text(page).casefold()
    detected_mfa_keyword = next(
        (keyword for keyword in MFA_KEYWORDS if keyword in body_text), None
    )
    if detected_mfa_keyword is not None:
        raise RuntimeError(
            "Northwestern login requested an additional verification step. "
            "This script currently supports password-only logins. "
            f"Detected keyword: {detected_mfa_keyword}."
        )

    netid_match = first_visible_locator(page, NETID_SELECTORS)
    password_match = first_visible_locator(page, PASSWORD_SELECTORS)
    netid_input = netid_match[0] if netid_match is not None else None
    password = password_match[0] if password_match is not None else None
    netid_selector = netid_match[1] if netid_match is not None else None
    password_selector = password_match[1] if password_match is not None else None

    if netid_input is not None and password is not None:
        fill_locator(netid_input, config.netid or "")
        fill_locator(password, config.password or "")
        submit_selector = click_submit(page)
        if submit_selector is None:
            password.press("Enter")
        return LoginStepResult(
            action="submitted_netid_and_password",
            netid_selector=netid_selector,
            password_selector=password_selector,
            submit_selector=submit_selector,
        )

    if netid_input is not None:
        fill_locator(netid_input, config.netid or "")
        submit_selector = click_submit(page)
        if submit_selector is None:
            netid_input.press("Enter")
        return LoginStepResult(
            action="submitted_netid",
            netid_selector=netid_selector,
            submit_selector=submit_selector,
        )

    if password is not None:
        fill_locator(password, config.password or "")
        submit_selector = click_submit(page)
        if submit_selector is None:
            password.press("Enter")
        return LoginStepResult(
            action="submitted_password",
            password_selector=password_selector,
            submit_selector=submit_selector,
        )

    return LoginStepResult(action="no_visible_login_inputs")


def create_authenticated_runtime_config(config: RuntimeConfig) -> RuntimeConfig:
    if not (config.netid and config.password):
        raise RuntimeError("Missing Northwestern credentials.")

    LOGGER.info(
        "Starting Northwestern login",
        extra={
            "sso_url": DEFAULT_SSO_URL,
            "login_step_timeout_ms": DEFAULT_LOGIN_TIMEOUT_MILLISECONDS,
            "overall_login_timeout_s": DEFAULT_LOGIN_TOTAL_TIMEOUT_SECONDS,
        },
    )

    with sync_playwright() as playwright:
        login_deadline = time.monotonic() + DEFAULT_LOGIN_TOTAL_TIMEOUT_SECONDS
        browser = playwright.chromium.launch(headless=True)
        context = browser.new_context()
        page = context.new_page()
        page.set_default_timeout(DEFAULT_LOGIN_TIMEOUT_MILLISECONDS)

        try:
            LOGGER.info(
                "Navigating to CampusGroups SSO entrypoint",
                extra={"url": DEFAULT_SSO_URL},
            )
            page.goto(DEFAULT_SSO_URL, wait_until="domcontentloaded")
            LOGGER.info("Loaded SSO entrypoint", extra={"current_url": page.url})

            for step_number in range(1, DEFAULT_LOGIN_MAX_STEPS + 1):
                if time.monotonic() >= login_deadline:
                    raise TimeoutError(
                        "CampusGroups login exceeded the overall timeout."
                    )

                cookie_map = wait_for_authenticated_cookies(context, 1_500)
                if cookie_map is not None:
                    LOGGER.info(
                        "Captured authenticated CampusGroups cookies",
                        extra={
                            "step": step_number,
                            "cookie_names": sorted(cookie_map.keys()),
                            "current_url": page.url,
                        },
                    )
                    return config.model_copy(
                        update={"session_id": cookie_map["CG.SessionID"]}
                    )

                current_host = urlparse(page.url).netloc
                LOGGER.info(
                    "Evaluating login page state",
                    extra={
                        "step": step_number,
                        "current_host": current_host,
                        "current_url": page.url,
                    },
                )
                if (
                    current_host == AUTHENTICATED_HOST
                    and "webapp/auth/login" not in page.url
                ):
                    LOGGER.info(
                        "Reached CampusGroups host before cookies were ready",
                        extra={"step": step_number, "current_url": page.url},
                    )
                    time.sleep(1.0)
                    continue

                step_result = maybe_complete_login_step(page, config)
                LOGGER.info(
                    "Completed login page interaction",
                    extra={
                        "step": step_number,
                        "action": step_result.action,
                        "netid_selector": step_result.netid_selector,
                        "password_selector": step_result.password_selector,
                        "submit_selector": step_result.submit_selector,
                        "current_url": page.url,
                    },
                )
                page.wait_for_timeout(1_500)
        except Exception:
            LOGGER.exception("CampusGroups login failed")
            raise
        finally:
            browser.close()
            LOGGER.info("Closed Playwright browser")

    raise RuntimeError("CampusGroups login did not produce an authenticated session.")


def build_opener_with_defaults():
    return build_opener(
        ProxyHandler({}),
        HTTPSHandler(context=ssl.create_default_context()),
    )


def fetch_text(url: str, headers: dict[str, str], timeout: int) -> str:
    request = Request(url, headers=headers)
    opener = build_opener_with_defaults()
    for attempt in range(1, DEFAULT_REQUEST_RETRIES + 1):
        LOGGER.info(
            "Starting HTTP request",
            extra={"url": url, "attempt": attempt, "timeout_s": timeout},
        )
        try:
            with opener.open(request, timeout=timeout) as response:
                charset = response.headers.get_content_charset() or "utf-8"
                body = response.read()
                LOGGER.info(
                    "Completed HTTP request",
                    extra={
                        "url": url,
                        "attempt": attempt,
                        "status_code": response.status,
                        "body_bytes": len(body),
                    },
                )
                return body.decode(charset)
        except HTTPError as exc:
            LOGGER.warning(
                "HTTP request returned an error response",
                extra={
                    "url": url,
                    "attempt": attempt,
                    "status_code": exc.code,
                    "retrying": exc.code >= 500 and attempt < DEFAULT_REQUEST_RETRIES,
                },
            )
            if exc.code < 500 or attempt == DEFAULT_REQUEST_RETRIES:
                raise exc
            time.sleep(DEFAULT_REQUEST_RETRY_BACKOFF_SECONDS * attempt)
        except (TimeoutError, URLError, ssl.SSLError) as exc:
            LOGGER.warning(
                "HTTP request failed before receiving a response",
                extra={
                    "url": url,
                    "attempt": attempt,
                    "error_type": type(exc).__name__,
                    "retrying": attempt < DEFAULT_REQUEST_RETRIES,
                },
            )
            if attempt == DEFAULT_REQUEST_RETRIES:
                raise exc
            time.sleep(DEFAULT_REQUEST_RETRY_BACKOFF_SECONDS * attempt)

    raise RuntimeError(f"Failed to fetch {url}.")


def post_json(url: str, payload: dict[str, object], timeout: int) -> None:
    LOGGER.info(
        "Posting Slack webhook payload",
        extra={
            "webhook_host": urlparse(url).netloc,
            "timeout_s": timeout,
            "block_count": len(payload.get("blocks", [])),
        },
    )
    request = Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "User-Agent": USER_AGENT,
        },
        method="POST",
    )
    opener = build_opener_with_defaults()
    try:
        with opener.open(request, timeout=timeout) as response:
            body = response.read().decode(
                response.headers.get_content_charset() or "utf-8"
            )
    except HTTPError as exc:
        error_body = exc.read().decode("utf-8", errors="replace").strip()
        raise RuntimeError(
            f"Slack webhook request failed with HTTP {exc.code}: {error_body or exc.reason}"
        ) from exc
    if body.strip().lower() != "ok":
        raise RuntimeError(f"Unexpected Slack webhook response: {body!r}")
    LOGGER.info(
        "Slack webhook accepted payload", extra={"webhook_host": urlparse(url).netloc}
    )


def session_cookie_header(config: RuntimeConfig) -> str:
    if config.session_id is None:
        raise RuntimeError("RuntimeConfig is missing session_id.")
    return f"CG.SessionID={config.session_id}"


def auth_check_url(config: RuntimeConfig) -> str:
    return urljoin(
        f"{DEFAULT_BASE_URL.rstrip('/')}/", DEFAULT_AUTH_CHECK_PATH.lstrip("/")
    )


def list_endpoint(config: RuntimeConfig, limit: int) -> str:
    query = urlencode(
        {
            "range": 0,
            "limit": limit,
            "filter4_contains": "OR",
            "filter4_notcontains": "OR",
            "order": "undefined",
            "search_word": "",
        }
    )
    base = urljoin(f"{DEFAULT_BASE_URL.rstrip('/')}/", DEFAULT_EVENTS_PATH.lstrip("/"))
    return f"{base}?{query}"


def fetch_list_payload(config: RuntimeConfig, limit: int) -> str:
    LOGGER.info(
        "Fetching CampusGroups events list",
        extra={"limit": limit, "list_url": list_endpoint(config, limit)},
    )
    headers = {
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Referer": f"{DEFAULT_BASE_URL.rstrip('/')}/events",
        "User-Agent": USER_AGENT,
        "X-Requested-With": "XMLHttpRequest",
        "Cookie": session_cookie_header(config),
    }
    return fetch_text(
        list_endpoint(config, limit),
        headers=headers,
        timeout=DEFAULT_REQUEST_TIMEOUT_SECONDS,
    )


def parse_list_entries(payload_text: str) -> list[CampusGroupsListEntry]:
    payload = json.loads(payload_text)
    if not isinstance(payload, list):
        raise ValueError("CampusGroups list payload was not a JSON array.")
    return [CampusGroupsListEntry.model_validate(item) for item in payload]


def normalize_spots_status(value: str | None) -> str | None:
    if value is None:
        return DEFAULT_SPOTS_STATUS
    cleaned = html_fragment_to_text(value)
    if cleaned is not None:
        return cleaned
    normalized = normalize_whitespace(unescape(value))
    return normalized or DEFAULT_SPOTS_STATUS


def build_event_url(config: RuntimeConfig, entry: CampusGroupsListEntry) -> str | None:
    if entry.event_url_path:
        return urljoin(DEFAULT_BASE_URL, entry.event_url_path)
    if entry.primary_value and entry.primary_value.isdigit():
        path = DEFAULT_EVENT_PATH_TEMPLATE.format(event_id=entry.primary_value)
        return urljoin(DEFAULT_BASE_URL, path)
    return None


def select_events_for_date(
    entries: Sequence[CampusGroupsListEntry], config: RuntimeConfig, target_date: date
) -> list[PublicEvent]:
    events: list[PublicEvent] = []

    for entry in entries:
        if entry.is_separator:
            continue

        if entry.primary_value is None or entry.title is None:
            continue

        if not entry.primary_value.isdigit():
            continue

        event_url = build_event_url(config, entry)
        if event_url is None:
            continue

        date_range = extract_event_date_range(entry)
        if date_range is None:
            continue

        start_date, end_date = date_range
        if not (start_date <= target_date <= end_date):
            continue

        events.append(
            PublicEvent(
                event_id=int(entry.primary_value),
                title=normalize_whitespace(entry.title),
                organizer_name=normalize_whitespace(
                    entry.organizer_name or "Unknown organizer"
                ),
                start_date=start_date,
                end_date=end_date,
                time_text=html_fragment_to_text(entry.event_dates_html) or "",
                room_text=clean_room_text(
                    html_fragment_to_text(entry.event_location_html)
                ),
                event_url=event_url,
                spots_status=normalize_spots_status(entry.registration_status),
            )
        )

    return events


def extract_json_ld_event(html_text: str) -> JsonLdEvent | None:
    pattern = re.compile(
        r"<script[^>]+type=[\"']application/ld\+json[\"'][^>]*>\s*(\{.*?\})\s*</script>",
        flags=re.DOTALL | re.IGNORECASE,
    )

    for match in pattern.finditer(html_text):
        raw_block = unescape(match.group(1))
        try:
            return JsonLdEvent.model_validate(json.loads(raw_block))
        except (json.JSONDecodeError, ValidationError):
            continue

    return None


def extract_meta_description(html_text: str) -> str | None:
    match = re.search(
        r'<meta\s+name="description"\s+content="([^"]+)"',
        html_text,
        flags=re.IGNORECASE,
    )
    if match is None:
        return None
    return normalize_whitespace(unescape(match.group(1)))


def parse_event_start_datetime(value: str | None) -> datetime | None:
    if value is None:
        return None

    normalized = value.strip()
    if not normalized:
        return None

    try:
        parsed = datetime.fromisoformat(normalized.replace("Z", "+00:00"))
    except ValueError:
        LOGGER.warning(
            "Failed to parse event start datetime",
            extra={"raw_start_datetime": value},
        )
        return None

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=DEFAULT_TIMEZONE)

    return parsed.astimezone(DEFAULT_TIMEZONE)


def is_lunch_start_datetime(start_datetime: datetime | None, target_date: date) -> bool:
    if start_datetime is None:
        return False

    local_start = start_datetime.astimezone(DEFAULT_TIMEZONE)
    if local_start.date() != target_date:
        return False

    start_minutes = (local_start.hour * 60) + local_start.minute
    return LUNCH_WINDOW_START_MINUTES <= start_minutes < LUNCH_WINDOW_END_MINUTES


def parse_event_detail_html(html_text: str) -> EventDetail:
    json_ld = extract_json_ld_event(html_text)
    description = None
    location_name = None
    address = None
    start_datetime = None

    if json_ld is not None:
        description = normalize_whitespace(json_ld.description or "") or None
        start_datetime = parse_event_start_datetime(json_ld.start_date)
        if json_ld.location is not None:
            location_name = clean_room_text(
                html_fragment_to_text(json_ld.location.name)
            )
            address = clean_room_text(html_fragment_to_text(json_ld.location.address))

    if description is None:
        description = extract_meta_description(html_text)

    food_provided = (
        re.search(r">\s*Food Provided\s*<", html_text, flags=re.IGNORECASE) is not None
    )

    return EventDetail(
        description=description,
        food_provided=food_provided,
        location_name=location_name,
        address=address,
        start_datetime=start_datetime,
    )


def fetch_event_detail(event_url: str, config: RuntimeConfig) -> EventDetail:
    LOGGER.info("Fetching event detail page", extra={"event_url": event_url})
    headers = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "User-Agent": USER_AGENT,
        "Cookie": session_cookie_header(config),
    }
    html_text = fetch_text(
        event_url, headers=headers, timeout=DEFAULT_REQUEST_TIMEOUT_SECONDS
    )
    detail = parse_event_detail_html(html_text)
    LOGGER.info(
        "Parsed event detail page",
        extra={
            "event_url": event_url,
            "food_provided": detail.food_provided,
            "has_location_name": detail.location_name is not None,
            "private_location": is_private_location(detail.location_name),
            "start_datetime": (
                detail.start_datetime.isoformat()
                if detail.start_datetime is not None
                else None
            ),
        },
    )
    return detail


def build_food_event(
    event: PublicEvent, config: RuntimeConfig, target_date: date
) -> FoodEvent | None:
    detail = fetch_event_detail(event.event_url, config)
    if not detail.food_provided:
        LOGGER.info(
            "Skipping event without food",
            extra={"event_id": event.event_id, "event_title": event.title},
        )
        return None

    if not is_lunch_start_datetime(detail.start_datetime, target_date):
        LOGGER.info(
            "Skipping event outside lunch window",
            extra={
                "event_id": event.event_id,
                "event_title": event.title,
                "start_datetime": (
                    detail.start_datetime.isoformat()
                    if detail.start_datetime is not None
                    else None
                ),
                "lunch_window_start": "11:00",
                "lunch_window_end": "15:00",
            },
        )
        return None

    room_text = event.room_text
    if detail.location_name is not None and not is_private_location(
        detail.location_name
    ):
        if room_text is None or is_private_location(room_text):
            room_text = detail.location_name

    food_event = FoodEvent(
        title=event.title,
        organizer_name=event.organizer_name,
        room_text=room_text,
        event_url=event.event_url,
        spots_status=event.spots_status,
    )
    LOGGER.info(
        "Built food event",
        extra={
            "event_id": event.event_id,
            "event_title": event.title,
            "room_text": food_event.room_text,
        },
    )
    return food_event


def collect_food_events(
    config: RuntimeConfig,
    target_date: date,
) -> DigestResult:
    LOGGER.info(
        "Collecting food events", extra={"target_date": target_date.isoformat()}
    )
    entries = parse_list_entries(fetch_list_payload(config, DEFAULT_LIMIT))
    matching_events = select_events_for_date(
        entries, config=config, target_date=target_date
    )
    LOGGER.info(
        "Selected date-matching events",
        extra={
            "target_date": target_date.isoformat(),
            "total_entries": len(entries),
            "matching_event_count": len(matching_events),
        },
    )

    if not matching_events:
        food_events: list[FoodEvent] = []
    else:
        worker_count = max(1, min(DEFAULT_MAX_WORKERS, len(matching_events)))
        LOGGER.info(
            "Fetching event details in parallel",
            extra={
                "worker_count": worker_count,
                "matching_event_count": len(matching_events),
                "lunch_window_start": "11:00",
                "lunch_window_end": "15:00",
            },
        )
        with ThreadPoolExecutor(max_workers=worker_count) as executor:
            food_events = [
                food_event
                for food_event in executor.map(
                    lambda event: build_food_event(event, config, target_date),
                    matching_events,
                )
                if food_event is not None
            ]

    digest_result = DigestResult(
        base_url=DEFAULT_BASE_URL,
        target_date=target_date,
        total_entries=len(entries),
        matching_event_count=len(matching_events),
        food_events=food_events,
    )
    LOGGER.info(
        "Finished collecting food events",
        extra={
            "target_date": target_date.isoformat(),
            "food_event_count": len(food_events),
            "matching_event_count": len(matching_events),
        },
    )
    return digest_result


def validate_session(config: RuntimeConfig) -> bool:
    LOGGER.info(
        "Validating authenticated CampusGroups session",
        extra={"auth_check_url": auth_check_url(config)},
    )
    headers = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "User-Agent": USER_AGENT,
        "Cookie": session_cookie_header(config),
    }
    request = Request(auth_check_url(config), headers=headers)
    opener = build_opener_with_defaults()
    with opener.open(request, timeout=DEFAULT_REQUEST_TIMEOUT_SECONDS) as response:
        final_url = response.geturl()
    session_valid = "/webapp/auth/login" not in final_url
    LOGGER.info(
        "Validated authenticated CampusGroups session",
        extra={"session_valid": session_valid, "final_url": final_url},
    )
    return session_valid


def classify_floor(room_text: str | None) -> str | None:
    if room_text is None or is_private_location(room_text):
        return None

    normalized = room_text.upper()
    if re.search(r"\b2\d{3}\b", normalized):
        return "2nd Floor"
    if re.search(r"\b1\d{3}\b", normalized):
        return "1st Floor"
    if re.search(r"\bL\d+\b", normalized):
        return "Lower Level"
    return None


def format_target_date(target_date: date) -> str:
    return target_date.strftime("%A, %B %-d")


def normalize_room_label(room_text: str | None) -> str:
    if room_text is None:
        return "Unknown room"
    if is_private_location(room_text):
        return "Requires Session ID"
    normalized = normalize_whitespace(room_text)
    normalized = re.sub(r"\s*-\s*(\d{3,4})\b", r" \1", normalized)
    normalized = re.sub(r"\b([A-Z]{2,}?)(L?\d{3,4})\b", r"\1 \2", normalized)
    return normalized


def truncate_title(title: str, max_length: int = DEFAULT_TITLE_MAX_LENGTH) -> str:
    normalized = normalize_whitespace(title)
    if len(normalized) <= max_length:
        return normalized

    cutoff = max_length - 3
    truncated = normalized[:cutoff].rstrip()
    last_space = truncated.rfind(" ")
    if last_space >= max(0, cutoff - 24):
        truncated = truncated[:last_space].rstrip()
    return f"{truncated}..."


def slack_text_style(
    *, bold: bool = False, code: bool = False
) -> SlackTextStyle | None:
    style = SlackTextStyle(
        bold=True if bold else None,
        code=True if code else None,
    )
    if style.bold is None and style.code is None:
        return None
    return style


def slack_text_element(
    text: str, *, bold: bool = False, code: bool = False
) -> SlackTextElement:
    return SlackTextElement(
        text=text,
        style=slack_text_style(bold=bold, code=code),
    )


def slack_link_element(url: str, text: str) -> SlackLinkElement:
    return SlackLinkElement(url=url, text=text)


def format_event_lines(event: FoodEvent) -> list[str]:
    title = truncate_title(event.title)
    lines = [
        f"- *{normalize_room_label(event.room_text)}*",
        f"  - <{event.event_url}|{title}>",
        f"  - {event.organizer_name}",
    ]
    lines.append(f"  - Status: {event.spots_status or DEFAULT_SPOTS_STATUS}")
    return lines


def group_food_events(
    food_events: Sequence[FoodEvent],
) -> tuple[dict[str, list[FoodEvent]], list[FoodEvent]]:
    grouped: dict[str, list[FoodEvent]] = {label: [] for label in FLOOR_ORDER}
    review: list[FoodEvent] = []

    for event in food_events:
        floor = classify_floor(event.room_text)
        if floor is None:
            review.append(event)
            continue
        grouped[floor].append(event)

    return grouped, review


def build_event_room_list(event: FoodEvent) -> SlackRichTextList:
    return SlackRichTextList(
        elements=[
            SlackRichTextSection(
                elements=[
                    slack_text_element(normalize_room_label(event.room_text), bold=True)
                ]
            )
        ]
    )


def build_event_detail_list(event: FoodEvent) -> SlackRichTextList:
    title = truncate_title(event.title)
    items = [
        SlackRichTextSection(elements=[slack_link_element(event.event_url, title)]),
        SlackRichTextSection(elements=[slack_text_element(event.organizer_name)]),
        SlackRichTextSection(
            elements=[
                slack_text_element("Status: "),
                slack_text_element(event.spots_status or DEFAULT_SPOTS_STATUS),
            ]
        ),
    ]
    return SlackRichTextList(elements=items, indent=1)


def build_floor_elements(
    floor: str, events: Sequence[FoodEvent]
) -> list[SlackRichTextSection | SlackRichTextList]:
    elements: list[SlackRichTextSection | SlackRichTextList] = [
        SlackRichTextSection(elements=[slack_text_element(floor, bold=True)])
    ]
    for event in events:
        elements.append(build_event_room_list(event))
        elements.append(build_event_detail_list(event))
    return elements


def build_section_gap() -> SlackRichTextSection:
    return SlackRichTextSection(elements=[slack_text_element("\n")])


def build_slack_text(result: DigestResult | None, session_valid: bool) -> str:
    if result is None or not result.food_events:
        target = (
            result.target_date.isoformat() if result is not None else "unknown date"
        )
        sections = [f"No lunch events with food found for {target}."]
        if not session_valid:
            sections.append(SESSION_WARNING_TEXT)
        return "\n\n".join(sections)

    grouped, review = group_food_events(result.food_events)

    sections: list[str] = []
    for floor in FLOOR_ORDER:
        floor_events = grouped[floor]
        if not floor_events:
            continue
        lines = "\n".join(
            line for event in floor_events for line in format_event_lines(event)
        )
        sections.append(f"*{floor}*:\n\n{lines}")

    if review:
        lines = "\n".join(
            line for event in review for line in format_event_lines(event)
        )
        sections.append(f"*{REVIEW_LABEL}*:\n\n{lines}")

    if not session_valid:
        sections.append(SESSION_WARNING_TEXT)

    header = f"*Lunch Events with Food for {format_target_date(result.target_date)}*"
    return "\n\n".join([header, *sections])


def build_slack_payload(
    result: DigestResult | None, session_valid: bool
) -> SlackPayload:
    text = build_slack_text(result, session_valid=session_valid)

    if not session_valid:
        warning_block = SlackRichTextBlock(
            elements=[
                SlackRichTextSection(
                    elements=[
                        slack_text_element("🔸 CampusGroups login failed. Check "),
                        slack_text_element("NORTHWESTERN_NETID", code=True),
                        slack_text_element(" and "),
                        slack_text_element("NORTHWESTERN_PASSWORD", code=True),
                        slack_text_element("."),
                    ]
                )
            ]
        )
    else:
        warning_block = None

    if result is None:
        if warning_block is not None:
            return SlackPayload(text=text, blocks=[warning_block])
        return SlackPayload(text=text)

    header_block = SlackRichTextBlock(
        elements=[
            SlackRichTextSection(
                elements=[
                    slack_text_element(
                        f"Lunch Events with Food for {format_target_date(result.target_date)}",
                        bold=True,
                    )
                ]
            )
        ]
    )

    if not result.food_events:
        blocks: list[SlackRichTextBlock | SlackDividerBlock] = [
            header_block,
            SlackDividerBlock(),
            SlackRichTextBlock(
                elements=[
                    SlackRichTextSection(
                        elements=[slack_text_element("No Lunch Events", bold=True)]
                    )
                ]
            ),
        ]
        if warning_block is not None:
            blocks.extend([SlackDividerBlock(), warning_block])
        return SlackPayload(
            text=text,
            blocks=blocks,
        )

    grouped, review = group_food_events(result.food_events)
    section_groups: list[list[SlackRichTextSection | SlackRichTextList]] = []

    for floor in FLOOR_ORDER:
        floor_events = grouped[floor]
        if not floor_events:
            continue
        section_groups.append(build_floor_elements(floor, floor_events))

    if review:
        section_groups.append(build_floor_elements(REVIEW_LABEL, review))

    body_elements: list[SlackRichTextSection | SlackRichTextList] = []
    for index, section_group in enumerate(section_groups):
        if index > 0:
            body_elements.append(build_section_gap())
        body_elements.extend(section_group)

    blocks = [
        header_block,
        SlackDividerBlock(),
        SlackRichTextBlock(elements=body_elements),
    ]
    if warning_block is not None:
        blocks.extend([SlackDividerBlock(), warning_block])

    return SlackPayload(text=text, blocks=blocks)


def parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Find CampusGroups lunch events with food and optionally post them to Slack."
    )
    parser.add_argument(
        "--date",
        type=parse_iso_date,
        default=None,
        help="Calendar date to inspect in YYYY-MM-DD format. Defaults to the current local date.",
    )
    parser.add_argument(
        "--send-slack",
        action="store_true",
        help="Post the formatted message to the Slack webhook in SLACK_WEBHOOK_URL.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print JSON output instead of plain text.",
    )
    return parser.parse_args(argv)


def run(config: RuntimeConfig, target_date: date) -> ScriptResult:
    LOGGER.info("Running digest", extra={"target_date": target_date.isoformat()})
    session_valid = validate_session(config)
    digest_result = collect_food_events(
        config=config,
        target_date=target_date,
    )
    result = ScriptResult(
        session_valid=session_valid,
        slack_text=build_slack_text(digest_result, session_valid=session_valid),
        digest_result=digest_result,
    )
    LOGGER.info(
        "Completed digest run",
        extra={
            "target_date": target_date.isoformat(),
            "session_valid": session_valid,
            "food_event_count": len(digest_result.food_events),
        },
    )
    return result


def main(argv: Sequence[str] | None = None) -> int:
    configure_logging()
    try:
        args = parse_args(sys.argv[1:] if argv is None else argv)
        LOGGER.info(
            "Parsed CLI arguments",
            extra={
                "json_output": args.json,
                "send_slack": args.send_slack,
                "date": args.date.isoformat() if args.date else None,
            },
        )
        config = create_authenticated_runtime_config(load_runtime_config())
        target_date = resolve_target_date(args.date)
        result = run(
            config=config,
            target_date=target_date,
        )

        if args.send_slack:
            if config.slack_webhook_url is None:
                raise RuntimeError(
                    "SLACK_WEBHOOK_URL is required when --send-slack is used."
                )
            post_json(
                config.slack_webhook_url,
                build_slack_payload(
                    result.digest_result,
                    session_valid=result.session_valid,
                ).model_dump(mode="json", exclude_none=True),
                timeout=DEFAULT_REQUEST_TIMEOUT_SECONDS,
            )

        if args.json:
            sys.stdout.write(
                f"{json.dumps(result.model_dump(mode='json'), indent=2)}\n"
            )
        else:
            sys.stdout.write(f"{result.slack_text}\n")

        exit_code = 0 if result.session_valid else 2
        LOGGER.info("Exiting digest process", extra={"exit_code": exit_code})
        return exit_code
    except Exception:
        LOGGER.exception("Digest execution failed")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
