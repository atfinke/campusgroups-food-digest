"""Optional event context upload. Failure must never block the daily Slack digest."""
from __future__ import annotations

import json
import logging
import os
from urllib.parse import urlparse
from urllib.request import Request, urlopen

LOGGER = logging.getLogger(__name__)


def publish_event_snapshot(result) -> bool:
    endpoint = os.environ.get('FEEDBACK_API_URL', '').rstrip('/')
    token = os.environ.get('FEEDBACK_API_TOKEN', '')
    if not endpoint and not token:
        return False
    if not endpoint or not token:
        LOGGER.warning('Feedback context upload not configured completely; digest will continue')
        return False
    parsed = urlparse(endpoint)
    if parsed.scheme != 'https' or not parsed.netloc or parsed.username or parsed.password or parsed.query or parsed.fragment:
        LOGGER.warning('Invalid feedback API URL; digest will continue')
        return False
    selected_urls = {event.event_url for event in result.food_events}
    snapshot = {
        'target_date': result.target_date.isoformat(),
        'events': [{
            'event_id': str(event.event_id), 'title': event.title,
            'organizer_name': event.organizer_name, 'time_text': event.time_text,
            'room_text': event.room_text, 'event_url': event.event_url,
            'in_digest': event.event_url in selected_urls,
        } for event in result.matching_events],
    }
    request = Request(endpoint + '/api/events', method='POST',
                      data=json.dumps(snapshot).encode('utf-8'),
                      headers={'Content-Type': 'application/json',
                               'Authorization': f'Bearer {token}'})
    try:
        with urlopen(request, timeout=10) as response:
            if response.status != 200:
                raise RuntimeError('Feedback upload failed')
        LOGGER.info('Uploaded feedback event context', extra={'event_count': len(snapshot['events'])})
        return True
    except Exception as exc:
        # Do not print the URL, bearer token, payload or response body.
        LOGGER.warning('Feedback context upload failed; digest will continue',
                       extra={'error_type': type(exc).__name__})
        return False
