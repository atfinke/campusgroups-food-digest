"""Durable daily delivery claims in GitHub refs; never retry an uncertain Slack send."""
from __future__ import annotations

import json
import os
from datetime import date
from urllib.error import HTTPError
from urllib.request import Request, urlopen


class DeliveryGuard:
    def __init__(self, repository: str, token: str, commit: str):
        if not all((repository, token, commit)):
            raise RuntimeError("Delivery guard requires GITHUB_REPOSITORY, GITHUB_TOKEN and GITHUB_SHA")
        self.base = f"https://api.github.com/repos/{repository}/git"
        self.token = token
        self.commit = commit

    @classmethod
    def from_environment(cls):
        return cls(*(os.environ.get(key, "") for key in
                     ("GITHUB_REPOSITORY", "GITHUB_TOKEN", "GITHUB_SHA")))

    @staticmethod
    def ref(day: date, state: str) -> str:
        return f"tags/food-digest/{day.isoformat()}/{state}"

    def request(self, path: str, body=None):
        request = Request(self.base + path,
                          data=json.dumps(body).encode() if body is not None else None,
                          headers={"Authorization": f"Bearer {self.token}",
                                   "Accept": "application/vnd.github+json",
                                   "X-GitHub-Api-Version": "2022-11-28",
                                   "Content-Type": "application/json",
                                   "User-Agent": "campusgroups-food-digest"})
        with urlopen(request, timeout=30) as response:
            return json.load(response)

    def exists(self, day: date, state: str) -> bool:
        try:
            self.request("/ref/" + self.ref(day, state))
            return True
        except HTTPError as error:
            if error.code == 404:
                return False
            raise

    def delivered(self, day: date) -> bool:
        return self.exists(day, "delivered")

    def ensure_available(self, day: date):
        if self.exists(day, "reserved"):
            raise RuntimeError(f"Delivery {day} is reserved but not confirmed; inspect Slack before clearing the reservation")

    def reserve(self, day: date) -> bool:
        # Ref creation is atomic across all runs, including different branches.
        try:
            self.request("/refs", {"ref": "refs/" + self.ref(day, "reserved"),
                                   "sha": self.commit})
            return True
        except HTTPError as error:
            if error.code == 422 and self.exists(day, "reserved"):
                return False
            raise

    def confirm(self, day: date):
        self.request("/refs", {"ref": "refs/" + self.ref(day, "delivered"),
                               "sha": self.commit})
