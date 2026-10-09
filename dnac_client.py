"""Minimal Catalyst Center (DNAC) REST client built on requests (Python 3.8+)."""

import threading
import time

import requests
import urllib3

TOKEN_PATH = "/dna/system/api/v1/auth/token"
DEFAULT_TIMEOUT = 60
MAX_429_WAITS = 5


class DnacError(Exception):
    """Raised for any failed Catalyst Center API call."""


class DnacClient:
    def __init__(self, host, username, password, verify=True, timeout=DEFAULT_TIMEOUT):
        self._base_url = "https://{}:443".format(host)
        self._username = username
        self._password = password
        self._timeout = timeout
        self._session = requests.Session()
        self._session.verify = verify
        if not verify:
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        self._token = None
        self._auth_lock = threading.Lock()

    def _authenticate(self, stale_token=None):
        """Fetch a token once even when many threads hit 401/startup together."""
        with self._auth_lock:
            if self._token is not None and self._token != stale_token:
                return
            try:
                resp = self._session.post(
                    self._base_url + TOKEN_PATH,
                    auth=(self._username, self._password),
                    timeout=self._timeout,
                )
                resp.raise_for_status()
                self._token = resp.json()["Token"]
            except (requests.RequestException, KeyError, ValueError) as exc:
                raise DnacError("Authentication failed: {}".format(exc)) from exc

    def get(self, path, **params):
        """GET path with query params (None values dropped); returns parsed JSON."""
        params = {k: v for k, v in params.items() if v is not None}
        url = self._base_url + path
        reauthed = False
        waits = 0
        while True:
            if self._token is None:
                self._authenticate()
            token = self._token
            try:
                resp = self._session.get(
                    url, params=params, headers={"X-Auth-Token": token}, timeout=self._timeout
                )
            except requests.RequestException as exc:
                raise DnacError("GET {} failed: {}".format(path, exc)) from exc

            if resp.status_code == 401 and not reauthed:
                reauthed = True
                self._authenticate(stale_token=token)
                continue
            if resp.status_code == 429 and waits < MAX_429_WAITS:
                waits += 1
                try:
                    delay = float(resp.headers.get("Retry-After", 1))
                except ValueError:
                    delay = 1.0
                time.sleep(delay)
                continue
            if not resp.ok:
                raise DnacError(
                    "GET {} returned {}: {}".format(path, resp.status_code, resp.text[:300])
                )
            try:
                return resp.json()
            except ValueError as exc:
                raise DnacError("GET {} returned non-JSON body".format(path)) from exc
