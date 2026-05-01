import hashlib
import json
import http.client
from urllib.parse import urlencode, urlparse

from src.logger import get_logger


logger = get_logger(__name__)


class DocsDeadlineSync:
    def __init__(self, conf: dict):
        remote_conf = conf.get("remote_sync", {}) if isinstance(conf.get("remote_sync", {}), dict) else {}
        self.enabled = bool(remote_conf.get("enabled", False))
        self.host = str(remote_conf.get("host", "")).strip()
        self.port = int(remote_conf.get("port", 443))
        self.api_key = str(remote_conf.get("api_key", "")).strip()
        self.username = str(remote_conf.get("username", "")).strip()
        self.password = str(remote_conf.get("password", "")).strip()
        self.timeout = int(remote_conf.get("timeout", 60))
        self.protocol = str(remote_conf.get("protocol", "http")).strip().lower() or "http"
        self.base_path = str(remote_conf.get("base_path", "/app/api.php")).strip() or "/app/api.php"

    def is_configured(self) -> bool:
        return self.enabled and bool(self.host) and bool(self.api_key) and bool(self.username)

    def _base_url(self) -> str:
        return f"{self.protocol}://{self.host}:{self.port}{self.base_path}"

    def _request(self, *, method: str, query: dict | None = None, payload: dict | None = None) -> dict:
        if not self.api_key:
            raise ValueError("Remote sync api_key is required.")

        url = self._base_url()
        if query:
            url = f"{url}?{urlencode(query)}"
        data = None
        headers = {}
        if payload is not None:
            headers["Content-Type"] = "application/json"
            data = json.dumps(payload)
        headers["api_key"] = self.api_key
        parsed_url = urlparse(url)
        path_with_query = parsed_url.path or "/"
        if parsed_url.query:
            path_with_query = f"{path_with_query}?{parsed_url.query}"
        connection_cls = http.client.HTTPSConnection if parsed_url.scheme == "https" else http.client.HTTPConnection
        connection = connection_cls(parsed_url.hostname, parsed_url.port, timeout=self.timeout)
        try:
            connection.request(method=method, url=path_with_query, body=data, headers=headers)
            response = connection.getresponse()
            text = response.read().decode("utf-8", errors="replace")
            if 200 <= response.status < 300:
                logger.info(
                    "Remote sync response: method=%s url=%s status=%s body=%s",
                    method,
                    url,
                    response.status,
                    text,
                )
                return json.loads(text)
            if response.status == 500:
                logger.error(
                    "Remote sync server 500 response: method=%s url=%s body=%s",
                    method,
                    url,
                    text,
                )
            logger.error(
                "Remote sync HTTP error: method=%s url=%s status=%s body=%s",
                method,
                url,
                response.status,
                text,
            )
            raise RuntimeError(f"Remote sync HTTP error {response.status}")
        except Exception as exc:
            logger.error(
                "Remote sync request failed without HTTP response: method=%s url=%s error=%s",
                method,
                url,
                exc,
            )
            raise
        finally:
            connection.close()

    @staticmethod
    def _normalize_field_for_backend_hash(value: str) -> str:
        # Backend hash is computed from raw values as-is (no transformation),
        # after validation checks.
        return str(value or "")

    @classmethod
    def calculate_local_hash(cls, deadlines: list[dict]) -> str:
        row_hashes: list[str] = []
        for row in deadlines:
            combined = (
                f"{cls._normalize_field_for_backend_hash(row.get('name', ''))}"
                f"{cls._normalize_field_for_backend_hash(row.get('description', ''))}"
                f"{cls._normalize_field_for_backend_hash(row.get('date', ''))}"
                f"{cls._normalize_field_for_backend_hash(row.get('time', ''))}"
                f"{cls._normalize_field_for_backend_hash(row.get('status', ''))}"
            )
            row_hashes.append(hashlib.md5(combined.encode("utf-8")).hexdigest())
        return hashlib.md5("".join(row_hashes).encode("utf-8")).hexdigest()

    def fetch_remote_status_hash(self) -> str:
        response = self._request(method="GET", query={"id": "sync_status", "username": self.username})
        return str(response.get("hash", "")).strip()

    def push_deadlines(self, deadlines: list[dict]) -> dict:
        payload = {
            "id": "sync",
            "username": self.username,
            "password": self.password,
            "deadline_items": [
                {
                    "name": str(item.get("name", "")).strip(),
                    "description": str(item.get("description", "")).strip(),
                    "date": str(item.get("date", "")).strip(),
                    "time": str(item.get("time", "")).strip() or "-",
                    "status": str(item.get("status", "")).strip(),
                }
                for item in deadlines
            ],
        }
        return self._request(method="POST", payload=payload)
