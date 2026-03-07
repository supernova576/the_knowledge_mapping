import json
import sqlite3
import traceback
from datetime import datetime
from pathlib import Path
from sys import exit as adieu

from .logger import get_logger


logger = get_logger(__name__)


class db:
    def __init__(self) -> None:
        try:
            path = Path(__file__).resolve().parent.parent / "conf.json"
            with open(path, "r", encoding="utf-8") as f:
                j = json.loads(f.read())
                self.db_path: Path = Path(__file__).resolve().parent.parent / j["db"]["db_path"]

            self.db_path.parent.mkdir(parents=True, exist_ok=True)

            self.conn = sqlite3.connect(str(self.db_path), check_same_thread=False)
            self.conn.row_factory = sqlite3.Row
            self.cursor = self.conn.cursor()

            self.__init_db()
            logger.info("Database connected at %s", self.db_path)
        except Exception:
            logger.error("Database initialization failed\n%s", traceback.format_exc())
            adieu(1)

    def __init_db(self) -> None:
        try:
            if not self.conn:
                return

            self.cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS docs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    title TEXT,
                    created_at TEXT,
                    changed_at TEXT,
                    links TEXT,
                    video_links TEXT,
                    tags TEXT,
                    is_compliant TEXT,
                    noncompliance_reason TEXT
                )
                """
            )

            self.cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS changes (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    file_name TEXT,
                    sync_time TEXT,
                    has_links_changed TEXT,
                    has_video_links_changed TEXT,
                    has_tags_changed TEXT,
                    has_compliance_changed TEXT
                )
                """
            )

            self.cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS settings (
                    key TEXT PRIMARY KEY,
                    value TEXT
                )
                """
            )

            self.conn.commit()
        except Exception:
            logger.error("sqlite_handler/init_db failed\n%s", traceback.format_exc())
            adieu(1)

    def _execute(self, query: str, params: tuple = ()) -> sqlite3.Cursor:
        self.cursor.execute(query, params)
        return self.cursor

    def _fetch_all_dict(self, query: str, params: tuple = ()) -> list[dict]:
        rows = self._execute(query, params).fetchall()
        return [dict(row) for row in rows]

    def _fetch_one_dict(self, query: str, params: tuple = ()) -> dict | None:
        row = self._execute(query, params).fetchone()
        return dict(row) if row else None

    def _commit(self) -> None:
        self.conn.commit()

    def create_new_docs_entry(self, ndd: dict) -> None:
        try:
            self._execute(
                "INSERT INTO docs (title, created_at, changed_at, links, tags, is_compliant, video_links, noncompliance_reason) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    ndd.get("title", "N/A"),
                    ndd.get("created_at", "N/A"),
                    ndd.get("changed_at", "N/A"),
                    ndd.get("links", "N/A"),
                    ndd.get("tags", "N/A"),
                    ndd.get("is_compliant", "false"),
                    ndd.get("video_links", "N/A"),
                    ndd.get("noncompliance_reason", "N/A"),
                ),
            )
            self._commit()
            logger.info("Created docs entry for title=%s", ndd.get("title", "N/A"))
        except Exception:
            logger.error("sqlite_handler/create_new_docs_entry failed\n%s", traceback.format_exc())
            adieu(1)

    def get_docs_by_id(self, id: int) -> dict:
        try:
            row_dict = self._fetch_one_dict("SELECT * FROM docs WHERE id = ?", (id,))
            if not row_dict:
                return {}
            return {row_dict.get("id"): row_dict}
        except Exception:
            logger.error("sqlite_handler/get_docs_by_id failed\n%s", traceback.format_exc())
            adieu(1)

    def get_docs_by_name(self, file_name: str) -> dict:
        try:
            rows = self._fetch_all_dict("SELECT * FROM docs WHERE title = ?", (file_name,))
            result = {}

            for row in rows:
                result[row.get("id")] = row

            return result
        except Exception:
            logger.error("sqlite_handler/get_docs_by_name failed\n%s", traceback.format_exc())
            adieu(1)

    def get_docs_by_tag(self, tag_name: str) -> dict:
        try:
            rows = self._fetch_all_dict("SELECT * FROM docs")
            result = {}

            for row_dict in rows:
                tags_raw = row_dict.get("tags", "N/A")
                tags = []

                if isinstance(tags_raw, str):
                    stripped = tags_raw.strip()
                    if stripped.startswith("[") and stripped.endswith("]"):
                        try:
                            parsed = json.loads(stripped)
                            if isinstance(parsed, list):
                                tags = [str(item) for item in parsed]
                        except json.JSONDecodeError:
                            tags = [tags_raw]
                    elif stripped not in ("", "N/A"):
                        tags = [tags_raw]
                elif isinstance(tags_raw, list):
                    tags = [str(item) for item in tags_raw]

                if tag_name in tags:
                    result[row_dict.get("id")] = row_dict

            return result
        except Exception:
            logger.error("sqlite_handler/get_docs_by_tag failed\n%s", traceback.format_exc())
            adieu(1)

    def get_all_docs(self) -> dict:
        try:
            rows = self._fetch_all_dict("SELECT * FROM docs")
            result = {}

            for row_dict in rows:
                result[row_dict.get("id")] = row_dict

            return result
        except Exception:
            logger.error("sqlite_handler/get_all_docs failed\n%s", traceback.format_exc())
            adieu(1)

    def update_docs_by_id(self, udd: dict, id: int) -> None:
        try:
            if id == "N/A":
                raise Exception("ID muss einen Wert haben: N/A erhalten")

            self._execute(
                "UPDATE docs SET title = ?, created_at = ?, changed_at = ?, links = ?, tags = ?, is_compliant = ?, video_links = ?, noncompliance_reason = ? WHERE id = ?",
                (
                    udd.get("title", "N/A"),
                    udd.get("created_at", "N/A"),
                    udd.get("changed_at", "N/A"),
                    udd.get("links", "N/A"),
                    udd.get("tags", "N/A"),
                    udd.get("is_compliant", "false"),
                    udd.get("video_links", "N/A"),
                    udd.get("noncompliance_reason", "N/A"),
                    id,
                ),
            )

            self._commit()
            logger.info("Updated docs entry id=%s title=%s", id, udd.get("title", "N/A"))
        except Exception:
            logger.error("sqlite_handler/update_docs_by_id failed\n%s", traceback.format_exc())
            adieu(1)

    def upsert_setting(self, key: str, value: str) -> None:
        try:
            self._execute(
                """
                INSERT INTO settings (key, value)
                VALUES (?, ?)
                ON CONFLICT(key) DO UPDATE SET value=excluded.value
                """,
                (key, value),
            )
            self._commit()
        except Exception:
            logger.error("sqlite_handler/upsert_setting failed\n%s", traceback.format_exc())
            adieu(1)

    def get_setting(self, key: str, default: str = "N/A") -> str:
        try:
            row = self._fetch_one_dict("SELECT value FROM settings WHERE key = ?", (key,))
            if not row:
                return default
            return row.get("value", default)
        except Exception:
            logger.error("sqlite_handler/get_setting failed\n%s", traceback.format_exc())
            adieu(1)

    def update_last_sync_time(self, sync_time: str | None = None) -> str:
        try:
            ts = sync_time or datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            self.upsert_setting("last_sync_time", ts)
            return ts
        except Exception:
            logger.error("sqlite_handler/update_last_sync_time failed\n%s", traceback.format_exc())
            adieu(1)

    def get_last_sync_time(self) -> str:
        try:
            return self.get_setting("last_sync_time", "Never")
        except Exception:
            logger.error("sqlite_handler/get_last_sync_time failed\n%s", traceback.format_exc())
            adieu(1)

    def log_change_if_needed(self, previous_doc: dict | None, current_doc: dict, sync_time: str) -> None:
        try:
            if not current_doc:
                return

            previous = previous_doc or {}
            changed = {
                "has_links_changed": "true" if previous.get("links") != current_doc.get("links") else "false",
                "has_video_links_changed": "true" if previous.get("video_links") != current_doc.get("video_links") else "false",
                "has_tags_changed": "true" if previous.get("tags") != current_doc.get("tags") else "false",
                "has_compliance_changed": "true"
                if (
                    previous.get("is_compliant") != current_doc.get("is_compliant")
                    or previous.get("noncompliance_reason") != current_doc.get("noncompliance_reason")
                )
                else "false",
            }

            if all(value == "false" for value in changed.values()):
                logger.info("No content changes detected for title=%s", current_doc.get("title", "N/A"))
                return

            self._execute(
                """
                INSERT INTO changes
                (file_name, sync_time, has_links_changed, has_video_links_changed, has_tags_changed, has_compliance_changed)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    current_doc.get("title", "N/A"),
                    sync_time,
                    changed["has_links_changed"],
                    changed["has_video_links_changed"],
                    changed["has_tags_changed"],
                    changed["has_compliance_changed"],
                ),
            )
            self._commit()
            logger.info("Logged changes for title=%s at sync_time=%s", current_doc.get("title", "N/A"), sync_time)
        except Exception:
            logger.error("sqlite_handler/log_change_if_needed failed\n%s", traceback.format_exc())
            adieu(1)

    def get_latest_change_versions(self, limit: int = 10) -> list[str]:
        try:
            rows = self._fetch_all_dict(
                """
                SELECT sync_time FROM changes
                GROUP BY sync_time
                ORDER BY sync_time DESC
                LIMIT ?
                """,
                (limit,),
            )
            return [row.get("sync_time") for row in rows if row.get("sync_time")]
        except Exception:
            logger.error("sqlite_handler/get_latest_change_versions failed\n%s", traceback.format_exc())
            adieu(1)

    def get_changes_by_version(self, sync_time: str) -> list[dict]:
        try:
            return self._fetch_all_dict(
                """
                SELECT * FROM changes
                WHERE sync_time = ?
                ORDER BY id ASC
                """,
                (sync_time,),
            )
        except Exception:
            logger.error("sqlite_handler/get_changes_by_version failed\n%s", traceback.format_exc())
            adieu(1)

    def trim_old_change_versions(self, keep: int = 10) -> None:
        try:
            self._execute(
                """
                DELETE FROM changes
                WHERE sync_time NOT IN (
                    SELECT sync_time
                    FROM (
                        SELECT sync_time
                        FROM changes
                        GROUP BY sync_time
                        ORDER BY sync_time DESC
                        LIMIT ?
                    )
                )
                """,
                (keep,),
            )
            self._commit()
            logger.info("Trimmed old change versions; keeping latest=%s", keep)
        except Exception:
            logger.error("sqlite_handler/trim_old_change_versions failed\n%s", traceback.format_exc())
            adieu(1)

    def delete_docs_by_id(self, id: int) -> None:
        try:
            self._execute("DELETE FROM docs WHERE id = ?", (id,))
            self._commit()
            logger.info("Deleted docs entry id=%s", id)
        except Exception:
            logger.error("sqlite_handler/delete_docs_by_id failed\n%s", traceback.format_exc())
            adieu(1)

    def delete_docs_by_name(self, file_name: str) -> None:
        try:
            self._execute("DELETE FROM docs WHERE title = ?", (file_name,))
            self._commit()
            logger.info("Deleted docs entries title=%s", file_name)
        except Exception:
            logger.error("sqlite_handler/delete_docs_by_name failed\n%s", traceback.format_exc())
            adieu(1)

    def delete_all_docs(self) -> None:
        try:
            self._execute("DELETE FROM docs")
            self._commit()
            logger.warning("Deleted all docs from database")
        except Exception:
            logger.error("sqlite_handler/delete_all_docs failed\n%s", traceback.format_exc())
            adieu(1)

    def delete_all_changes(self) -> None:
        try:
            self._execute("DELETE FROM changes")
            self._commit()
            logger.warning("Deleted all changes from database")
        except Exception:
            logger.error("sqlite_handler/delete_all_changes failed\n%s", traceback.format_exc())
            adieu(1)

    def get_non_compliant_docs(self) -> dict:
        try:
            rows = self._fetch_all_dict("SELECT * FROM docs WHERE is_compliant = ?", ("false",))
            result = {}

            for row_dict in rows:
                result[row_dict.get("id")] = row_dict

            return result
        except Exception:
            logger.error("sqlite_handler/get_non_compliant_docs failed\n%s", traceback.format_exc())
            adieu(1)

    def get_compliant_docs(self) -> dict:
        try:
            rows = self._fetch_all_dict("SELECT * FROM docs WHERE is_compliant = ?", ("true",))
            result = {}

            for row_dict in rows:
                result[row_dict.get("id")] = row_dict

            return result
        except Exception:
            logger.error("sqlite_handler/get_compliant_docs failed\n%s", traceback.format_exc())
            adieu(1)

    def check_if_doc_is_already_in_db(self, file_name: str) -> dict:
        try:
            if file_name == "N/A":
                raise Exception("Filename kann nicht N/A sein!! fehler beim Parsen!")

            r = self._fetch_all_dict("SELECT id FROM docs WHERE title = ? LIMIT 1", (file_name,))
            if len(r) > 0:
                return {"bool": True, "id": r[0].get("id", "N/A")}
            return {"bool": False, "id": "N/A"}

        except Exception:
            logger.error("sqlite_handler/check_if_doc_is_already_in_db failed\n%s", traceback.format_exc())
            adieu(1)

    def __del__(self) -> None:
        try:
            if getattr(self, "conn", None):
                self.conn.close()
        except Exception:
            logger.error("sqlite_handler/close failed\n%s", traceback.format_exc())
