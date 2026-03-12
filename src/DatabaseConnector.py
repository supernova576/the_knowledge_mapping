import json
import sqlite3
import traceback
from pathlib import Path
from sys import exit as adieu

from .logger import get_logger
from .timezone_utils import now_in_zurich_str


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
                    noncompliance_reason TEXT,
                    manual_compliant_override TEXT,
                    is_under_construction TEXT
                )
                """
            )

            self.cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS tags (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT UNIQUE
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
                    value TEXT,
                    hslu_semester_overview_standard_semester TEXT
                )
                """
            )

            settings_columns = [row["name"] for row in self._execute("PRAGMA table_info(settings)").fetchall()]
            if "hslu_semester_overview_standard_semester" not in settings_columns:
                self._execute("ALTER TABLE settings ADD COLUMN hslu_semester_overview_standard_semester TEXT")

            self.cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS todos (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    note TEXT UNIQUE,
                    type TEXT,
                    progress TEXT,
                    last_update TEXT
                )
                """
            )

            self.cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS hslu_sw_overview (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    semester TEXT,
                    module TEXT,
                    KW TEXT,
                    SW TEXT,
                    thema TEXT,
                    downloaded TEXT,
                    documented TEXT,
                    deadlines TEXT
                )
                """
            )

            self.cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS hslu_sw_checklist (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    semester TEXT,
                    section TEXT,
                    sw TEXT,
                    checklist_row TEXT,
                    checklist_item TEXT,
                    status TEXT,
                    file_path TEXT
                )
                """
            )

            self.cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS version_control_snapshots (
                    id INTEGER PRIMARY KEY CHECK (id = 1),
                    has_changes TEXT NOT NULL,
                    changes_json TEXT NOT NULL,
                    untracked_files_json TEXT NOT NULL DEFAULT '[]',
                    remote_status_json TEXT NOT NULL DEFAULT '{}',
                    synced_at TEXT NOT NULL
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
                "INSERT INTO docs (title, created_at, changed_at, links, tags, is_compliant, video_links, noncompliance_reason, manual_compliant_override, is_under_construction) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    ndd.get("title", "N/A"),
                    ndd.get("created_at", "N/A"),
                    ndd.get("changed_at", "N/A"),
                    ndd.get("links", "N/A"),
                    ndd.get("tags", "N/A"),
                    ndd.get("is_compliant", "false"),
                    ndd.get("video_links", "N/A"),
                    ndd.get("noncompliance_reason", "N/A"),
                    ndd.get("manual_compliant_override", ""),
                    ndd.get("is_under_construction", "false"),
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
                "UPDATE docs SET title = ?, created_at = ?, changed_at = ?, links = ?, tags = ?, is_compliant = ?, video_links = ?, noncompliance_reason = ?, manual_compliant_override = ?, is_under_construction = ? WHERE id = ?",
                (
                    udd.get("title", "N/A"),
                    udd.get("created_at", "N/A"),
                    udd.get("changed_at", "N/A"),
                    udd.get("links", "N/A"),
                    udd.get("tags", "N/A"),
                    udd.get("is_compliant", "false"),
                    udd.get("video_links", "N/A"),
                    udd.get("noncompliance_reason", "N/A"),
                    udd.get("manual_compliant_override", ""),
                    udd.get("is_under_construction", "false"),
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
            ts = sync_time or now_in_zurich_str()
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

    def save_version_control_snapshot(self, snapshot: dict, synced_at: str | None = None) -> str:
        try:
            timestamp = synced_at or now_in_zurich_str()
            changes = snapshot.get("changes") if isinstance(snapshot, dict) else []
            if not isinstance(changes, list):
                changes = []

            has_changes = "true" if bool(snapshot.get("has_changes")) else "false"
            changes_json = json.dumps(changes, ensure_ascii=False)
            untracked_files = snapshot.get("untracked_files") if isinstance(snapshot, dict) else []
            if not isinstance(untracked_files, list):
                untracked_files = []
            untracked_files_json = json.dumps(untracked_files, ensure_ascii=False)
            remote_status = snapshot.get("remote_status") if isinstance(snapshot, dict) else {}
            if not isinstance(remote_status, dict):
                remote_status = {}
            remote_status_json = json.dumps(remote_status, ensure_ascii=False)

            self._execute(
                """
                INSERT INTO version_control_snapshots (id, has_changes, changes_json, untracked_files_json, remote_status_json, synced_at)
                VALUES (1, ?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    has_changes=excluded.has_changes,
                    changes_json=excluded.changes_json,
                    untracked_files_json=excluded.untracked_files_json,
                    remote_status_json=excluded.remote_status_json,
                    synced_at=excluded.synced_at
                """,
                (has_changes, changes_json, untracked_files_json, remote_status_json, timestamp),
            )
            self._commit()
            return timestamp
        except Exception:
            logger.error("sqlite_handler/save_version_control_snapshot failed\n%s", traceback.format_exc())
            adieu(1)

    def get_version_control_snapshot(self) -> dict:
        try:
            row = self._fetch_one_dict(
                "SELECT has_changes, changes_json, untracked_files_json, remote_status_json, synced_at FROM version_control_snapshots WHERE id = 1"
            )
            if not row:
                return {
                    "has_changes": False,
                    "changes": [],
                    "untracked_files": [],
                    "remote_status": {},
                    "synced_at": "Never",
                }

            raw_changes = row.get("changes_json", "[]")
            try:
                parsed_changes = json.loads(raw_changes)
            except json.JSONDecodeError:
                parsed_changes = []

            if not isinstance(parsed_changes, list):
                parsed_changes = []

            raw_untracked_files = row.get("untracked_files_json", "[]")
            try:
                parsed_untracked_files = json.loads(raw_untracked_files)
            except json.JSONDecodeError:
                parsed_untracked_files = []

            if not isinstance(parsed_untracked_files, list):
                parsed_untracked_files = []

            raw_remote_status = row.get("remote_status_json", "{}")
            try:
                parsed_remote_status = json.loads(raw_remote_status)
            except json.JSONDecodeError:
                parsed_remote_status = {}

            if not isinstance(parsed_remote_status, dict):
                parsed_remote_status = {}

            return {
                "has_changes": str(row.get("has_changes", "false")).lower() == "true",
                "changes": parsed_changes,
                "untracked_files": parsed_untracked_files,
                "remote_status": parsed_remote_status,
                "synced_at": row.get("synced_at", "Never") or "Never",
            }
        except Exception:
            logger.error("sqlite_handler/get_version_control_snapshot failed\n%s", traceback.format_exc())
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

    def update_manual_compliance_by_id(self, id: int, manual_override: str) -> None:
        try:
            if manual_override == "true":
                self._execute(
                    "UPDATE docs SET manual_compliant_override = ?, is_compliant = ?, noncompliance_reason = ? WHERE id = ?",
                    ("true", "true", "N/A", id),
                )
            else:
                self._execute(
                    "UPDATE docs SET manual_compliant_override = ? WHERE id = ?",
                    ("false", id),
                )
            self._commit()
            logger.info("Updated manual compliance override for id=%s to=%s", id, manual_override)
        except Exception:
            logger.error("sqlite_handler/update_manual_compliance_by_id failed\n%s", traceback.format_exc())
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


    def get_under_construction_docs(self) -> dict:
        try:
            rows = self._fetch_all_dict("SELECT * FROM docs WHERE is_under_construction = ?", ("true",))
            result = {}

            for row_dict in rows:
                result[row_dict.get("id")] = row_dict

            return result
        except Exception:
            logger.error("sqlite_handler/get_under_construction_docs failed\n%s", traceback.format_exc())
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

    def replace_all_tags(self, tags: list[str]) -> None:
        try:
            normalized = sorted({str(tag).strip() for tag in tags if str(tag).strip()}, key=lambda value: value.casefold())
            self._execute("DELETE FROM tags")
            for tag in normalized:
                self._execute("INSERT INTO tags (name) VALUES (?)", (tag,))
            self._commit()
            logger.info("Replaced tags table with %s entries", len(normalized))
        except Exception:
            logger.error("sqlite_handler/replace_all_tags failed\n%s", traceback.format_exc())
            adieu(1)

    def get_all_tags(self) -> list[str]:
        try:
            rows = self._fetch_all_dict("SELECT name FROM tags ORDER BY name COLLATE NOCASE ASC")
            return [row.get("name", "") for row in rows if row.get("name")]
        except Exception:
            logger.error("sqlite_handler/get_all_tags failed\n%s", traceback.format_exc())
            adieu(1)

    def replace_all_todos(self, todos: list[dict]) -> None:
        try:
            self._execute("DELETE FROM todos")
            for todo in todos:
                self._execute(
                    "INSERT INTO todos (note, type, progress, last_update) VALUES (?, ?, ?, ?)",
                    (
                        todo.get("note", "N/A"),
                        todo.get("type", "[]"),
                        todo.get("progress", "Not Started"),
                        todo.get("last_update", "N/A"),
                    ),
                )
            self._commit()
            logger.info("Replaced todos table with %s entries", len(todos))
        except Exception:
            logger.error("sqlite_handler/replace_all_todos failed\n%s", traceback.format_exc())
            adieu(1)

    def get_all_todos(self) -> list[dict]:
        try:
            return self._fetch_all_dict("SELECT * FROM todos ORDER BY id ASC")
        except Exception:
            logger.error("sqlite_handler/get_all_todos failed\n%s", traceback.format_exc())
            adieu(1)

    def get_todos_by_note(self, query: str) -> list[dict]:
        try:
            return self._fetch_all_dict(
                "SELECT * FROM todos WHERE lower(note) LIKE lower(?) ORDER BY id ASC",
                (f"%{query}%",),
            )
        except Exception:
            logger.error("sqlite_handler/get_todos_by_note failed\n%s", traceback.format_exc())
            adieu(1)

    def update_todo_progress(self, todo_id: int, progress: str, last_update: str) -> None:
        try:
            self._execute(
                "UPDATE todos SET progress = ?, last_update = ? WHERE id = ?",
                (progress, last_update, todo_id),
            )
            self._commit()
            logger.info("Updated todo progress id=%s to=%s", todo_id, progress)
        except Exception:
            logger.error("sqlite_handler/update_todo_progress failed\n%s", traceback.format_exc())
            adieu(1)

    def delete_todo_by_id(self, todo_id: int) -> None:
        try:
            self._execute("DELETE FROM todos WHERE id = ?", (todo_id,))
            self._commit()
            logger.info("Deleted todo id=%s", todo_id)
        except Exception:
            logger.error("sqlite_handler/delete_todo_by_id failed\n%s", traceback.format_exc())
            adieu(1)

    def replace_all_hslu_sw_overview(self, rows: list[dict]) -> None:
        try:
            self._execute("DELETE FROM hslu_sw_overview")
            for row in rows:
                self._execute(
                    "INSERT INTO hslu_sw_overview (semester, module, KW, SW, thema, downloaded, documented, deadlines) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        row.get("semester", "N/A"),
                        row.get("module", "N/A"),
                        row.get("KW", "N/A"),
                        row.get("SW", "N/A"),
                        row.get("thema", "N/A"),
                        row.get("downloaded", ""),
                        row.get("documented", ""),
                        row.get("deadlines", "-"),
                    ),
                )
            self._commit()
            logger.info("Replaced hslu_sw_overview with %s entries", len(rows))
        except Exception:
            logger.error("sqlite_handler/replace_all_hslu_sw_overview failed\n%s", traceback.format_exc())
            adieu(1)

    def replace_all_hslu_sw_checklist(self, rows: list[dict]) -> None:
        try:
            self._execute("DELETE FROM hslu_sw_checklist")
            for row in rows:
                self._execute(
                    "INSERT INTO hslu_sw_checklist (semester, section, sw, checklist_row, checklist_item, status, file_path) VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (
                        row.get("semester", "N/A"),
                        row.get("section", "N/A"),
                        row.get("sw", ""),
                        row.get("checklist_row", "N/A"),
                        row.get("checklist_item", "N/A"),
                        row.get("status", ""),
                        row.get("file_path", "N/A"),
                    ),
                )
            self._commit()
            logger.info("Replaced hslu_sw_checklist with %s entries", len(rows))
        except Exception:
            logger.error("sqlite_handler/replace_all_hslu_sw_checklist failed\n%s", traceback.format_exc())
            adieu(1)

    def get_hslu_semesters(self) -> list[str]:
        try:
            rows = self._fetch_all_dict(
                "SELECT DISTINCT semester FROM hslu_sw_overview WHERE semester IS NOT NULL AND trim(semester) != '' ORDER BY semester COLLATE NOCASE ASC"
            )
            return [row.get("semester", "") for row in rows if row.get("semester")]
        except Exception:
            logger.error("sqlite_handler/get_hslu_semesters failed\n%s", traceback.format_exc())
            adieu(1)

    def get_hslu_modules_by_semester(self, semester: str) -> list[str]:
        try:
            rows = self._fetch_all_dict(
                "SELECT DISTINCT module FROM hslu_sw_overview WHERE semester = ? AND module IS NOT NULL AND trim(module) != '' ORDER BY module COLLATE NOCASE ASC",
                (semester,),
            )
            return [row.get("module", "") for row in rows if row.get("module")]
        except Exception:
            logger.error("sqlite_handler/get_hslu_modules_by_semester failed\n%s", traceback.format_exc())
            adieu(1)

    def get_hslu_checklist_semesters(self) -> list[str]:
        try:
            rows = self._fetch_all_dict(
                "SELECT DISTINCT semester FROM hslu_sw_checklist WHERE semester IS NOT NULL AND trim(semester) != '' ORDER BY semester COLLATE NOCASE ASC"
            )
            return [row.get("semester", "") for row in rows if row.get("semester")]
        except Exception:
            logger.error("sqlite_handler/get_hslu_checklist_semesters failed\n%s", traceback.format_exc())
            adieu(1)



    def get_hslu_sw_checklist_by_id(self, checklist_id: int) -> dict | None:
        try:
            return self._fetch_one_dict("SELECT * FROM hslu_sw_checklist WHERE id = ?", (checklist_id,))
        except Exception:
            logger.error("sqlite_handler/get_hslu_sw_checklist_by_id failed\n%s", traceback.format_exc())
            adieu(1)

    def get_hslu_sw_checklist_by_semester_and_sw(self, semester: str, sw: str = "") -> list[dict]:
        try:
            if sw:
                return self._fetch_all_dict(
                    """
                    SELECT * FROM hslu_sw_checklist
                    WHERE semester = ? AND (sw = ? OR trim(sw) = '')
                    ORDER BY section COLLATE NOCASE ASC, CAST(NULLIF(sw, '') AS INTEGER) ASC, checklist_row COLLATE NOCASE ASC, checklist_item COLLATE NOCASE ASC
                    """,
                    (semester, sw),
                )
            return self._fetch_all_dict(
                """
                SELECT * FROM hslu_sw_checklist
                WHERE semester = ?
                ORDER BY section COLLATE NOCASE ASC, CAST(NULLIF(sw, '') AS INTEGER) ASC, checklist_row COLLATE NOCASE ASC, checklist_item COLLATE NOCASE ASC
                """,
                (semester,),
            )
        except Exception:
            logger.error("sqlite_handler/get_hslu_sw_checklist_by_semester_and_sw failed\n%s", traceback.format_exc())
            adieu(1)

    def get_hslu_sw_overview_by_semester_and_module(self, semester: str, module: str = "") -> list[dict]:
        try:
            if module:
                return self._fetch_all_dict(
                    "SELECT * FROM hslu_sw_overview WHERE semester = ? AND module = ? ORDER BY CAST(KW AS INTEGER) ASC, CAST(SW AS INTEGER) ASC, module COLLATE NOCASE ASC",
                    (semester, module),
                )
            return self._fetch_all_dict(
                "SELECT * FROM hslu_sw_overview WHERE semester = ? ORDER BY CAST(KW AS INTEGER) ASC, CAST(SW AS INTEGER) ASC, module COLLATE NOCASE ASC",
                (semester,),
            )
        except Exception:
            logger.error("sqlite_handler/get_hslu_sw_overview_by_semester_and_module failed\n%s", traceback.format_exc())
            adieu(1)

    def set_hslu_standard_semester(self, semester: str) -> None:
        try:
            self._execute(
                """
                INSERT INTO settings (key, value, hslu_semester_overview_standard_semester)
                VALUES (?, ?, ?)
                ON CONFLICT(key) DO UPDATE SET hslu_semester_overview_standard_semester=excluded.hslu_semester_overview_standard_semester
                """,
                ("hslu_semester_overview", "", semester),
            )
            self._commit()
        except Exception:
            logger.error("sqlite_handler/set_hslu_standard_semester failed\n%s", traceback.format_exc())
            adieu(1)

    def get_hslu_standard_semester(self) -> str:
        try:
            row = self._fetch_one_dict(
                "SELECT hslu_semester_overview_standard_semester FROM settings WHERE key = ?",
                ("hslu_semester_overview",),
            )
            if not row:
                return ""
            return (row.get("hslu_semester_overview_standard_semester") or "").strip()
        except Exception:
            logger.error("sqlite_handler/get_hslu_standard_semester failed\n%s", traceback.format_exc())
            adieu(1)

    def __del__(self) -> None:
        try:
            if getattr(self, "conn", None):
                self.conn.close()
        except Exception:
            logger.error("sqlite_handler/close failed\n%s", traceback.format_exc())
