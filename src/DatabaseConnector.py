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
            self.cursor.execute("PRAGMA foreign_keys = ON")

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
                CREATE TABLE IF NOT EXISTS settings (
                    key TEXT PRIMARY KEY,
                    value TEXT
                )
                """
            )

            self.cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS ai_feedback (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    file_name TEXT NOT NULL,
                    version INTEGER NOT NULL,
                    score REAL NOT NULL,
                    path_to_feedback TEXT NOT NULL UNIQUE,
                    creation_date TEXT NOT NULL
                )
                """
            )

            self.cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS learnings (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    file_name TEXT NOT NULL,
                    source_note_name TEXT NOT NULL,
                    path_to_learning TEXT NOT NULL UNIQUE,
                    creation_date TEXT NOT NULL,
                    last_modified_date TEXT NOT NULL DEFAULT 'N/A'
                )
                """
            )
            learning_columns = {
                str(row["name"]).strip().casefold()
                for row in self._execute("PRAGMA table_info(learnings)").fetchall()
            }
            if "last_modified_date" not in learning_columns:
                self._execute(
                    "ALTER TABLE learnings ADD COLUMN last_modified_date TEXT NOT NULL DEFAULT 'N/A'"
                )

            self.cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS learning_exam_drafts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    learning_id INTEGER NOT NULL UNIQUE,
                    answers_json TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    FOREIGN KEY (learning_id) REFERENCES learnings(id) ON DELETE CASCADE
                )
                """
            )

            self.cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS learning_exam_attempts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    learning_id INTEGER NOT NULL,
                    answers_json TEXT NOT NULL,
                    score REAL NOT NULL,
                    total_questions INTEGER NOT NULL,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY (learning_id) REFERENCES learnings(id) ON DELETE CASCADE
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

    def get_docs_by_name(self, file_name: str, exact_match: bool = True) -> dict:
        try:
            if exact_match:
                rows = self._fetch_all_dict("SELECT * FROM docs WHERE title = ?", (file_name,))
            else:
                rows = self._fetch_all_dict("SELECT * FROM docs WHERE lower(title) LIKE lower(?)", (f"%{file_name}%",))
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

    def replace_all_ai_feedback(self, rows: list[dict]) -> None:
        try:
            self._execute("DELETE FROM ai_feedback")
            for row in rows:
                self._execute(
                    """
                    INSERT INTO ai_feedback (file_name, version, score, path_to_feedback, creation_date)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (
                        str(row.get("file_name", "N/A")).strip(),
                        int(row.get("version", 1)),
                        float(row.get("score", 0)),
                        str(row.get("path_to_feedback", "")).strip(),
                        str(row.get("creation_date", "N/A")).strip(),
                    ),
                )
            self._commit()
            logger.info("Replaced ai_feedback table with %s entries", len(rows))
        except Exception:
            logger.error("sqlite_handler/replace_all_ai_feedback failed\n%s", traceback.format_exc())
            adieu(1)

    def get_all_ai_feedback(self) -> list[dict]:
        try:
            return self._fetch_all_dict(
                """
                SELECT * FROM ai_feedback
                ORDER BY lower(file_name) ASC, version DESC, id DESC
                """
            )
        except Exception:
            logger.error("sqlite_handler/get_all_ai_feedback failed\n%s", traceback.format_exc())
            adieu(1)

    def get_ai_feedback_by_id(self, feedback_id: int) -> dict | None:
        try:
            return self._fetch_one_dict("SELECT * FROM ai_feedback WHERE id = ?", (feedback_id,))
        except Exception:
            logger.error("sqlite_handler/get_ai_feedback_by_id failed\n%s", traceback.format_exc())
            adieu(1)

    def delete_ai_feedback_by_id(self, feedback_id: int) -> None:
        try:
            self._execute("DELETE FROM ai_feedback WHERE id = ?", (feedback_id,))
            self._commit()
        except Exception:
            logger.error("sqlite_handler/delete_ai_feedback_by_id failed\n%s", traceback.format_exc())
            adieu(1)

    def get_latest_ai_feedback_for_file(self, file_name: str) -> dict | None:
        try:
            return self._fetch_one_dict(
                """
                SELECT * FROM ai_feedback
                WHERE lower(file_name) = lower(?)
                ORDER BY version DESC, id DESC
                LIMIT 1
                """,
                (file_name,),
            )
        except Exception:
            logger.error("sqlite_handler/get_latest_ai_feedback_for_file failed\n%s", traceback.format_exc())
            adieu(1)

    def upsert_learning(self, row: dict) -> None:
        try:
            self._execute(
                """
                INSERT INTO learnings (file_name, source_note_name, path_to_learning, creation_date, last_modified_date)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(path_to_learning) DO UPDATE SET
                    file_name = excluded.file_name,
                    source_note_name = excluded.source_note_name,
                    creation_date = excluded.creation_date,
                    last_modified_date = excluded.last_modified_date
                """,
                (
                    str(row.get("file_name", "")).strip(),
                    str(row.get("source_note_name", "")).strip(),
                    str(row.get("path_to_learning", "")).strip(),
                    str(row.get("creation_date", "N/A")).strip(),
                    str(row.get("last_modified_date", "N/A")).strip(),
                ),
            )
            self._commit()
        except Exception:
            logger.error("sqlite_handler/upsert_learning failed\n%s", traceback.format_exc())
            adieu(1)

    def get_all_learnings(self) -> list[dict]:
        try:
            return self._fetch_all_dict("SELECT * FROM learnings ORDER BY lower(file_name) ASC, id DESC")
        except Exception:
            logger.error("sqlite_handler/get_all_learnings failed\n%s", traceback.format_exc())
            adieu(1)

    def get_learning_docs_by_tags(self, tags: list[str]) -> list[dict]:
        try:
            normalized_tags = sorted({str(tag).strip() for tag in tags if str(tag).strip()}, key=lambda value: value.casefold())
            if not normalized_tags:
                return []

            clauses: list[str] = []
            params: list[str] = []
            for tag in normalized_tags:
                clauses.append(
                    """
                    EXISTS (
                        SELECT 1
                        FROM json_each(
                            CASE
                                WHEN docs.tags IS NULL OR trim(docs.tags) = '' OR trim(docs.tags) = 'N/A' THEN '[]'
                                ELSE docs.tags
                            END
                        )
                        WHERE lower(trim(json_each.value)) = lower(trim(?))
                    )
                    """
                )
                params.append(tag)

            where_clause = " OR ".join(clauses)
            query = f"""
                SELECT
                    docs.id AS doc_id,
                    docs.title AS doc_title,
                    docs.tags AS doc_tags,
                    learnings.id AS learning_id,
                    learnings.file_name AS learning_file_name,
                    learnings.source_note_name AS learning_source_note_name,
                    learnings.path_to_learning AS learning_path_to_learning,
                    learnings.creation_date AS learning_creation_date
                FROM docs
                LEFT JOIN learnings
                    ON lower(trim(replace(learnings.source_note_name, '.md', ''))) = lower(trim(replace(docs.title, '.md', '')))
                    OR lower(trim(replace(learnings.file_name, ' - Learning', ''))) = lower(trim(replace(docs.title, '.md', '')))
                WHERE {where_clause}
                ORDER BY lower(docs.title) ASC, learnings.id DESC
            """
            return self._fetch_all_dict(query, tuple(params))
        except Exception:
            logger.error("sqlite_handler/get_learning_docs_by_tags failed\n%s", traceback.format_exc())
            adieu(1)

    def get_learning_by_id(self, learning_id: int) -> dict | None:
        try:
            return self._fetch_one_dict("SELECT * FROM learnings WHERE id = ?", (learning_id,))
        except Exception:
            logger.error("sqlite_handler/get_learning_by_id failed\n%s", traceback.format_exc())
            adieu(1)

    def delete_learning_by_id(self, learning_id: int) -> None:
        try:
            self._execute("DELETE FROM learnings WHERE id = ?", (learning_id,))
            self._commit()
        except Exception:
            logger.error("sqlite_handler/delete_learning_by_id failed\n%s", traceback.format_exc())
            adieu(1)

    def delete_learnings_not_in_paths(self, kept_paths: list[str]) -> None:
        try:
            normalized_paths = [str(path).strip() for path in kept_paths if str(path).strip()]
            if not normalized_paths:
                self._execute("DELETE FROM learnings")
                self._commit()
                return

            placeholders = ",".join("?" for _ in normalized_paths)
            self._execute(
                f"DELETE FROM learnings WHERE path_to_learning NOT IN ({placeholders})",
                tuple(normalized_paths),
            )
            self._commit()
        except Exception:
            logger.error("sqlite_handler/delete_learnings_not_in_paths failed\n%s", traceback.format_exc())
            adieu(1)

    def upsert_learning_exam_draft(self, learning_id: int, answers_json: str, updated_at: str) -> None:
        try:
            self._execute(
                """
                INSERT INTO learning_exam_drafts (learning_id, answers_json, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(learning_id) DO UPDATE SET
                    answers_json = excluded.answers_json,
                    updated_at = excluded.updated_at
                """,
                (learning_id, answers_json, updated_at),
            )
            self._commit()
        except Exception:
            logger.error("sqlite_handler/upsert_learning_exam_draft failed\n%s", traceback.format_exc())
            adieu(1)

    def get_learning_exam_draft(self, learning_id: int) -> dict | None:
        try:
            return self._fetch_one_dict("SELECT * FROM learning_exam_drafts WHERE learning_id = ?", (learning_id,))
        except Exception:
            logger.error("sqlite_handler/get_learning_exam_draft failed\n%s", traceback.format_exc())
            adieu(1)

    def delete_learning_exam_draft(self, learning_id: int) -> None:
        try:
            self._execute("DELETE FROM learning_exam_drafts WHERE learning_id = ?", (learning_id,))
            self._commit()
        except Exception:
            logger.error("sqlite_handler/delete_learning_exam_draft failed\n%s", traceback.format_exc())
            adieu(1)

    def create_learning_exam_attempt(
        self,
        learning_id: int,
        answers_json: str,
        score: float,
        total_questions: int,
        created_at: str,
    ) -> int:
        try:
            cursor = self._execute(
                """
                INSERT INTO learning_exam_attempts (learning_id, answers_json, score, total_questions, created_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (learning_id, answers_json, score, total_questions, created_at),
            )
            self._commit()
            return int(cursor.lastrowid)
        except Exception:
            logger.error("sqlite_handler/create_learning_exam_attempt failed\n%s", traceback.format_exc())
            adieu(1)

    def get_learning_exam_attempts(self, learning_id: int) -> list[dict]:
        try:
            return self._fetch_all_dict(
                """
                SELECT * FROM learning_exam_attempts
                WHERE learning_id = ?
                ORDER BY id DESC
                """,
                (learning_id,),
            )
        except Exception:
            logger.error("sqlite_handler/get_learning_exam_attempts failed\n%s", traceback.format_exc())
            adieu(1)

    def get_learning_exam_attempt_by_id(self, attempt_id: int) -> dict | None:
        try:
            return self._fetch_one_dict("SELECT * FROM learning_exam_attempts WHERE id = ?", (attempt_id,))
        except Exception:
            logger.error("sqlite_handler/get_learning_exam_attempt_by_id failed\n%s", traceback.format_exc())
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
            self.upsert_setting("hslu_semester_overview", str(semester or "").strip())
        except Exception:
            logger.error("sqlite_handler/set_hslu_standard_semester failed\n%s", traceback.format_exc())
            adieu(1)

    def get_hslu_standard_semester(self) -> str:
        try:
            return str(self.get_setting("hslu_semester_overview", "") or "").strip()
        except Exception:
            logger.error("sqlite_handler/get_hslu_standard_semester failed\n%s", traceback.format_exc())
            adieu(1)

    def __del__(self) -> None:
        try:
            if getattr(self, "conn", None):
                self.conn.close()
        except Exception:
            logger.error("sqlite_handler/close failed\n%s", traceback.format_exc())
