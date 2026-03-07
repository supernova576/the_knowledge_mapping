import sqlite3
import json
import traceback
from pathlib import Path
from datetime import datetime
from sys import exit as adieu

class db:
    def __init__(self) -> None:
        try:
            # -- Get config-parameters --
            path = Path(__file__).resolve().parent.parent / "conf.json"

            with open(f"{path}", "r") as f:
                j = json.loads(f.read())

                self.db_path: Path = Path(__file__).resolve().parent.parent / j["db"]["db_path"]

            self.db_path.parent.mkdir(parents=True, exist_ok=True)

            # connect and initialize DB
            self.conn = sqlite3.connect(str(self.db_path), check_same_thread=False)
            self.conn.row_factory = sqlite3.Row
            self.cursor = self.conn.cursor()

            self.__init_db()
        except Exception:
            print(traceback.format_exc())
            adieu(1)

    def __init_db(self) -> None:
        try:
            if not self.conn:
                # nothing to do when not connected
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
                    last_sync TEXT
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

            self.conn.commit()
        except Exception:
            print("sqlite_handler/init_db: {0}".format(traceback.format_exc()))
            adieu(1)

    ## ---- STANDARD CRUD ---- ##
    def create_new_docs_entry(self, ndd: dict, sync_time: str | None = None) -> None:
        try:
            ts = sync_time or datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            self.cursor.execute(
                "INSERT INTO docs (title, created_at, changed_at, links, tags, is_compliant, video_links, last_sync) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    ndd.get("title", "N/A"), 
                    ndd.get("created_at", "N/A"), 
                    ndd.get("changed_at", "N/A"), 
                    ndd.get("links", "N/A"), 
                    ndd.get("tags", "N/A"), 
                    ndd.get("is_compliant", "false"),
                    ndd.get("video_links", "N/A"),
                    ts
                ),
            )
            self.conn.commit()
        except Exception:
            print("sqlite_handler/create_new_docs_entry: {0}".format(traceback.format_exc()))
            adieu(1)

    def get_docs_by_id(self, id: int) -> dict:
        try:
            self.cursor.execute(
                "SELECT * FROM docs WHERE id = ?",
                (id,),
            )
            row = self.cursor.fetchall()
            if len(row) == 0:
                return {}

            result = {}

            row_dict = dict(row[0])
            result[row_dict.get("id")] = row_dict

            return result
        except Exception:
            print("sqlite_handler/get_docs_by_id: {0}".format(traceback.format_exc()))
            adieu(1)

    def get_docs_by_name(self, file_name: str) -> dict:
        try:
            self.cursor.execute(
                "SELECT * FROM docs WHERE title = ?",
                (file_name,),
            )
            rows = self.cursor.fetchall()
            result = {}

            for row in rows:
                row_dict = dict(row)
                result[row_dict.get("id")] = row_dict

            return result
        except Exception:
            print("sqlite_handler/get_docs_by_name: {0}".format(traceback.format_exc()))
            adieu(1)

    def get_docs_by_tag(self, tag_name: str) -> dict:
        try:
            self.cursor.execute("SELECT * FROM docs")
            rows = self.cursor.fetchall()
            result = {}

            for row in rows:
                row_dict = dict(row)
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
            print("sqlite_handler/get_docs_by_tag: {0}".format(traceback.format_exc()))
            adieu(1)

    def get_all_docs(self) -> dict:
        try:
            self.cursor.execute("SELECT * FROM docs")
            rows = self.cursor.fetchall()
            result = {}

            for row in rows:
                row_dict = dict(row)
                result[row_dict.get("id")] = row_dict

            return result
        except Exception:
            print("sqlite_handler/get_all_docs: {0}".format(traceback.format_exc()))
            adieu(1)

    def update_docs_by_id(self, udd: dict, id: int, sync_time: str | None = None) -> None:
        try:
            if id == "N/A":
                raise Exception("ID muss einen Wert haben: N/A erhalten")

            ts = sync_time or datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            self.cursor.execute(
                "UPDATE docs SET title = ?, created_at = ?, changed_at = ?, links = ?, tags = ?, is_compliant = ?, video_links = ?, last_sync = ? WHERE id = ?",
                (
                    udd.get("title", "N/A"), 
                    udd.get("created_at", "N/A"), 
                    udd.get("changed_at", "N/A"), 
                    udd.get("links", "N/A"), 
                    udd.get("tags", "N/A"), 
                    udd.get("is_compliant", "false"),
                    udd.get("video_links", "N/A"),
                    ts,
                    id
                ),
            )

            self.conn.commit()
        except Exception:
            print("sqlite_handler/update_docs_by_id: {0}".format(traceback.format_exc()))
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
                "has_compliance_changed": "true" if previous.get("is_compliant") != current_doc.get("is_compliant") else "false",
            }

            if all(value == "false" for value in changed.values()):
                return

            self.cursor.execute(
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
            self.conn.commit()
        except Exception:
            print("sqlite_handler/log_change_if_needed: {0}".format(traceback.format_exc()))
            adieu(1)

    def get_latest_change_versions(self, limit: int = 10) -> list[str]:
        try:
            self.cursor.execute(
                """
                SELECT sync_time FROM changes
                GROUP BY sync_time
                ORDER BY sync_time DESC
                LIMIT ?
                """,
                (limit,),
            )
            rows = self.cursor.fetchall()
            return [dict(row).get("sync_time") for row in rows if dict(row).get("sync_time")]
        except Exception:
            print("sqlite_handler/get_latest_change_versions: {0}".format(traceback.format_exc()))
            adieu(1)

    def get_changes_by_version(self, sync_time: str) -> list[dict]:
        try:
            self.cursor.execute(
                """
                SELECT * FROM changes
                WHERE sync_time = ?
                ORDER BY id ASC
                """,
                (sync_time,),
            )
            rows = self.cursor.fetchall()
            return [dict(row) for row in rows]
        except Exception:
            print("sqlite_handler/get_changes_by_version: {0}".format(traceback.format_exc()))
            adieu(1)

    def trim_old_change_versions(self, keep: int = 10) -> None:
        try:
            self.cursor.execute(
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
            self.conn.commit()
        except Exception:
            print("sqlite_handler/trim_old_change_versions: {0}".format(traceback.format_exc()))
            adieu(1)

    def delete_docs_by_id(self, id: int) -> None:
        try:
            self.cursor.execute(
                "DELETE FROM docs WHERE id = ?",
                (id,),
            )
            self.conn.commit()
        except Exception:
            print("sqlite_handler/delete_docs_by_id: {0}".format(traceback.format_exc()))
            adieu(1)

    def delete_docs_by_name(self, file_name: str) -> None:
        try:
            self.cursor.execute(
                "DELETE FROM docs WHERE title = ?",
                (file_name,),
            )
            self.conn.commit()
        except Exception:
            print("sqlite_handler/delete_docs_by_name: {0}".format(traceback.format_exc()))
            adieu(1)

    def delete_all_docs(self) -> None:
        try:
            self.cursor.execute("DELETE FROM docs")
            self.conn.commit()
        except Exception:
            print("sqlite_handler/delete_all_docs: {0}".format(traceback.format_exc()))
            adieu(1)

    ## ---- Usecases ---- ##
    def get_non_compliant_docs(self) -> dict:
        try:
            self.cursor.execute(
                "SELECT * FROM docs WHERE is_compliant = ?",
                ("false",),
            )
            rows = self.cursor.fetchall()
            result = {}

            for row in rows:
                row_dict = dict(row)
                result[row_dict.get("id")] = row_dict

            return result
        except Exception:
            print("sqlite_handler/get_non_compliant_docs: {0}".format(traceback.format_exc()))
            adieu(1)

    def get_compliant_docs(self) -> dict:
        try:
            self.cursor.execute(
                "SELECT * FROM docs WHERE is_compliant = ?",
                ("true",),
            )
            rows = self.cursor.fetchall()
            result = {}

            for row in rows:
                row_dict = dict(row)
                result[row_dict.get("id")] = row_dict

            return result
        except Exception:
            print("sqlite_handler/get_compliant_docs: {0}".format(traceback.format_exc()))
            adieu(1)
    
    def check_if_doc_is_already_in_db(self, file_name: str) -> dict:
        try:
            if file_name == "N/A":
                raise Exception("Filename kann nicht N/A sein!! fehler beim Parsen!")

            self.cursor.execute(
                "SELECT id FROM docs WHERE title = ? LIMIT 1",
                (file_name,),
            )

            r = self.cursor.fetchall()
            if len(r) > 0:
                return {"bool": True, "id": dict(r[0]).get("id", "N/A")}
            else:
                return {"bool": False, "id": "N/A"}

        except Exception:
            print("sqlite_handler/check_if_doc_is_already_in_db: {0}".format(traceback.format_exc()))
            adieu(1)

    def __del__(self) -> None:
        try:
            if getattr(self, "conn", None):
                self.conn.close()
        except Exception:
            print("sqlite_handler/close: {0}".format(traceback.format_exc()))
