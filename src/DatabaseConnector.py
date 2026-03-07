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

                self.db_path: str = Path(__file__).resolve().parent.parent / j["db"]["db_path"]

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

            self.conn.commit()
        except Exception:
            print("sqlite_handler/init_db: {0}".format(traceback.format_exc()))
            adieu(1)

    ## ---- STANDARD CRUD ---- ##
    def create_new_docs_entry(self, ndd: dict) -> None:
        try:
            ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
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
                (id),
            )
            row = self.cursor.fetchall()
            result = {}

            row_dict = dict(row)
            result[row_dict.get("id")] = row_dict

            return result
        except Exception:
            print("sqlite_handler/get_docs_by_id: {0}".format(traceback.format_exc()))
            adieu(1)

    def update_docs_by_id(self, udd: dict) -> None:
        try:
            ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

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
                    ts
                ),
            )
            
            self.cursor.execute()
        except Exception:
            print("sqlite_handler/update_docs_by_id: {0}".format(traceback.format_exc()))
            adieu(1)

    def delete_docs_by_id(self, id: int) -> None:
        try:
            self.cursor.execute(
                "DELETE FROM docs WHERE id = ?",
                (id),
            )
            
            self.cursor.execute()
        except Exception:
            print("sqlite_handler/delete_docs_by_id: {0}".format(traceback.format_exc()))
            adieu(1)

    ## ---- Usecases ---- ##
    def get_non_compliant_docs(self) -> dict:
        try:
            self.cursor.execute(
                "SELECT * FROM docs WHERE is_compliant = ?",
                ("false"),
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
    
    def __del__(self) -> None:
        try:
            if getattr(self, "conn", None):
                self.conn.close()
        except Exception:
            print("sqlite_handler/close: {0}".format(traceback.format_exc()))

