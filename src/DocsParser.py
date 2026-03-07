import re
import json
import traceback
from pathlib import Path
from sys import exit as adieu

from DatabaseConnector import db

class DocsParser:
    def __init__(self) -> None:
        try:
            # -- Get config-parameters --
            path = Path(__file__).resolve().parent.parent / "conf.json"

            with open(f"{path}", "r") as f:
                j: dict = json.loads(f.read())

                self.docs_path: str = j.get("full_path_to_docs", False)

            if self.docs_path == False:
                raise Exception(f"Docs-Pfad wurde nicht gefunden oder ist ungültig!")

        except Exception:
            print(traceback.format_exc())
            adieu(1)

    def __get_full_document_list(self) -> list[str]:
        try:
            p = Path(self.docs_path)

            if not p.exists():
                raise Exception(f"Docs-Pfad '{self.docs_path}' existiert nicht")

            files: list[str] = [str(fp.resolve()) for fp in p.rglob("*") if fp.is_file()]

            return files

        except Exception:
            print(traceback.format_exc())
            adieu(1)

    def __parse_title_from_doc(file_name: str) -> str:
        try:
            pass

        except Exception:
            print(traceback.format_exc())
            adieu(1)
    
    def __parse_title_from_doc(doc_content: str) -> str:
        try:
            pass

        except Exception:
            print(traceback.format_exc())
            adieu(1)

    def __parse_created_at_from_doc(doc_content: str) -> str:
        try:
            pass

        except Exception:
            print(traceback.format_exc())
            adieu(1)

    def __parse_changed_at_from_doc(doc_content: str) -> str:
        try:
            pass

        except Exception:
            print(traceback.format_exc())
            adieu(1)

    def __parse_links_from_doc(doc_content: str) -> str:
        try:
            pass

        except Exception:
            print(traceback.format_exc())
            adieu(1)
    
    def __parse_video_links_from_doc(doc_content: str) -> str:
        try:
            pass

        except Exception:
            print(traceback.format_exc())
            adieu(1)

    def __parse_tags_from_doc(doc_content: str) -> str:
        try:
            pass

        except Exception:
            print(traceback.format_exc())
            adieu(1)
    
    def __enumerate_is_compliant(doc_content: str) -> str:
        try:
            pass

        except Exception:
            print(traceback.format_exc())
            adieu(1)

    def parse_and_add_ALL_docs_to_db(self) -> None:
        try:
            db_object = db()

            for doc_full_path in self.__get_full_document_list():
                with open(doc_full_path, "r") as f:
                    file_contents = f.read()
                    
                    append_dict = {
                        "title": self.__parse_title_from_doc("test")
                    }
                    
                    db_object.create_new_docs_entry(append_dict)

        except Exception:
            print(traceback.format_exc())
            adieu(1)