import json
import re
import traceback
import datetime
from pathlib import Path
from sys import exit as adieu

from .DatabaseConnector import db
from .logger import get_logger
from .timezone_utils import now_in_zurich_str


logger = get_logger(__name__)


class DocsParser:
    PROGRESS_ICON_TO_STATE = {
        "![[not started.png]]": "Not Started",
        "![[in progress.png]]": "In Progress",
        "![[done.png]]": "Done",
    }

    def __init__(self) -> None:
        try:
            path = Path(__file__).resolve().parent.parent / "conf.json"

            with open(path, "r", encoding="utf-8") as f:
                j: dict = json.loads(f.read())
                self.docs_path: str = j.get("docs", {}).get("full_path_to_docs", False)
                self.todo_file_path: str = j.get("todo", {}).get("full_path_to_todo_file", False)

            if self.docs_path is False:
                raise Exception("Docs-Pfad wurde nicht gefunden oder ist ungültig!")

            logger.info("Docs parser initialized with docs_path=%s", self.docs_path)
        except Exception:
            logger.error("Docs parser initialization failed\n%s", traceback.format_exc())
            adieu(1)

    def __get_full_document_list(self) -> list[str]:
        try:
            p = Path(self.docs_path)

            if not p.exists():
                raise Exception(f"Docs-Pfad '{self.docs_path}' existiert nicht")

            files: list[str] = [str(fp.resolve()) for fp in p.rglob("*") if fp.is_file()]
            logger.info("Found %s document files for scanning", len(files))
            return files

        except Exception:
            logger.error("Failed to enumerate docs\n%s", traceback.format_exc())
            adieu(1)

    def __strip_ignored_sections(self, doc_content: str) -> str:
        try:
            ignored_headings = [
                "Was weiss ich nach kurzer Recherche? (nachher löschen)",
                "New Note Checks",
            ]

            cleaned = doc_content
            for heading in ignored_headings:
                cleaned = re.sub(
                    rf"(?ims)^##\s+{re.escape(heading)}\s*$.*?(?=^##\s+|\Z)",
                    "",
                    cleaned,
                )

            return cleaned

        except Exception:
            logger.error("Failed while stripping ignored sections\n%s", traceback.format_exc())
            adieu(1)

    def __extract_subsection_block(self, doc_content: str, subsection_name: str) -> str:
        try:
            match = re.search(
                rf"(?ims)^####\s+{re.escape(subsection_name)}\s*$\n(.*?)(?=^####\s+|^##\s+|\Z)",
                doc_content,
            )
            return match.group(1).strip() if match else ""

        except Exception:
            logger.error("Failed to extract subsection %s\n%s", subsection_name, traceback.format_exc())
            adieu(1)

    def __extract_markdown_links(self, text: str) -> list[str]:
        try:
            links = re.findall(r"\[[^\]]+\]\((https?://[^)\s]+)\)", text)
            links.extend(re.findall(r"(?<!\()\bhttps?://[^\s)>]+", text))

            deduped: list[str] = []
            for link in links:
                if link not in deduped:
                    deduped.append(link)

            return deduped

        except Exception:
            logger.error("Failed to parse markdown links\n%s", traceback.format_exc())
            adieu(1)

    def __to_db_text(self, value: str | list[str]) -> str:
        try:
            if isinstance(value, list):
                return json.dumps(value, ensure_ascii=False) if value else "N/A"
            return value if value else "N/A"

        except Exception:
            logger.error("Failed to convert parser value to database text\n%s", traceback.format_exc())
            adieu(1)

    def __parse_title_from_doc(self, file_name: str) -> str:
        try:
            title = Path(file_name).stem.strip()
            return title if title else "N/A"

        except Exception:
            logger.error("Failed to parse title from %s\n%s", file_name, traceback.format_exc())
            adieu(1)

    def __parse_created_at_from_doc(self, doc_content: str) -> str:
        try:
            cleaned = self.__strip_ignored_sections(doc_content)
            match = re.search(r"(?im)^>\s*Erstellt\s*:\s*(\d{2}\.\d{2}\.\d{4})\s*$", cleaned)
            return match.group(1) if match else "N/A"

        except Exception:
            logger.error("Failed to parse created_at\n%s", traceback.format_exc())
            adieu(1)

    def __parse_changed_at_from_doc(self, doc_content: str) -> str:
        try:
            cleaned = self.__strip_ignored_sections(doc_content)
            dates = re.findall(r"(?im)^>\s*Überarbeitet\s+am\s*:\s*(\d{2}\.\d{2}\.\d{4})\b", cleaned)

            if not dates:
                return "N/A"

            unique_dates = sorted(set(dates), key=lambda d: now_in_zurich_str())
            return json.dumps(unique_dates, ensure_ascii=False)

        except Exception:
            logger.error("Failed to parse changed_at\n%s", traceback.format_exc())
            adieu(1)

    def __parse_links_from_doc(self, doc_content: str) -> str:
        try:
            cleaned = self.__strip_ignored_sections(doc_content)
            external_refs_block = self.__extract_subsection_block(cleaned, "Externe Referenzen")
            links = self.__extract_markdown_links(external_refs_block) if external_refs_block else []
            return self.__to_db_text(links)

        except Exception:
            logger.error("Failed to parse links\n%s", traceback.format_exc())
            adieu(1)

    def __parse_video_links_from_doc(self, doc_content: str) -> str:
        try:
            cleaned = self.__strip_ignored_sections(doc_content)
            video_block = self.__extract_subsection_block(cleaned, "Erklärvideo")
            links = self.__extract_markdown_links(video_block) if video_block else []
            return self.__to_db_text(links)

        except Exception:
            logger.error("Failed to parse video links\n%s", traceback.format_exc())
            adieu(1)

    def __parse_tags_from_doc(self, doc_content: str) -> str:
        try:
            cleaned = self.__strip_ignored_sections(doc_content)
            tags_block = self.__extract_subsection_block(cleaned, "Page Tags")
            tags = re.findall(r"(?<!\w)#[-\w]+", tags_block)
            tags = list(dict.fromkeys(tags))
            return self.__to_db_text(tags)

        except Exception:
            logger.error("Failed to parse tags\n%s", traceback.format_exc())
            adieu(1)

    def __enumerate_compliance(self, doc_content: str) -> tuple[str, str]:
        try:
            cleaned = self.__strip_ignored_sections(doc_content)
            noncompliance_reasons: list[str] = []

            beschreibung_match = re.search(r"(?ims)^##\s+Beschreibung\s*$\n(.*?)(?=^#{1,6}\s+|\Z)", cleaned)
            beschreibung_text = beschreibung_match.group(1).strip() if beschreibung_match else ""
            sentence_count = len([s for s in re.split(r"(?<=[.!?])\s+", beschreibung_text) if s.strip()])
            beschreibung_ok = sentence_count <= 3 and bool(beschreibung_text)
            if not beschreibung_ok:
                noncompliance_reasons.append(
                    "Beschreibung: Maximal 3 Sätze!"
                )

            external_refs_block = self.__extract_subsection_block(cleaned, "Externe Referenzen")
            external_links = self.__extract_markdown_links(external_refs_block) if external_refs_block else []
            external_links_ok = len(external_links) >= 1
            if not external_links_ok:
                noncompliance_reasons.append("Links: Mind. 1 externer Link")

            tags_block = self.__extract_subsection_block(cleaned, "Page Tags")
            tags = list(dict.fromkeys(re.findall(r"(?<!\w)#[-\w]+", tags_block)))
            tags_ok = len(tags) >= 2
            if not tags_ok:
                noncompliance_reasons.append("Tags: Mind. 2 Tags")

            requires_video = len(cleaned) > 6000
            if requires_video:
                video_block = self.__extract_subsection_block(cleaned, "Erklärvideo")
                video_links = self.__extract_markdown_links(video_block) if video_block else []
                video_ok = len(video_links) >= 1
                if not video_ok:
                    noncompliance_reasons.append(
                        "Erklärvideo: ab 6000 Zeichen"
                    )
            else:
                video_ok = True

            is_compliant = "true" if (beschreibung_ok and external_links_ok and tags_ok and video_ok) else "false"
            return is_compliant, self.__to_db_text(noncompliance_reasons)

        except Exception:
            logger.error("Failed to evaluate compliance\n%s", traceback.format_exc())
            adieu(1)

    def parse_and_add_ALL_docs_to_db(self) -> None:
        try:
            db_object = db()
            sync_time = now_in_zurich_str()
            logger.info("Starting full docs sync at %s", sync_time)
            scanned_doc_titles: set[str] = set()

            for doc_full_path in self.__get_full_document_list():
                with open(doc_full_path, "r", encoding="utf-8") as f:
                    file_contents = f.read()

                is_compliant, noncompliance_reason = self.__enumerate_compliance(file_contents)

                append_dict = {
                    "title": self.__parse_title_from_doc(doc_full_path),
                    "created_at": self.__parse_created_at_from_doc(file_contents),
                    "changed_at": self.__parse_changed_at_from_doc(file_contents),
                    "links": self.__parse_links_from_doc(file_contents),
                    "video_links": self.__parse_video_links_from_doc(file_contents),
                    "tags": self.__parse_tags_from_doc(file_contents),
                    "is_compliant": is_compliant,
                    "noncompliance_reason": noncompliance_reason,
                    "manual_compliant_override": "false",
                }
                scanned_doc_titles.add(append_dict.get("title", "N/A"))

                existing_docs = db_object.get_docs_by_name(append_dict.get("title", "N/A"))
                if existing_docs:
                    first_existing = next(iter(existing_docs.values()))

                    db_object.log_change_if_needed(first_existing, append_dict, sync_time)
                    db_object.update_docs_by_id(append_dict, first_existing.get("id", "N/A"))
                else:
                    db_object.create_new_docs_entry(append_dict)

            for existing_doc in db_object.get_all_docs().values():
                existing_title = existing_doc.get("title", "N/A")
                existing_id = existing_doc.get("id")
                if existing_title not in scanned_doc_titles and isinstance(existing_id, int):
                    db_object.delete_docs_by_id(existing_id)
                    logger.info("Deleted stale docs entry id=%s title=%s", existing_id, existing_title)

            db_object.update_last_sync_time(sync_time)
            db_object.trim_old_change_versions(10)
            logger.info("Full docs sync completed")

        except Exception:
            logger.error("Full docs sync failed\n%s", traceback.format_exc())
            adieu(1)

    def _clean_note(self, note: str) -> str:
        try:
            return re.sub(r"\s*\(.*?\)", "", note).strip()
        except Exception:
            logger.error("Failed to clean note\n%s", traceback.format_exc())
            adieu(1)

    def _parse_todo_type(self, todo_type: str) -> list[str]:
        try:
            parts = [part.strip() for part in todo_type.split("/") if part.strip()]
            return parts if parts else ["N/A"]
        except Exception:
            logger.error("Failed to parse todo type\n%s", traceback.format_exc())
            adieu(1)

    def _parse_todo_progress(self, raw_progress: str) -> str:
        try:
            return self.PROGRESS_ICON_TO_STATE.get(raw_progress.strip(), "Not Started")
        except Exception:
            logger.error("Failed to parse todo progress\n%s", traceback.format_exc())
            adieu(1)

    def parse_todos_from_markdown(self) -> list[dict]:
        try:
            if not self.todo_file_path:
                raise Exception("Todo path missing in conf.json")

            todo_path = Path(self.todo_file_path)
            with open(todo_path, "r", encoding="utf-8") as file:
                content = file.read()

            table_pattern = re.compile(
                r"\|\s*Note\s*\|\s*Type\s*\|\s*Progress\s*\|\s*last Update\s*\|\n"
                r"\|[^\n]+\|\n"
                r"((?:\|[^\n]+\|\n?)*)",
                re.MULTILINE,
            )
            match = table_pattern.search(content)
            if not match:
                return []

            rows = [line.strip() for line in match.group(1).splitlines() if line.strip().startswith("|")]
            todos: list[dict] = []

            for row in rows:
                parts = [cell.strip() for cell in row.strip("|").split("|")]
                if len(parts) != 4:
                    continue

                note, todo_type, progress, last_update = parts
                todos.append(
                    {
                        "note": self._clean_note(note),
                        "type": json.dumps(self._parse_todo_type(todo_type), ensure_ascii=False),
                        "progress": self._parse_todo_progress(progress),
                        "last_update": last_update,
                    }
                )

            return todos
        except Exception:
            logger.error("Failed to parse todos markdown\n%s", traceback.format_exc())
            adieu(1)

    def sync_todos_to_db(self) -> list[dict]:
        try:
            todos = self.parse_todos_from_markdown()
            db().replace_all_todos(todos)
            logger.info("Todo sync completed with %s entries", len(todos))
            return todos
        except Exception:
            logger.error("Todo sync failed\n%s", traceback.format_exc())
            adieu(1)
