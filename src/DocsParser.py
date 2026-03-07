import re
import json
import traceback
from pathlib import Path
from datetime import datetime
from sys import exit as adieu

from .DatabaseConnector import db


class DocsParser:
    def __init__(self) -> None:
        try:
            # -- Get config-parameters --
            path = Path(__file__).resolve().parent.parent / "conf.json"

            with open(f"{path}", "r") as f:
                j: dict = json.loads(f.read())

                self.docs_path: str = j.get("docs", {}).get("full_path_to_docs", False)

            if self.docs_path == False:
                raise Exception("Docs-Pfad wurde nicht gefunden oder ist ungültig!")

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
            print(traceback.format_exc())
            adieu(1)

    def __extract_subsection_block(self, doc_content: str, subsection_name: str) -> str:
        try:
            match = re.search(
                rf"(?ims)^####\s+{re.escape(subsection_name)}\s*$\n(.*?)(?=^####\s+|^##\s+|\Z)",
                doc_content,
            )
            return match.group(1).strip() if match else ""

        except Exception:
            print(traceback.format_exc())
            adieu(1)

    def __extract_markdown_links(self, text: str) -> list[str]:
        try:
            # [label](url)
            links = re.findall(r"\[[^\]]+\]\((https?://[^)\s]+)\)", text)
            # plain URL fallback (e.g. in copied citations)
            links.extend(re.findall(r"(?<!\()\bhttps?://[^\s)>]+", text))

            # deduplicate while preserving order
            deduped: list[str] = []
            for link in links:
                if link not in deduped:
                    deduped.append(link)

            return deduped

        except Exception:
            print(traceback.format_exc())
            adieu(1)

    def __to_db_text(self, value: str | list[str]) -> str:
        try:
            if isinstance(value, list):
                return json.dumps(value, ensure_ascii=False) if value else "N/A"
            return value if value else "N/A"

        except Exception:
            print(traceback.format_exc())
            adieu(1)

    def __parse_title_from_doc(self, file_name: str) -> str:
        try:
            title = Path(file_name).stem.strip()
            return title if title else "N/A"

        except Exception:
            print(traceback.format_exc())
            adieu(1)

    def __parse_created_at_from_doc(self, doc_content: str) -> str:
        try:
            cleaned = self.__strip_ignored_sections(doc_content)
            match = re.search(r"(?im)^>\s*Erstellt\s*:\s*(\d{2}\.\d{2}\.\d{4})\s*$", cleaned)
            return match.group(1) if match else "N/A"

        except Exception:
            print(traceback.format_exc())
            adieu(1)

    def __parse_changed_at_from_doc(self, doc_content: str) -> str:
        try:
            cleaned = self.__strip_ignored_sections(doc_content)
            dates = re.findall(r"(?im)^>\s*Überarbeitet\s+am\s*:\s*(\d{2}\.\d{2}\.\d{4})\b", cleaned)

            if not dates:
                return "N/A"

            unique_dates = sorted(
                set(dates),
                key=lambda d: datetime.strptime(d, "%d.%m.%Y"),
            )
            return json.dumps(unique_dates, ensure_ascii=False)

        except Exception:
            print(traceback.format_exc())
            adieu(1)

    def __parse_links_from_doc(self, doc_content: str) -> str:
        try:
            cleaned = self.__strip_ignored_sections(doc_content)
            external_refs_block = self.__extract_subsection_block(cleaned, "Externe Referenzen")
            links = self.__extract_markdown_links(external_refs_block) if external_refs_block else []
            return self.__to_db_text(links)

        except Exception:
            print(traceback.format_exc())
            adieu(1)

    def __parse_video_links_from_doc(self, doc_content: str) -> str:
        try:
            cleaned = self.__strip_ignored_sections(doc_content)
            video_block = self.__extract_subsection_block(cleaned, "Erklärvideo")
            links = self.__extract_markdown_links(video_block) if video_block else []
            return self.__to_db_text(links)

        except Exception:
            print(traceback.format_exc())
            adieu(1)

    def __parse_tags_from_doc(self, doc_content: str) -> str:
        try:
            cleaned = self.__strip_ignored_sections(doc_content)
            tags_block = self.__extract_subsection_block(cleaned, "Page Tags")
            tags = re.findall(r"(?<!\w)#[-\w]+", tags_block)
            tags = list(dict.fromkeys(tags))
            return self.__to_db_text(tags)

        except Exception:
            print(traceback.format_exc())
            adieu(1)

    def __enumerate_is_compliant(self, doc_content: str) -> str:
        try:
            cleaned = self.__strip_ignored_sections(doc_content)

            # Beschreibung: max 3 Sätze
            beschreibung_match = re.search(
                r"(?ims)^##\s+Beschreibung\s*$\n(.*?)(?=^##\s+|\Z)",
                cleaned,
            )
            beschreibung_text = beschreibung_match.group(1).strip() if beschreibung_match else ""
            sentence_count = len(
                [s for s in re.split(r"(?<=[.!?])\s+", beschreibung_text) if s.strip()]
            )
            beschreibung_ok = sentence_count <= 3 and bool(beschreibung_text)

            # Externe Referenzen: mindestens 1
            external_refs_block = self.__extract_subsection_block(cleaned, "Externe Referenzen")
            external_links = self.__extract_markdown_links(external_refs_block) if external_refs_block else []
            external_links_ok = len(external_links) >= 1

            # Tags: mindestens 2
            tags_block = self.__extract_subsection_block(cleaned, "Page Tags")
            tags = list(dict.fromkeys(re.findall(r"(?<!\w)#[-\w]+", tags_block)))
            tags_ok = len(tags) >= 2

            # Video erst nötig ab ca. 6000 Zeichen (ohne ignorierte Kapitel)
            requires_video = len(cleaned) > 6000
            if requires_video:
                video_block = self.__extract_subsection_block(cleaned, "Erklärvideo")
                video_links = self.__extract_markdown_links(video_block) if video_block else []
                video_ok = len(video_links) >= 1
            else:
                video_ok = True

            return "true" if (beschreibung_ok and external_links_ok and tags_ok and video_ok) else "false"

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
                    "title": self.__parse_title_from_doc(doc_full_path),
                    "created_at": self.__parse_created_at_from_doc(file_contents),
                    "changed_at": self.__parse_changed_at_from_doc(file_contents),
                    "links": self.__parse_links_from_doc(file_contents),
                    "video_links": self.__parse_video_links_from_doc(file_contents),
                    "tags": self.__parse_tags_from_doc(file_contents),
                    "is_compliant": self.__enumerate_is_compliant(file_contents),
                }

                r = db_object.check_if_doc_is_already_in_db(append_dict.get("title", "N/A"))
                if r.get("bool", "N/A") == True:
                    db_object.update_docs_by_id(append_dict, r.get("id", "N/A"))
                else:
                    db_object.create_new_docs_entry(append_dict)

        except Exception:
            print(traceback.format_exc())
            adieu(1)
