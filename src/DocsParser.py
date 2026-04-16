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
    UNDER_CONSTRUCTION_MARKER = "> ==unter Bearbeitung=="
    DEFAULT_REQUIRED_NOTE_STRUCTURE_STRINGS = [
        "## Zusätzliche Ressourcen",
        "#### Erklärvideo",
        "#### Externe Referenzen",
        "#### Page History",
        "#### Page Tags",
    ]
    DEFAULT_COMPLIANCE_CHECK = {
        "structure": {
            "enabled": True,
            "strings_to_check": DEFAULT_REQUIRED_NOTE_STRUCTURE_STRINGS,
        },
        "created": {
            "enabled": True,
        },
        "beschreibung": {
            "enabled": True,
            "max": 3,
        },
        "external_links": {
            "enabled": True,
            "min": 1,
        },
        "tags": {
            "enabled": True,
            "min": 2,
        },
        "video_links": {
            "enabled": True,
            "char": 6000,
        },
        "ai_feedback": {
            "enabled": True,
            "min": 80,
        },
    }
    PROGRESS_ICON_TO_STATE = {
        "![[not started.png]]": "Not Started",
        "![[in progress.png]]": "In Progress",
        "![[done.png]]": "Done",
        "![[not needed.png]]": "Not Needed",
    }
    SW_PROGRESS_STATE_TO_RAW = {
        "Done": "![[done.png]]",
        "In Progress": "![[in progress.png]]",
        "Not Needed": "![[not needed.png]]",
        "Not Started": "![[not started.png]]",
        "": "",
    }
    TODO_PRIORITY_VALUES = {"low": "Low", "medium": "Medium", "high": "High"}
    KANBAN_STATUS_VALUES = {"not started": "Not Started", "in progress": "In Progress", "done": "Done"}
    def __init__(self) -> None:
        try:
            path = Path(__file__).resolve().parent.parent / "conf.json"
            with open(path, "r", encoding="utf-8") as f:
                j: dict = json.loads(f.read())
                self.docs_path: str = j.get("docs", {}).get("full_path_to_docs", False)
                self.todo_file_path: str = j.get("todo", {}).get("full_path_to_todo_file", False)
                self.deadlines_file_path: str = j.get("deadlines", {}).get("full_path_to_deadlines_file", "/the-knowledge/Deadlines.md")
                self.hslu_base_path: str = j.get("hslu", {}).get("full_path_to_hslu", "/the-knowledge/00_HSLU")
                self.ai_feedback_path: str = j.get("ai_feedback", {}).get("output_path") or j.get("ai_feedback", {}).get("the_knowledge_path", "")
                self.learning_path: str = j.get("learning", {}).get("learning_path", "/the-knowledge/07_LEARNINGS")
                self.projects_root_path: str = j.get("projects", {}).get("root_path", "/the-knowledge/01_PROJ")
                self.compliance_check: dict = self.__load_compliance_check_config(j.get("compliance_check", {}))
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
                rf"(?ims)^####\s+{re.escape(subsection_name)}\s*$\n(.*?)(?=^#{{1,6}}\s+|\Z)",
                doc_content,
            )
            return match.group(1).strip() if match else ""
        except Exception:
            logger.error("Failed to extract subsection %s\n%s", subsection_name, traceback.format_exc())
            adieu(1)
    def __extract_markdown_links(self, text: str) -> list[str]:
        try:
            markdown_links = re.findall(r"\[[^\]]+\]\((https?://[^)\s]+)\)", text)
            remaining_text = re.sub(r"\[[^\]]+\]\(https?://[^)\s]+\)", "", text)
            plain_links = re.findall(r"\bhttps?://[^\s)>]+", remaining_text)
            links = [*markdown_links, *plain_links]
            deduped: list[str] = []
            for link in links:
                if link not in deduped:
                    deduped.append(link)
            return deduped
        except Exception:
            logger.error("Failed to parse markdown links\n%s", traceback.format_exc())
            adieu(1)
    def __extract_markdown_link_map(self, text: str) -> dict[str, str]:
        try:
            mapping: dict[str, str] = {}
            markdown_links = re.findall(r"\[([^\]]+)\]\((https?://[^)\s]+)\)", text)
            for description, link in markdown_links:
                normalized_link = str(link).strip()
                normalized_description = str(description).strip()
                if normalized_link:
                    mapping[normalized_link] = normalized_description or normalized_link

            remaining_text = re.sub(r"\[[^\]]+\]\(https?://[^)\s]+\)", "", text)
            plain_links = re.findall(r"\bhttps?://[^\s)>]+", remaining_text)
            for link in plain_links:
                normalized_link = str(link).strip()
                if normalized_link and normalized_link not in mapping:
                    mapping[normalized_link] = normalized_link

            return mapping
        except Exception:
            logger.error("Failed to parse markdown link map\n%s", traceback.format_exc())
            adieu(1)
    def __coerce_bool(self, value: object, fallback: bool) -> bool:
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            lowered = value.strip().casefold()
            if lowered in {"true", "1", "yes", "y", "on"}:
                return True
            if lowered in {"false", "0", "no", "n", "off"}:
                return False
        return fallback
    def __coerce_int(self, value: object, fallback: int, minimum: int = 0) -> int:
        try:
            parsed = int(str(value).strip())
            if parsed < minimum:
                return fallback
            return parsed
        except (TypeError, ValueError):
            return fallback
    def __coerce_string_list(self, value: object, fallback: list[str]) -> list[str]:
        if not isinstance(value, list):
            return fallback
        normalized = [str(item).strip() for item in value if str(item).strip()]
        return normalized if normalized else fallback
    def __load_compliance_check_config(self, raw_config: object) -> dict:
        defaults = self.DEFAULT_COMPLIANCE_CHECK
        config = raw_config if isinstance(raw_config, dict) else {}

        structure_raw = config.get("structure", {}) if isinstance(config.get("structure", {}), dict) else {}
        created_raw = config.get("created", {}) if isinstance(config.get("created", {}), dict) else {}
        beschreibung_raw = config.get("beschreibung", {}) if isinstance(config.get("beschreibung", {}), dict) else {}
        external_links_raw = config.get("external_links", {}) if isinstance(config.get("external_links", {}), dict) else {}
        tags_raw = config.get("tags", {}) if isinstance(config.get("tags", {}), dict) else {}
        video_links_raw = config.get("video_links", {}) if isinstance(config.get("video_links", {}), dict) else {}
        ai_feedback_raw = config.get("ai_feedback", {}) if isinstance(config.get("ai_feedback", {}), dict) else {}

        return {
            "structure": {
                "enabled": self.__coerce_bool(structure_raw.get("enabled"), defaults["structure"]["enabled"]),
                "strings_to_check": self.__coerce_string_list(
                    structure_raw.get("strings_to_check"),
                    defaults["structure"]["strings_to_check"],
                ),
            },
            "created": {
                "enabled": self.__coerce_bool(created_raw.get("enabled"), defaults["created"]["enabled"]),
            },
            "beschreibung": {
                "enabled": self.__coerce_bool(beschreibung_raw.get("enabled"), defaults["beschreibung"]["enabled"]),
                "max": self.__coerce_int(beschreibung_raw.get("max"), defaults["beschreibung"]["max"], minimum=1),
            },
            "external_links": {
                "enabled": self.__coerce_bool(external_links_raw.get("enabled"), defaults["external_links"]["enabled"]),
                "min": self.__coerce_int(external_links_raw.get("min"), defaults["external_links"]["min"], minimum=0),
            },
            "tags": {
                "enabled": self.__coerce_bool(tags_raw.get("enabled"), defaults["tags"]["enabled"]),
                "min": self.__coerce_int(tags_raw.get("min"), defaults["tags"]["min"], minimum=0),
            },
            "video_links": {
                "enabled": self.__coerce_bool(video_links_raw.get("enabled"), defaults["video_links"]["enabled"]),
                "char": self.__coerce_int(video_links_raw.get("char"), defaults["video_links"]["char"], minimum=1),
            },
            "ai_feedback": {
                "enabled": self.__coerce_bool(ai_feedback_raw.get("enabled"), defaults["ai_feedback"]["enabled"]),
                "min": self.__coerce_int(ai_feedback_raw.get("min"), defaults["ai_feedback"]["min"], minimum=0),
            },
        }
    def __to_db_text(self, value: str | list[str] | dict[str, str]) -> str:
        try:
            if isinstance(value, (list, dict)):
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
    def __is_under_construction(self, doc_content: str) -> bool:
        try:
            normalized = str(doc_content or "").lstrip("\ufeff")
            first_line = normalized.splitlines()[0].strip() if normalized else ""
            return first_line == self.UNDER_CONSTRUCTION_MARKER
        except Exception:
            logger.error("Failed to detect under-construction marker\n%s", traceback.format_exc())
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
            link_map = self.__extract_markdown_link_map(external_refs_block) if external_refs_block else {}
            return self.__to_db_text(link_map)
        except Exception:
            logger.error("Failed to parse links\n%s", traceback.format_exc())
            adieu(1)
    def __parse_video_links_from_doc(self, doc_content: str) -> str:
        try:
            cleaned = self.__strip_ignored_sections(doc_content)
            video_block = self.__extract_subsection_block(cleaned, "Erklärvideo")
            link_map = self.__extract_markdown_link_map(video_block) if video_block else {}
            return self.__to_db_text(link_map)
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
    def __normalize_doc_key(self, value: str) -> str:
        return re.sub(r"\.md$", "", str(value or "").strip(), flags=re.IGNORECASE).casefold()

    def __build_learning_doc_keys(self, learning_row: dict) -> set[str]:
        keys: set[str] = set()
        for raw_value in (
            learning_row.get("source_note_name", ""),
            learning_row.get("file_name", ""),
        ):
            normalized = self.__normalize_doc_key(raw_value)
            if not normalized:
                continue
            keys.add(normalized)
            keys.add(re.sub(r"\s*-\s*learning$", "", normalized, flags=re.IGNORECASE).strip())
        return {item for item in keys if item}

    def __enumerate_compliance(self, doc_content: str, doc_title: str, database: db) -> tuple[str, str]:
        try:
            cleaned = self.__strip_ignored_sections(doc_content)
            noncompliance_reasons: list[str] = []
            compliance_conf = self.compliance_check

            structure_enabled = compliance_conf["structure"]["enabled"]
            note_structure_ok = (not structure_enabled) or self.__has_required_note_structure(cleaned)
            if structure_enabled and not note_structure_ok:
                noncompliance_reasons.append("Struktur: Nicht alle Kapitel da")

            created_at = self.__parse_created_at_from_doc(cleaned)
            created_enabled = compliance_conf["created"]["enabled"]
            created_at_ok = (not created_enabled) or (created_at != "N/A")
            if created_enabled and not created_at_ok:
                noncompliance_reasons.append("Erstelldatum: Nicht vorhanden")

            beschreibung_max = compliance_conf["beschreibung"]["max"]
            beschreibung_text = self.__extract_beschreibung_text(cleaned)
            sentence_count = len([s for s in re.split(r"(?<=[.!?])\s+", beschreibung_text) if s.strip()])
            beschreibung_enabled = compliance_conf["beschreibung"]["enabled"]
            beschreibung_ok = (not beschreibung_enabled) or (sentence_count <= beschreibung_max and bool(beschreibung_text))
            if beschreibung_enabled and not beschreibung_ok:
                noncompliance_reasons.append(
                    f"Beschreibung: Maximal {beschreibung_max} Sätze!"
                )

            external_refs_block = self.__extract_subsection_block(cleaned, "Externe Referenzen")
            external_links = self.__extract_markdown_links(external_refs_block) if external_refs_block else []
            external_links_min = compliance_conf["external_links"]["min"]
            external_links_enabled = compliance_conf["external_links"]["enabled"]
            external_links_ok = (not external_links_enabled) or (len(external_links) >= external_links_min)
            if external_links_enabled and not external_links_ok:
                noncompliance_reasons.append(f"Links: Mind. {external_links_min} externer Link")

            tags_block = self.__extract_subsection_block(cleaned, "Page Tags")
            tags = list(dict.fromkeys(re.findall(r"(?<!\w)#[-\w]+", tags_block)))
            tags_min = compliance_conf["tags"]["min"]
            tags_enabled = compliance_conf["tags"]["enabled"]
            tags_ok = (not tags_enabled) or (len(tags) >= tags_min)
            if tags_enabled and not tags_ok:
                noncompliance_reasons.append(f"Tags: Mind. {tags_min} Tags")

            video_char_threshold = compliance_conf["video_links"]["char"]
            video_links_enabled = compliance_conf["video_links"]["enabled"]
            requires_video = video_links_enabled and (len(cleaned) > video_char_threshold)
            if requires_video:
                video_block = self.__extract_subsection_block(cleaned, "Erklärvideo")
                video_links = self.__extract_markdown_links(video_block) if video_block else []
                video_ok = len(video_links) >= 1
                if not video_ok:
                    noncompliance_reasons.append(
                        f"Erklärvideo: ab {video_char_threshold} Zeichen"
                    )
            else:
                video_ok = True

            ai_feedback_conf = compliance_conf["ai_feedback"]
            ai_feedback_enabled = ai_feedback_conf["enabled"]
            ai_feedback_min = ai_feedback_conf["min"]
            ai_feedback_ok = True
            if ai_feedback_enabled:
                latest_feedback = database.get_latest_ai_feedback_for_file(doc_title)
                if latest_feedback is not None:
                    try:
                        latest_score = float(latest_feedback.get("score"))
                        ai_feedback_ok = latest_score >= ai_feedback_min
                    except (TypeError, ValueError):
                        ai_feedback_ok = False
                    if not ai_feedback_ok:
                        noncompliance_reasons.append("AI Feedback: Wert zu niedrig")

            is_compliant = (
                "true"
                if (
                    note_structure_ok
                    and created_at_ok
                    and beschreibung_ok
                    and external_links_ok
                    and tags_ok
                    and video_ok
                    and ai_feedback_ok
                )
                else "false"
            )
            return is_compliant, self.__to_db_text(noncompliance_reasons)
        except Exception:
            logger.error("Failed to evaluate compliance\n%s", traceback.format_exc())
            adieu(1)

    def __has_required_note_structure(self, doc_content: str) -> bool:
        try:
            normalized_content = str(doc_content or "")
            required_strings = self.compliance_check["structure"]["strings_to_check"]
            return all(required_string in normalized_content for required_string in required_strings)
        except Exception:
            logger.error("Failed to check note structure compliance\n%s", traceback.format_exc())
            adieu(1)

    def __extract_beschreibung_text(self, doc_content: str) -> str:
        try:
            cleaned = self.__strip_ignored_sections(doc_content)
            beschreibung_match = re.search(r"(?ims)^##\s+Beschreibung\s*$\n(.*?)(?=^#{1,6}\s+|\Z)", cleaned)
            return beschreibung_match.group(1).strip() if beschreibung_match else ""
        except Exception:
            logger.error("Failed to extract Beschreibung section\n%s", traceback.format_exc())
            adieu(1)

    def get_doc_titles_by_description_query(self, query: str) -> set[str]:
        try:
            normalized_query = str(query or "").strip().casefold()
            if not normalized_query:
                return set()

            matching_titles: set[str] = set()
            for doc_full_path in self.__get_full_document_list():
                with open(doc_full_path, "r", encoding="utf-8") as f:
                    file_contents = f.read()

                beschreibung_text = self.__extract_beschreibung_text(file_contents)
                if normalized_query in beschreibung_text.casefold():
                    matching_titles.add(self.__parse_title_from_doc(doc_full_path))

            return matching_titles
        except Exception:
            logger.error("Failed to search docs by Beschreibung\n%s", traceback.format_exc())
            adieu(1)
    def parse_and_add_ALL_docs_to_db(self) -> None:
        try:
            db_object = db()
            sync_time = now_in_zurich_str()
            logger.info("Starting full docs sync at %s", sync_time)
            scanned_doc_titles: set[str] = set()
            collected_tags: set[str] = set()
            learning_doc_keys: set[str] = set()
            ai_feedback_doc_keys = {
                self.__normalize_doc_key(row.get("file_name", ""))
                for row in db_object.get_all_ai_feedback()
                if self.__normalize_doc_key(row.get("file_name", ""))
            }
            for learning_row in db_object.get_all_learnings():
                learning_doc_keys.update(self.__build_learning_doc_keys(learning_row))
            for doc_full_path in self.__get_full_document_list():
                with open(doc_full_path, "r", encoding="utf-8") as f:
                    file_contents = f.read()
                doc_title = self.__parse_title_from_doc(doc_full_path)
                normalized_doc_title = self.__normalize_doc_key(doc_title)
                is_under_construction = self.__is_under_construction(file_contents)
                if is_under_construction:
                    is_compliant = "Not Determined"
                    noncompliance_reason = "N/A"
                else:
                    is_compliant, noncompliance_reason = self.__enumerate_compliance(file_contents, doc_title, db_object)
                append_dict = {
                    "title": doc_title,
                    "created_at": self.__parse_created_at_from_doc(file_contents),
                    "changed_at": self.__parse_changed_at_from_doc(file_contents),
                    "links": self.__parse_links_from_doc(file_contents),
                    "video_links": self.__parse_video_links_from_doc(file_contents),
                    "tags": self.__parse_tags_from_doc(file_contents),
                    "is_compliant": is_compliant,
                    "noncompliance_reason": noncompliance_reason,
                    "manual_compliant_override": "false",
                    "is_under_construction": "true" if is_under_construction else "false",
                    "has_learning": "true" if normalized_doc_title in learning_doc_keys else "false",
                    "has_ai_feedback": "true" if normalized_doc_title in ai_feedback_doc_keys else "false",
                }
                raw_tags = append_dict.get("tags", "N/A")
                if isinstance(raw_tags, str) and raw_tags.startswith("[") and raw_tags.endswith("]"):
                    try:
                        parsed_tags = json.loads(raw_tags)
                        if isinstance(parsed_tags, list):
                            for tag in parsed_tags:
                                tag_value = str(tag).strip()
                                if tag_value:
                                    collected_tags.add(tag_value)
                    except json.JSONDecodeError:
                        pass
                scanned_doc_titles.add(append_dict.get("title", "N/A"))
                existing_docs = db_object.get_docs_by_name(append_dict.get("title", "N/A"))
                if existing_docs:
                    first_existing = next(iter(existing_docs.values()))
                    db_object.update_docs_by_id(append_dict, first_existing.get("id", "N/A"))
                else:
                    db_object.create_new_docs_entry(append_dict)
            for existing_doc in db_object.get_all_docs().values():
                existing_title = existing_doc.get("title", "N/A")
                existing_id = existing_doc.get("id")
                if existing_title not in scanned_doc_titles and isinstance(existing_id, int):
                    db_object.delete_docs_by_id(existing_id)
                    logger.info("Deleted stale docs entry id=%s title=%s", existing_id, existing_title)
            db_object.replace_all_tags(list(collected_tags))
            db_object.update_last_sync_time(sync_time)
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
    def _parse_todo_priority(self, raw_priority: str) -> str:
        try:
            normalized = str(raw_priority or "").strip().casefold()
            return self.TODO_PRIORITY_VALUES.get(normalized, "Medium")
        except Exception:
            logger.error("Failed to parse todo priority\n%s", traceback.format_exc())
            adieu(1)
    def _normalize_sw_progress(self, raw_value: str) -> str:
        try:
            value = raw_value.strip()
            if not value or value == "-":
                return ""
            lowered = value.casefold()
            if "done.png" in lowered:
                return "Done"
            if "in progress.png" in lowered:
                return "In Progress"
            if "not needed.png" in lowered:
                return "Not Needed"
            if "not started.png" in lowered:
                return "Not Started"
            if value in ("Done", "In Progress", "Not Needed", "Not Started"):
                return value
            return ""
        except Exception:
            logger.error("Failed to normalize HSLU SW progress\n%s", traceback.format_exc())
            adieu(1)

    def _normalize_hslu_deadline_marker(self, raw_value: str) -> str:
        try:
            value = str(raw_value or "").strip()
            if value.casefold() == "created":
                return "CREATED"
            return ""
        except Exception:
            logger.error("Failed to normalize HSLU deadline marker\n%s", traceback.format_exc())
            adieu(1)
    def _sw_progress_state_to_raw(self, state: str) -> str:
        try:
            normalized_state = state.strip()
            if normalized_state == "-":
                normalized_state = ""
            if normalized_state not in self.SW_PROGRESS_STATE_TO_RAW:
                raise ValueError(f"Invalid SW progress state: {state}")
            return self.SW_PROGRESS_STATE_TO_RAW[normalized_state]
        except Exception:
            logger.error("Failed to convert SW progress state to raw markdown\n%s", traceback.format_exc())
            adieu(1)
    def update_hslu_sw_status(
        self,
        semester: str,
        module: str,
        kw: str,
        sw: str,
        field: str,
        target_status: str,
    ) -> None:
        try:
            if field not in ("downloaded", "documented"):
                raise ValueError(f"Invalid field for update: {field}")
            index_file = Path(self.hslu_base_path) / semester / module / "Index.md"
            if not index_file.exists() or not index_file.is_file():
                raise FileNotFoundError(f"Could not find Index.md at {index_file}")
            raw_value = self._sw_progress_state_to_raw(target_status)
            lines = index_file.read_text(encoding="utf-8").splitlines(keepends=True)
            section_start = -1
            section_end = len(lines)
            for idx, line in enumerate(lines):
                if re.match(r"^##\s+Übersicht\s+SW\s*$", line.strip()):
                    section_start = idx
                    break
            if section_start == -1:
                raise ValueError("Section '## Übersicht SW' not found")
            for idx in range(section_start + 1, len(lines)):
                if re.match(r"^##\s+", lines[idx].strip()):
                    section_end = idx
                    break
            row_pattern = re.compile(
                r"^\|\s*(\d{1,2})\s*\|\s*(\d{1,2})\s*\|\s*(.*?)\s*\|\s*(.*?)\s*\|\s*(.*?)\s*\|\s*(.*?)\s*\|\s*$"
            )
            row_updated = False
            for idx in range(section_start + 1, section_end):
                match = row_pattern.match(lines[idx].rstrip("\n"))
                if not match:
                    continue
                current_kw, current_sw, thema, downloaded, documented, deadlines = match.groups()
                if current_kw.strip() != kw.strip() or current_sw.strip() != sw.strip():
                    continue
                if field == "downloaded":
                    downloaded = raw_value
                else:
                    documented = raw_value
                lines[idx] = f"| {current_kw.strip()} | {current_sw.strip()} | {thema.strip()} | {downloaded.strip()} | {documented.strip()} | {deadlines.strip()} |\n"
                row_updated = True
                break
            if not row_updated:
                raise ValueError(f"Could not find row for KW={kw}, SW={sw} in {index_file}")
            index_file.write_text("".join(lines), encoding="utf-8")
            logger.info(
                "Updated HSLU status in markdown semester=%s module=%s KW=%s SW=%s field=%s status=%s",
                semester,
                module,
                kw,
                sw,
                field,
                target_status,
            )
        except Exception:
            logger.error("Failed to update HSLU SW status in markdown\n%s", traceback.format_exc())
            adieu(1)

    def update_hslu_sw_deadline_marker(
        self,
        semester: str,
        module: str,
        kw: str,
        sw: str,
        marker: str,
    ) -> None:
        try:
            normalized_marker = self._normalize_hslu_deadline_marker(marker)
            index_file = Path(self.hslu_base_path) / semester / module / "Index.md"
            if not index_file.exists() or not index_file.is_file():
                raise FileNotFoundError(f"Could not find Index.md at {index_file}")
            lines = index_file.read_text(encoding="utf-8").splitlines(keepends=True)
            section_start = -1
            section_end = len(lines)
            for idx, line in enumerate(lines):
                if re.match(r"^##\s+Übersicht\s+SW\s*$", line.strip()):
                    section_start = idx
                    break
            if section_start == -1:
                raise ValueError("Section '## Übersicht SW' not found")
            for idx in range(section_start + 1, len(lines)):
                if re.match(r"^##\s+", lines[idx].strip()):
                    section_end = idx
                    break
            row_pattern = re.compile(
                r"^\|\s*(\d{1,2})\s*\|\s*(\d{1,2})\s*\|\s*(.*?)\s*\|\s*(.*?)\s*\|\s*(.*?)\s*\|\s*(.*?)\s*\|\s*$"
            )
            row_updated = False
            for idx in range(section_start + 1, section_end):
                match = row_pattern.match(lines[idx].rstrip("\n"))
                if not match:
                    continue
                current_kw, current_sw, thema, downloaded, documented, _deadlines = match.groups()
                if current_kw.strip() != kw.strip() or current_sw.strip() != sw.strip():
                    continue
                lines[idx] = (
                    f"| {current_kw.strip()} | {current_sw.strip()} | {thema.strip()} | "
                    f"{downloaded.strip()} | {documented.strip()} | {normalized_marker} |\n"
                )
                row_updated = True
                break
            if not row_updated:
                raise ValueError(f"Could not find row for KW={kw}, SW={sw} in {index_file}")
            index_file.write_text("".join(lines), encoding="utf-8")
            logger.info(
                "Updated HSLU deadline marker in markdown semester=%s module=%s KW=%s SW=%s marker=%s",
                semester,
                module,
                kw,
                sw,
                normalized_marker,
            )
        except Exception:
            logger.error("Failed to update HSLU SW deadline marker in markdown\n%s", traceback.format_exc())
            adieu(1)
    def _extract_uebersicht_sw_rows(self, markdown_content: str) -> list[list[str]]:
        try:
            section_match = re.search(
                r"(?ims)^##\s+Übersicht\s+SW\s*$\n(.*?)(?=^##\s+|\Z)",
                markdown_content,
            )
            if not section_match:
                return []
            section_block = section_match.group(1)
            row_pattern = re.compile(
                r"^\|\s*(\d{1,2})\s*\|\s*(\d{1,2})\s*\|\s*(.*?)\s*\|\s*(.*?)\s*\|\s*(.*?)\s*\|\s*(.*?)\s*\|\s*$",
                re.MULTILINE,
            )
            parsed_rows: list[list[str]] = []
            for kw, sw, thema, downloaded, documented, deadlines in row_pattern.findall(section_block):
                parsed_rows.append(
                    [
                        kw.strip(),
                        sw.strip(),
                        thema.strip(),
                        downloaded.strip(),
                        documented.strip(),
                        deadlines.replace("\n", " ").strip(),
                    ]
                )
            return parsed_rows
        except Exception:
            logger.error("Failed to extract Übersicht SW rows\n%s", traceback.format_exc())
            adieu(1)
    def parse_hslu_sw_overview(self) -> list[dict]:
        try:
            hslu_root = Path(self.hslu_base_path)
            if not hslu_root.exists() or not hslu_root.is_dir():
                logger.warning("HSLU path not found: %s", hslu_root)
                return []
            all_rows: list[dict] = []
            for semester_dir in sorted([p for p in hslu_root.iterdir() if p.is_dir()], key=lambda item: item.name.casefold()):
                semester_name = semester_dir.name
                for module_dir in sorted([p for p in semester_dir.iterdir() if p.is_dir()], key=lambda item: item.name.casefold()):
                    module_name = module_dir.name
                    index_file = module_dir / "Index.md"
                    if not index_file.exists() or not index_file.is_file():
                        continue
                    markdown_content = index_file.read_text(encoding="utf-8")
                    for kw, sw, thema, downloaded, documented, deadlines in self._extract_uebersicht_sw_rows(markdown_content):
                        all_rows.append(
                            {
                                "semester": semester_name,
                                "module": module_name,
                                "KW": kw,
                                "SW": sw,
                                "thema": thema,
                                "downloaded": self._normalize_sw_progress(downloaded),
                                "documented": self._normalize_sw_progress(documented),
                                "deadlines": self._normalize_hslu_deadline_marker(deadlines),
                            }
                        )
            logger.info("Parsed %s HSLU SW overview rows", len(all_rows))
            return all_rows
        except Exception:
            logger.error("Failed to parse HSLU SW overview\n%s", traceback.format_exc())
            adieu(1)
    def _split_markdown_table_row(self, row_line: str) -> list[str]:
        return [cell.strip() for cell in row_line.strip().strip("|").split("|")]

    def _parse_checklist_status(self, raw_value: str) -> str:
        return self._normalize_sw_progress(raw_value)

    def _find_hslu_semester_checklist_file(self, semester_dir: Path) -> Path | None:
        pattern = re.compile(r"^SE0\d\s*-\s*Semester\s+Checklist\.md$", re.IGNORECASE)
        for entry in sorted([p for p in semester_dir.iterdir() if p.is_file()], key=lambda item: item.name.casefold()):
            if pattern.match(entry.name):
                return entry
        return None

    def _extract_checklist_sections(self, markdown_content: str) -> list[tuple[str, str]]:
        section_pattern = re.compile(r"(?ms)^##\s+(.+?)\s*$\n(.*?)(?=^##\s+|\Z)")
        return [(title.strip(), block) for title, block in section_pattern.findall(markdown_content)]

    def _extract_table_lines(self, section_block: str) -> list[str]:
        return [line.strip() for line in section_block.splitlines() if line.strip().startswith("|")]

    def _parse_checklist_table_rows(self, header_cells: list[str], data_lines: list[str], semester: str, section: str, file_path: str) -> list[dict]:
        parsed_rows: list[dict] = []
        sw_column_index = -1
        for idx, header in enumerate(header_cells):
            if header.casefold() == "sw":
                sw_column_index = idx
                break
        for data_line in data_lines:
            cells = self._split_markdown_table_row(data_line)
            if len(cells) < len(header_cells):
                cells.extend([""] * (len(header_cells) - len(cells)))
            sw_value = ""
            if sw_column_index >= 0 and sw_column_index < len(cells):
                sw_match = re.search(r"\d{1,2}", cells[sw_column_index])
                sw_value = sw_match.group(0).zfill(2) if sw_match else ""
            if sw_column_index >= 0:
                for col_idx, header in enumerate(header_cells):
                    if col_idx == sw_column_index:
                        continue
                    parsed_rows.append(
                        {
                            "semester": semester,
                            "section": section,
                            "sw": sw_value,
                            "checklist_row": f"SW{sw_value}" if sw_value else "",
                            "checklist_item": header,
                            "status": self._parse_checklist_status(cells[col_idx] if col_idx < len(cells) else ""),
                            "file_path": file_path,
                        }
                    )
            else:
                if len(cells) < 2:
                    continue
                parsed_rows.append(
                    {
                        "semester": semester,
                        "section": section,
                        "sw": "",
                        "checklist_row": cells[0],
                        "checklist_item": cells[0],
                        "status": self._parse_checklist_status(cells[1]),
                        "file_path": file_path,
                    }
                )
        return parsed_rows

    def parse_hslu_semester_checklist(self) -> list[dict]:
        try:
            hslu_root = Path(self.hslu_base_path)
            if not hslu_root.exists() or not hslu_root.is_dir():
                logger.warning("HSLU path not found: %s", hslu_root)
                return []
            all_rows: list[dict] = []
            for semester_dir in sorted([p for p in hslu_root.iterdir() if p.is_dir()], key=lambda item: item.name.casefold()):
                checklist_file = self._find_hslu_semester_checklist_file(semester_dir)
                if not checklist_file:
                    continue
                markdown_content = checklist_file.read_text(encoding="utf-8")
                for section, block in self._extract_checklist_sections(markdown_content):
                    table_lines = self._extract_table_lines(block)
                    if len(table_lines) < 3:
                        continue
                    header_cells = self._split_markdown_table_row(table_lines[0])
                    data_lines = table_lines[2:]
                    all_rows.extend(
                        self._parse_checklist_table_rows(
                            header_cells=header_cells,
                            data_lines=data_lines,
                            semester=semester_dir.name,
                            section=section,
                            file_path=str(checklist_file),
                        )
                    )
            logger.info("Parsed %s HSLU semester checklist rows", len(all_rows))
            return all_rows
        except Exception:
            logger.error("Failed to parse HSLU semester checklist\n%s", traceback.format_exc())
            adieu(1)

    def update_hslu_semester_checklist_status(self, target: dict, target_status: str) -> None:
        try:
            file_path = Path(str(target.get("file_path", "")).strip())
            if not file_path.exists():
                raise FileNotFoundError(f"Checklist file not found: {file_path}")
            content = file_path.read_text(encoding="utf-8")
            raw_icon = self._sw_progress_state_to_raw(target_status)
            section = str(target.get("section", "")).strip()
            item = str(target.get("checklist_item", "")).strip()
            sw = str(target.get("sw", "") or "").strip()
            checklist_row = str(target.get("checklist_row", "") or "").strip()
            section_pattern = re.compile(rf"(?ms)(^##\s+{re.escape(section)}\s*$\n)(.*?)(?=^##\s+|\Z)")
            section_match = section_pattern.search(content)
            if not section_match:
                raise ValueError(f"Section not found: {section}")
            block = section_match.group(2)
            lines = block.splitlines(keepends=True)
            table_indices = [i for i, line in enumerate(lines) if line.strip().startswith("|")]
            if len(table_indices) < 3:
                raise ValueError("Checklist table not found")
            header_idx = table_indices[0]
            header_cells = self._split_markdown_table_row(lines[header_idx])
            data_start = table_indices[2]
            if any(header.casefold() == "sw" for header in header_cells):
                sw_idx = next(index for index, header in enumerate(header_cells) if header.casefold() == "sw")
                item_idx = next((index for index, header in enumerate(header_cells) if header == item), -1)
                if item_idx < 0:
                    raise ValueError(f"Checklist item column not found: {item}")
                for index in range(data_start, len(lines)):
                    if not lines[index].strip().startswith("|"):
                        continue
                    cells = self._split_markdown_table_row(lines[index])
                    if len(cells) < len(header_cells):
                        cells.extend([""] * (len(header_cells) - len(cells)))
                    row_sw_match = re.search(r"\d{1,2}", cells[sw_idx] if sw_idx < len(cells) else "")
                    row_sw = row_sw_match.group(0).zfill(2) if row_sw_match else ""
                    if row_sw != sw:
                        continue
                    cells[item_idx] = raw_icon
                    line_ending = "\n" if lines[index].endswith("\n") else ""
                    lines[index] = "| " + " | ".join(cells) + f" |{line_ending}"
                    break
                else:
                    raise ValueError(f"SW row not found for {sw}")
            else:
                for index in range(data_start, len(lines)):
                    if not lines[index].strip().startswith("|"):
                        continue
                    cells = self._split_markdown_table_row(lines[index])
                    if len(cells) < 2:
                        continue
                    if cells[0] != checklist_row:
                        continue
                    cells[1] = raw_icon
                    line_ending = "\n" if lines[index].endswith("\n") else ""
                    lines[index] = "| " + " | ".join(cells) + f" |{line_ending}"
                    break
                else:
                    raise ValueError(f"Checklist row not found for {checklist_row}")
            new_block = "".join(lines)
            new_content = content[:section_match.start(2)] + new_block + content[section_match.end(2):]
            file_path.write_text(new_content, encoding="utf-8")
            logger.info(
                "Updated HSLU checklist markdown file=%s section=%s item=%s status=%s",
                file_path,
                section,
                item,
                target_status,
            )
        except Exception:
            logger.error("Failed to update HSLU checklist status in markdown\n%s", traceback.format_exc())
            adieu(1)

    def parse_ai_feedback_file(self, file_path: str | Path) -> dict:
        try:
            target_path = Path(file_path).resolve()
            content = target_path.read_text(encoding="utf-8")

            note_name_match = re.search(r"(?ims)^##\s+Note Name\s*$\n(.*?)(?=^##\s+|\Z)", content)
            version_match = re.search(r"(?ims)^##\s+Version\s*$\n(.*?)(?=^##\s+|\Z)", content)
            score_match = re.search(r"(?ims)^##\s+Score\s*$\n(.*?)(?=^##\s+|\Z)", content)
            feedback_match = re.search(r"(?ims)^##\s+Feedback\s*$\n(.*?)(?=\Z)", content)

            note_name = note_name_match.group(1).strip() if note_name_match else target_path.stem.strip()
            version_line = version_match.group(1).strip() if version_match else ""
            score_line = score_match.group(1).strip() if score_match else ""
            feedback_text = feedback_match.group(1).strip() if feedback_match else ""

            version_number = 1
            creation_date = "N/A"
            if version_line:
                version_parts = [part.strip() for part in version_line.split("/", 1)]
                if version_parts and version_parts[0].isdigit():
                    version_number = int(version_parts[0])
                if len(version_parts) > 1 and version_parts[1]:
                    creation_date = version_parts[1]

            score_match_numeric = re.search(r"-?\d+(?:\.\d+)?", score_line)
            if not score_match_numeric:
                raise ValueError(f"AI feedback score missing or invalid in {target_path.name}")

            return {
                "file_name": note_name,
                "version": version_number,
                "score": float(score_match_numeric.group(0)),
                "path_to_feedback": str(target_path),
                "creation_date": creation_date,
                "feedback": feedback_text,
            }
        except Exception:
            logger.error("Failed to parse AI feedback file %s\n%s", file_path, traceback.format_exc())
            raise

    def _extract_markdown_section(self, content: str, section_name: str) -> str:
        match = re.search(rf"(?ims)^##\s+{re.escape(section_name)}\s*$\n(.*?)(?=^##\s+|\Z)", content)
        return match.group(1).strip() if match else ""

    def _parse_json_code_block(self, raw_block: str) -> dict:
        stripped = str(raw_block or "").strip()
        if not stripped:
            return {}

        code_match = re.search(r"(?is)^```json\s*(.*?)\s*```$", stripped)
        candidate = code_match.group(1).strip() if code_match else stripped
        try:
            parsed = json.loads(candidate)
            return parsed if isinstance(parsed, dict) else {}
        except json.JSONDecodeError:
            return {}

    def parse_learning_file(self, file_path: str | Path) -> dict:
        target_path = Path(file_path).resolve()
        content = target_path.read_text(encoding="utf-8")
        note_name = self._extract_markdown_section(content, "Note Name") or target_path.stem.replace(" - Learning", "").strip()
        creation_date = self._extract_markdown_section(content, "Creation") or "N/A"
        last_modified_date = self._extract_markdown_section(content, "Last Modified") or "N/A"
        questions_payload = self._parse_json_code_block(self._extract_markdown_section(content, "Questions"))
        answers_payload = self._parse_json_code_block(self._extract_markdown_section(content, "Answers"))

        parsed_questions = questions_payload.get("questions", [])
        parsed_answers = answers_payload.get("answers", [])
        if not isinstance(parsed_questions, list):
            parsed_questions = []
        if not isinstance(parsed_answers, list):
            parsed_answers = []

        return {
            "file_name": target_path.stem.strip(),
            "source_note_name": str(note_name).strip() or target_path.stem.strip(),
            "path_to_learning": str(target_path),
            "creation_date": str(creation_date).strip() or "N/A",
            "last_modified_date": str(last_modified_date).strip() or "N/A",
            "questions": parsed_questions,
            "answers": parsed_answers,
        }

    def parse_learning_files(self) -> list[dict]:
        if not self.learning_path:
            return []
        learning_dir = Path(self.learning_path)
        if not learning_dir.exists():
            return []
        rows: list[dict] = []
        for file_path in sorted(learning_dir.rglob("*.md"), key=lambda item: str(item).casefold()):
            try:
                rows.append(self.parse_learning_file(file_path))
            except Exception:
                logger.warning("Skipping malformed learning file=%s", file_path)
        return rows

    def sync_learning_to_db(self) -> list[dict]:
        rows = self.parse_learning_files()
        database = db()
        kept_paths: list[str] = []
        for row in rows:
            database.upsert_learning(row)
            kept_paths.append(str(row.get("path_to_learning", "")).strip())
        database.delete_learnings_not_in_paths(kept_paths)
        return rows

    def parse_ai_feedback_files(self) -> list[dict]:
        try:
            if not self.ai_feedback_path:
                raise ValueError("AI feedback path missing in conf.json")

            feedback_dir = Path(self.ai_feedback_path)
            if not feedback_dir.exists():
                logger.info("AI feedback directory does not exist yet: %s", feedback_dir)
                return []

            rows: list[dict] = []
            for file_path in sorted(feedback_dir.rglob("*.md"), key=lambda item: str(item).casefold()):
                rows.append(self.parse_ai_feedback_file(file_path))

            return rows
        except Exception:
            logger.error("Failed to parse AI feedback files\n%s", traceback.format_exc())
            raise

    def sync_ai_feedback_to_db(self) -> list[dict]:
        try:
            rows = self.parse_ai_feedback_files()
            db().replace_all_ai_feedback(rows)
            logger.info("AI feedback sync completed with %s entries", len(rows))
            return rows
        except Exception:
            logger.error("AI feedback sync failed\n%s", traceback.format_exc())
            raise

    def parse_todos_from_markdown(self) -> list[dict]:
        try:
            if not self.todo_file_path:
                raise Exception("Todo path missing in conf.json")
            todo_path = Path(self.todo_file_path)
            with open(todo_path, "r", encoding="utf-8") as file:
                content = file.read()
            table_pattern = re.compile(
                r"\|\s*Note\s*\|\s*Type\s*\|\s*Progress\s*\|\s*last Update\s*\|(?:\s*Priority\s*\|)?\n"
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
                if len(parts) not in (4, 5):
                    continue
                note, todo_type, progress, last_update = parts[:4]
                priority = parts[4] if len(parts) == 5 else "Medium"
                todos.append(
                    {
                        "note": self._clean_note(note),
                        "type": json.dumps(self._parse_todo_type(todo_type), ensure_ascii=False),
                        "progress": self._parse_todo_progress(progress),
                        "last_update": last_update,
                        "priority": self._parse_todo_priority(priority),
                    }
                )
            return todos
        except Exception:
            logger.error("Failed to parse todos markdown\n%s", traceback.format_exc())
            adieu(1)

    def find_note_path(self, note_name: str) -> Path:
        try:
            normalized = str(note_name or "").strip()
            if not normalized:
                raise ValueError("note_name is required.")
            sanitized = re.sub(r"[^A-Za-z0-9._ -]+", "_", normalized).strip(" ._")
            if not sanitized:
                raise ValueError("note_name contains only invalid characters.")
            if "/" in sanitized or "\\" in sanitized:
                raise ValueError("note_name must not contain path separators.")

            file_name = sanitized if sanitized.lower().endswith(".md") else f"{sanitized}.md"
            docs_root = Path(self.docs_path).resolve()
            target = (docs_root / file_name).resolve()
            if docs_root not in target.parents:
                raise ValueError("Invalid note_name path.")
            if not target.exists() or not target.is_file():
                raise FileNotFoundError(f"Note not found: {file_name}")
            return target
        except Exception:
            logger.error("Failed to resolve note path for '%s'\n%s", note_name, traceback.format_exc())
            raise

    def _parse_deadline_status(self, raw_progress: str) -> str:
        normalized = str(raw_progress or "").strip()
        return self.PROGRESS_ICON_TO_STATE.get(normalized, "Not Started")

    def _resolve_projects_root(self) -> Path:
        projects_root = Path(self.projects_root_path or "/the-knowledge/01_PROJ").resolve()
        if not projects_root.exists() or not projects_root.is_dir():
            raise FileNotFoundError(f"Projects root not found: {projects_root}")
        return projects_root

    def normalize_project_name(self, raw_value: str) -> str:
        cleaned = str(raw_value or "").strip()
        if not cleaned:
            raise ValueError("Project name is required.")
        if len(cleaned) > 120:
            raise ValueError("Project name is too long.")
        if not re.fullmatch(r"[A-Za-z0-9 _.-]+", cleaned):
            raise ValueError("Project name contains invalid characters.")
        if cleaned in {".", ".."}:
            raise ValueError("Project name is invalid.")
        return cleaned

    def resolve_project_path(self, project_name: str) -> Path:
        normalized_name = self.normalize_project_name(project_name)
        projects_root = self._resolve_projects_root()
        project_path = (projects_root / normalized_name).resolve()
        if projects_root != project_path and projects_root not in project_path.parents:
            raise ValueError("Project path traversal detected.")
        if not project_path.exists() or not project_path.is_dir():
            raise FileNotFoundError(f"Project not found: {normalized_name}")
        return project_path

    def _extract_markdown_table(self, markdown_content: str, title: str) -> list[dict[str, str]]:
        section_pattern = re.compile(rf"(?ims)^#\s+{re.escape(title)}\s*$\n(.*?)(?=^#\s+|\Z)")
        section_match = section_pattern.search(str(markdown_content or ""))
        if not section_match:
            return []

        lines = [line.strip() for line in section_match.group(1).splitlines() if line.strip().startswith("|")]
        if len(lines) < 2:
            return []

        headers = [cell.strip() for cell in lines[0].strip("|").split("|")]
        rows: list[dict[str, str]] = []
        for line in lines[2:]:
            cells = [cell.strip() for cell in line.strip("|").split("|")]
            if len(cells) < len(headers):
                cells.extend([""] * (len(headers) - len(cells)))
            rows.append({headers[index]: cells[index] for index in range(len(headers))})
        return rows

    def _normalize_markdown_table_cell(self, value: str) -> str:
        return str(value or "").replace("<br>", "\n").replace("\\|", "|").strip()

    def parse_resources(self, project_path: str | Path) -> dict:
        project_dir = Path(project_path).resolve()
        resources_file = (project_dir / "Ressourcen.md").resolve()
        if project_dir != resources_file.parent:
            raise ValueError("Invalid resources file path.")
        if not resources_file.exists() or not resources_file.is_file():
            return {
                "project_name": project_dir.name,
                "description": "",
                "resources": [],
                "links": [],
                "tag": f"#PROJECT_{project_dir.name}",
                "settings_description": "",
                "warnings": ["Ressourcen.md not found."],
            }

        content = resources_file.read_text(encoding="utf-8")
        resource_rows = self._extract_markdown_table(content, "Ressourcen")
        settings_rows = self._extract_markdown_table(content, "Settings")

        links: list[dict[str, str]] = []
        for index, row in enumerate(resource_rows, start=1):
            description = self._normalize_markdown_table_cell(row.get("Beschreibung", ""))
            link = self._normalize_markdown_table_cell(row.get("Link", ""))
            note = self._normalize_markdown_table_cell(row.get("Note", ""))
            if description or link or note:
                links.append({"id": index, "description": description, "link": link, "note": note})

        settings_map: dict[str, str] = {}
        for row in settings_rows:
            key = str(row.get("Key", "")).strip()
            value = self._normalize_markdown_table_cell(row.get("Value", ""))
            if key:
                settings_map[key.casefold()] = value

        project_description = settings_map.get("description", "")
        project_tag = settings_map.get("tag", f"#PROJECT_{project_dir.name}") or f"#PROJECT_{project_dir.name}"

        return {
            "project_name": project_dir.name,
            "description": project_description,
            "resources": links,
            "links": [item for item in links if str(item.get("link", "")).strip()],
            "notes": [item for item in links if str(item.get("note", "")).strip() and not str(item.get("link", "")).strip()],
            "tag": project_tag,
            "settings_description": project_description,
            "warnings": [],
        }

    def parse_kanban(self, project_path: str | Path) -> dict:
        project_dir = Path(project_path).resolve()
        kanban_file = (project_dir / "Kanban.md").resolve()
        if project_dir != kanban_file.parent:
            raise ValueError("Invalid Kanban file path.")
        if not kanban_file.exists() or not kanban_file.is_file():
            return {"project_name": project_dir.name, "items": [], "columns": {"Not Started": [], "In Progress": [], "Done": []}, "warnings": ["Kanban.md not found."]}

        content = kanban_file.read_text(encoding="utf-8")
        rows = self._extract_markdown_table(content, "Kanban")
        parsed_items: list[dict] = []
        for index, row in enumerate(rows, start=1):
            deliverable = self._normalize_markdown_table_cell(row.get("Deliverable", ""))
            raw_status = self._normalize_markdown_table_cell(row.get("Status", ""))
            due = self._normalize_markdown_table_cell(row.get("Due", ""))
            normalized_status = self.KANBAN_STATUS_VALUES.get(raw_status.casefold(), "Not Started")
            if not deliverable and not raw_status and not due:
                continue
            parsed_items.append(
                {
                    "id": index,
                    "deliverable": deliverable,
                    "status": raw_status if raw_status in self.KANBAN_STATUS_VALUES.values() else normalized_status,
                    "status_normalized": normalized_status,
                    "due": due,
                }
            )

        columns = {"Not Started": [], "In Progress": [], "Done": []}
        for item in parsed_items:
            columns[item["status_normalized"]].append(item)

        return {"project_name": project_dir.name, "items": parsed_items, "columns": columns, "warnings": []}

    def validate_canvas(self, data: dict) -> dict:
        warnings: list[str] = []
        if not isinstance(data, dict):
            raise ValueError("Canvas payload must be an object.")

        nodes = data.get("nodes")
        edges = data.get("edges")
        if not isinstance(nodes, list) or not isinstance(edges, list):
            raise ValueError("Canvas must include nodes and edges arrays.")

        valid_nodes: list[dict] = []
        valid_node_ids: set[str] = set()
        for node in nodes:
            if not isinstance(node, dict):
                warnings.append("Skipped non-object node.")
                continue
            required = ("id", "x", "y", "width", "height")
            if any(key not in node for key in required):
                warnings.append(f"Skipped node missing required keys: {node}.")
                continue
            try:
                normalized_node = {
                    "id": str(node.get("id")),
                    "x": float(node.get("x", 0)),
                    "y": float(node.get("y", 0)),
                    "width": float(node.get("width", 0)),
                    "height": float(node.get("height", 0)),
                    "text": str(node.get("text", "")),
                    "type": str(node.get("type", "")).strip(),
                    "raw": node,
                }
            except (TypeError, ValueError):
                warnings.append(f"Skipped malformed node: {node}.")
                continue
            valid_nodes.append(normalized_node)
            valid_node_ids.add(normalized_node["id"])

        valid_sides = {"top", "right", "bottom", "left"}
        valid_edges: list[dict] = []
        for edge in edges:
            if not isinstance(edge, dict):
                warnings.append("Skipped non-object edge.")
                continue
            from_node = str(edge.get("fromNode", "")).strip()
            to_node = str(edge.get("toNode", "")).strip()
            if not from_node or not to_node:
                warnings.append(f"Skipped edge without fromNode/toNode: {edge}.")
                continue
            if from_node not in valid_node_ids or to_node not in valid_node_ids:
                warnings.append(f"Skipped edge with unknown node references: {edge}.")
                continue
            from_side = str(edge.get("fromSide", "right")).strip().lower() or "right"
            to_side = str(edge.get("toSide", "left")).strip().lower() or "left"
            if from_side not in valid_sides or to_side not in valid_sides:
                warnings.append(f"Skipped edge with invalid side value: {edge}.")
                continue
            valid_edges.append(
                {
                    "fromNode": from_node,
                    "toNode": to_node,
                    "fromSide": from_side,
                    "toSide": to_side,
                    "label": str(edge.get("label", "")),
                    "raw": edge,
                }
            )
        return {"nodes": valid_nodes, "edges": valid_edges, "warnings": warnings}

    def compute_canvas_bounds(self, nodes: list[dict]) -> dict:
        if not nodes:
            return {"min_x": 0, "min_y": 0, "max_x": 1000, "max_y": 800, "width": 1000, "height": 800}
        min_x = min(node["x"] for node in nodes)
        min_y = min(node["y"] for node in nodes)
        max_x = max(node["x"] + node["width"] for node in nodes)
        max_y = max(node["y"] + node["height"] for node in nodes)
        return {
            "min_x": min_x,
            "min_y": min_y,
            "max_x": max_x,
            "max_y": max_y,
            "width": max(1, max_x - min_x),
            "height": max(1, max_y - min_y),
        }

    def load_canvas(self, project_path: str | Path, canvas_file_name: str | None = None) -> dict:
        project_dir = Path(project_path).resolve()
        canvas_dir = (project_dir / "Canvas").resolve()
        if project_dir != canvas_dir.parent:
            raise ValueError("Invalid canvas directory path.")

        requested_file_name = str(canvas_file_name or "").strip()
        if requested_file_name:
            canvas_path = (canvas_dir / requested_file_name).resolve()
            if canvas_dir != canvas_path.parent:
                raise ValueError("Invalid canvas file path.")
        else:
            canvas_files = sorted(
                [entry for entry in canvas_dir.glob("*.canvas") if entry.is_file()],
                key=lambda item: item.name.casefold(),
            )
            if not canvas_files:
                return {
                    "canvas_file_name": "",
                    "nodes": [],
                    "edges": [],
                    "bounds": self.compute_canvas_bounds([]),
                    "warnings": ["No canvas files found."],
                }
            canvas_path = canvas_files[0]

        if canvas_dir != canvas_path.parent:
            raise ValueError("Invalid canvas file path.")
        if not canvas_path.exists() or not canvas_path.is_file():
            return {"nodes": [], "edges": [], "bounds": self.compute_canvas_bounds([]), "warnings": [f"{canvas_path.name} not found."]}
        try:
            data = json.loads(canvas_path.read_text(encoding="utf-8") or "{}")
        except json.JSONDecodeError:
            return {"nodes": [], "edges": [], "bounds": self.compute_canvas_bounds([]), "warnings": ["Canvas JSON is malformed."]}
        validated = self.validate_canvas(data)
        return {
            "canvas_file_name": canvas_path.name,
            "nodes": validated["nodes"],
            "edges": validated["edges"],
            "bounds": self.compute_canvas_bounds(validated["nodes"]),
            "warnings": validated["warnings"],
        }

    def parse_deadlines_from_markdown(self, include_description: bool = False) -> list[dict]:
        try:
            if not self.deadlines_file_path:
                raise Exception("Deadlines path missing in conf.json")

            deadline_path = Path(self.deadlines_file_path)
            with open(deadline_path, "r", encoding="utf-8") as file:
                content = file.read()

            table_pattern = re.compile(
                r"\|\s*Name\s*\|\s*Description\s*\|\s*Date\s*\|\s*Time\s*\|\s*Status\s*\|\n"
                r"\|[^\n]+\|\n"
                r"((?:\|[^\n]+\|\n?)*)",
                re.MULTILINE,
            )
            match = table_pattern.search(content)
            if not match:
                return []

            rows = [line.strip() for line in match.group(1).splitlines() if line.strip().startswith("|")]
            deadlines: list[dict] = []
            for row in rows:
                parts = [cell.strip() for cell in row.strip("|").split("|")]
                if len(parts) != 5:
                    continue

                name, description, date, time, status = parts
                deadline_row = {
                    "name": name,
                    "date": date,
                    "time": time,
                    "status": self._parse_deadline_status(status),
                }
                if include_description:
                    deadline_row["description"] = description
                deadlines.append(deadline_row)

            return deadlines
        except Exception:
            logger.error("Failed to parse deadlines markdown\n%s", traceback.format_exc())
            adieu(1)
