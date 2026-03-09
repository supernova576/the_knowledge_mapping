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
        "![[not needed.png]]": "Not Needed",
    }
    SW_PROGRESS_STATE_TO_RAW = {
        "Done": "![[done.png]]",
        "In Progress": "![[in progress.png]]",
        "Not Needed": "![[not needed.png]]",
        "Not Started": "![[not started.png]]",
        "": "",
    }
    def __init__(self) -> None:
        try:
            path = Path(__file__).resolve().parent.parent / "conf.json"
            with open(path, "r", encoding="utf-8") as f:
                j: dict = json.loads(f.read())
                self.docs_path: str = j.get("docs", {}).get("full_path_to_docs", False)
                self.todo_file_path: str = j.get("todo", {}).get("full_path_to_todo_file", False)
                self.hslu_base_path: str = j.get("hslu", {}).get("full_path_to_hslu", "/the-knowledge/00_HSLU")
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
                        deadlines.replace("\n", " ").strip() or "-",
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
                                "deadlines": deadlines if deadlines else "-",
                            }
                        )
            logger.info("Parsed %s HSLU SW overview rows", len(all_rows))
            return all_rows
        except Exception:
            logger.error("Failed to parse HSLU SW overview\n%s", traceback.format_exc())
            adieu(1)
    def sync_hslu_sw_overview_to_db(self) -> list[dict]:
        try:
            rows = self.parse_hslu_sw_overview()
            db().replace_all_hslu_sw_overview(rows)
            logger.info("HSLU SW overview sync completed with %s rows", len(rows))
            return rows
        except Exception:
            logger.error("HSLU SW overview sync failed\n%s", traceback.format_exc())
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
    def sync_hslu_semester_checklist_to_db(self) -> list[dict]:
        try:
            rows = self.parse_hslu_semester_checklist()
            db().replace_all_hslu_sw_checklist(rows)
            logger.info("HSLU semester checklist sync completed with %s rows", len(rows))
            return rows
        except Exception:
            logger.error("HSLU semester checklist sync failed\n%s", traceback.format_exc())
            adieu(1)
    def update_hslu_semester_checklist_status(self, row_id: int, target_status: str) -> None:
        try:
            target = db().get_hslu_sw_checklist_by_id(row_id)
            if not target:
                raise ValueError(f"Checklist row id={row_id} not found")
            file_path = Path(target.get("file_path", ""))
            if not file_path.exists():
                raise FileNotFoundError(f"Checklist file not found: {file_path}")
            content = file_path.read_text(encoding="utf-8")
            raw_icon = self._sw_progress_state_to_raw(target_status)
            section = target.get("section", "")
            item = target.get("checklist_item", "")
            sw = (target.get("sw", "") or "").strip()
            checklist_row = target.get("checklist_row", "")
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
            if any(h.casefold() == "sw" for h in header_cells):
                sw_idx = next(i for i,h in enumerate(header_cells) if h.casefold()=="sw")
                item_idx = next((i for i,h in enumerate(header_cells) if h == item), -1)
                if item_idx < 0:
                    raise ValueError(f"Checklist item column not found: {item}")
                for i in range(data_start, len(lines)):
                    if not lines[i].strip().startswith("|"):
                        continue
                    cells = self._split_markdown_table_row(lines[i])
                    if len(cells) < len(header_cells):
                        cells.extend([""] * (len(header_cells) - len(cells)))
                    row_sw_match = re.search(r"\d{1,2}", cells[sw_idx] if sw_idx < len(cells) else "")
                    row_sw = row_sw_match.group(0).zfill(2) if row_sw_match else ""
                    if row_sw != sw:
                        continue
                    cells[item_idx] = raw_icon
                    line_ending = "\n" if lines[i].endswith("\n") else ""
                    lines[i] = "| " + " | ".join(cells) + f" |{line_ending}"
                    break
                else:
                    raise ValueError(f"SW row not found for {sw}")
            else:
                for i in range(data_start, len(lines)):
                    if not lines[i].strip().startswith("|"):
                        continue
                    cells = self._split_markdown_table_row(lines[i])
                    if len(cells) < 2:
                        continue
                    if cells[0] != checklist_row:
                        continue
                    cells[1] = raw_icon
                    line_ending = "\n" if lines[i].endswith("\n") else ""
                    lines[i] = "| " + " | ".join(cells) + f" |{line_ending}"
                    break
                else:
                    raise ValueError(f"Checklist row not found for {checklist_row}")
            new_block = "".join(lines)
            new_content = content[:section_match.start(2)] + new_block + content[section_match.end(2):]
            file_path.write_text(new_content, encoding="utf-8")
            logger.info("Updated HSLU checklist markdown id=%s status=%s", row_id, target_status)
        except Exception:
            logger.error("Failed to update HSLU checklist status in markdown\n%s", traceback.format_exc())
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
