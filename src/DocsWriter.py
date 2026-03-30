import json
import re
import traceback
from datetime import datetime
from pathlib import Path
from sys import exit as adieu

from .logger import get_logger


logger = get_logger(__name__)


class DocsWriter:
    PROGRESS_TO_ICON = {
        "Not Started": "![[not started.png]]",
        "In Progress": "![[in progress.png]]",
        "Done": "![[done.png]]",
    }
    TODO_PRIORITY_VALUES = {"low": "Low", "medium": "Medium", "high": "High"}
    TODO_PRIORITY_ORDER = {"High": 0, "Medium": 1, "Low": 2}

    def __init__(self, todo_file_path: str = "", deadlines_file_path: str = "") -> None:
        self.todo_path = Path(todo_file_path) if todo_file_path else None
        self.deadlines_path = Path(deadlines_file_path) if deadlines_file_path else None

    def _ensure_file_exists(self) -> None:
        if self.todo_path is None:
            raise FileNotFoundError("Todo file path not configured.")
        if not self.todo_path.exists():
            raise FileNotFoundError(f"Todo file not found: {self.todo_path}")

    def _today_dd_mm(self) -> str:
        return datetime.now().strftime("%d.%m")

    def _extract_table_bounds(self, lines: list[str]) -> tuple[int, int]:
        start = -1
        end = -1
        for index, line in enumerate(lines):
            if line.strip().startswith("|") and "Note" in line and "Progress" in line and "last Update" in line:
                start = index
                break

        if start == -1:
            raise ValueError("Could not find todo markdown table header")

        end = start + 1
        while end < len(lines) and lines[end].strip().startswith("|"):
            end += 1

        return start, end

    def _serialize_table(self, todos: list[dict]) -> list[str]:
        header = "| Note                             | Type       | Progress             | last Update | Priority |\n"
        separator = "| -------------------------------- | ---------- | -------------------- | ----------- | -------- |\n"
        rows = []

        sorted_todos = sorted(
            todos,
            key=lambda todo: (
                self.TODO_PRIORITY_ORDER.get(
                    self.TODO_PRIORITY_VALUES.get(str(todo.get("priority", "Medium")).strip().casefold(), "Medium"),
                    len(self.TODO_PRIORITY_ORDER),
                ),
                str(todo.get("note", "")).casefold(),
            ),
        )

        for todo in sorted_todos:
            note = todo.get("note", "").strip()
            todo_type = todo.get("type", [])
            progress = todo.get("progress", "Not Started")
            last_update = todo.get("last_update", self._today_dd_mm())
            priority = self.TODO_PRIORITY_VALUES.get(str(todo.get("priority", "Medium")).strip().casefold(), "Medium")

            if isinstance(todo_type, str):
                try:
                    todo_type = json.loads(todo_type)
                except json.JSONDecodeError:
                    todo_type = [todo_type]

            if not isinstance(todo_type, list):
                todo_type = [str(todo_type)]

            type_text = "/".join([str(item).strip() for item in todo_type if str(item).strip()]) or "N/A"
            note_clean = re.sub(r"\s*\(.*?\)", "", note).strip()
            progress_icon = self.PROGRESS_TO_ICON.get(progress, self.PROGRESS_TO_ICON["Not Started"])
            rows.append(f"| {note_clean} | {type_text} | {progress_icon} | {last_update} | {priority} |\n")

        return [header, separator, *rows]

    def write_todos_table(self, todos: list[dict]) -> None:
        try:
            self._ensure_file_exists()
            with open(self.todo_path, "r", encoding="utf-8") as file:
                lines = file.readlines()

            start, end = self._extract_table_bounds(lines)
            new_table = self._serialize_table(todos)
            updated_lines = lines[:start] + new_table + lines[end:]

            with open(self.todo_path, "w", encoding="utf-8") as file:
                file.writelines(updated_lines)
            logger.info("Wrote %s todo entries to %s", len(todos), self.todo_path)
        except Exception:
            logger.error("Failed to write todo markdown file\n%s", traceback.format_exc())
            adieu(1)

    def _ensure_deadlines_file_exists(self) -> None:
        if not self.deadlines_path:
            raise FileNotFoundError("Deadlines file path not configured.")
        if not self.deadlines_path.exists():
            raise FileNotFoundError(f"Deadlines file not found: {self.deadlines_path}")

    def _extract_deadlines_table_bounds(self, lines: list[str]) -> tuple[int, int]:
        start = -1
        end = -1
        for index, line in enumerate(lines):
            if (
                line.strip().startswith("|")
                and "Name" in line
                and "Description" in line
                and "Date" in line
                and "Time" in line
                and "Status" in line
            ):
                start = index
                break

        if start == -1:
            raise ValueError("Could not find deadline markdown table header")

        end = start + 1
        while end < len(lines) and lines[end].strip().startswith("|"):
            end += 1

        return start, end

    def _serialize_deadlines_table(self, deadlines: list[dict]) -> list[str]:
        header = "| Name | Description | Date | Time | Status |\n"
        separator = "| ---- | ----------- | ---- | ---- | ------ |\n"
        rows = []

        for deadline in deadlines:
            name = str(deadline.get("name", "")).strip()
            description = str(deadline.get("description", "")).strip()
            date = str(deadline.get("date", "-")).strip() or "-"
            time = str(deadline.get("time", "-")).strip() or "-"
            status = str(deadline.get("status", "Not Started")).strip()
            progress_icon = self.PROGRESS_TO_ICON.get(status, self.PROGRESS_TO_ICON["Not Started"])

            rows.append(f"| {name} | {description} | {date} | {time} | {progress_icon} |\n")

        return [header, separator, *rows]

    def write_deadlines_table(self, deadlines: list[dict]) -> None:
        try:
            self._ensure_deadlines_file_exists()
            with open(self.deadlines_path, "r", encoding="utf-8") as file:
                lines = file.readlines()

            start, end = self._extract_deadlines_table_bounds(lines)
            new_table = self._serialize_deadlines_table(deadlines)
            updated_lines = lines[:start] + new_table + lines[end:]

            with open(self.deadlines_path, "w", encoding="utf-8") as file:
                file.writelines(updated_lines)
            logger.info("Wrote %s deadline entries to %s", len(deadlines), self.deadlines_path)
        except Exception:
            logger.error("Failed to write deadline markdown file\n%s", traceback.format_exc())
            adieu(1)

    def create_note_from_template(self, target_path: Path, template_content: str) -> None:
        try:
            target_path.write_text(template_content, encoding="utf-8")
        except Exception:
            logger.error("Failed to create markdown note %s\n%s", target_path, traceback.format_exc())
            adieu(1)

    def render_ai_feedback_template(
        self,
        template_content: str,
        note_name: str,
        version: int,
        creation_date: str,
        score: str,
        feedback: str,
    ) -> str:
        try:
            rendered = str(template_content)
            replacements = {
                "{{ name_of_controlled_note }}": str(note_name).strip(),
                "{{ version }}": str(version),
                "{{ creation_date }}": str(creation_date).strip(),
                "{{ score }}": str(score).strip(),
                "{{ feedback }}": str(feedback).strip(),
            }

            for placeholder, value in replacements.items():
                rendered = rendered.replace(placeholder, value)

            return rendered
        except Exception:
            logger.error("Failed to render AI feedback template\n%s", traceback.format_exc())
            raise

    def write_ai_feedback_file(self, output_dir: Path, note_name: str, version: int, rendered_content: str) -> Path:
        try:
            safe_name = re.sub(r"[^A-Za-z0-9._ -]+", "_", str(note_name).strip()).strip(" ._")
            if not safe_name:
                raise ValueError("Invalid note name for AI feedback output file.")

            output_dir.mkdir(parents=True, exist_ok=True)
            target_path = output_dir / f"{safe_name} - AI Feedback v{int(version)}.md"
            if target_path.exists():
                raise FileExistsError(f"AI feedback file already exists: {target_path}")

            target_path.write_text(rendered_content.rstrip() + "\n", encoding="utf-8")
            return target_path
        except Exception:
            logger.error("Failed to write AI feedback markdown file\n%s", traceback.format_exc())
            raise

    def prepend_template_to_existing_note(
        self,
        target_path: Path,
        template_content: str,
        reason: str,
        create_history: bool,
    ) -> tuple[bool, list[str]]:
        try:
            current_content = target_path.read_text(encoding="utf-8")
            updated_content, history_present = self._insert_history_entry(
                current_content=current_content,
                reason=reason,
                should_create_history=create_history,
            )
            if updated_content is None and not history_present:
                return False, ["#### Page History"]

            template_prefix = self._strip_resources_section(template_content).rstrip()
            combined_content = updated_content.lstrip()
            if template_prefix:
                combined_content = f"{template_prefix}\n\n{combined_content}"
            target_path.write_text(combined_content, encoding="utf-8")
            return True, []
        except Exception:
            logger.error("Failed to prepend template to markdown note %s\n%s", target_path, traceback.format_exc())
            adieu(1)

    def update_doc_resources(
        self,
        doc_path: Path,
        tags_to_add: list[str],
        tags_to_remove: list[str],
        links_map: dict[str, str],
        video_links_map: dict[str, str],
        create_missing_sections: bool,
    ) -> tuple[bool, list[str]]:
        try:
            lines = doc_path.read_text(encoding="utf-8").splitlines()
            missing_sections: list[str] = []

            target_sections = ["#### Erklärvideo", "#### Externe Referenzen", "#### Page Tags"]
            for section in target_sections:
                if self._find_section_index(lines, section) == -1:
                    missing_sections.append(section)

            if missing_sections and not create_missing_sections:
                return False, missing_sections

            if missing_sections:
                lines = self._create_missing_sections(lines, missing_sections)

            lines = self._update_link_section(lines, "#### Erklärvideo", video_links_map)
            lines = self._update_link_section(lines, "#### Externe Referenzen", links_map)
            lines = self._update_tags_section(lines, tags_to_add, tags_to_remove)

            doc_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
            return True, []
        except Exception:
            logger.error("Failed to update markdown resource sections in %s\n%s", doc_path, traceback.format_exc())
            adieu(1)

    def _insert_history_entry(self, current_content: str, reason: str, should_create_history: bool) -> tuple[str | None, bool]:
        history_header = "#### Page History"
        tags_header = "#### Page Tags"
        history_entry = f"> Überarbeitet am: {datetime.now().strftime('%d.%m.%Y')} => {reason.strip()}"

        lines = current_content.splitlines()
        history_index = next((index for index, line in enumerate(lines) if line.strip() == history_header), -1)

        if history_index == -1:
            if not should_create_history:
                return None, False

            tags_index = next((index for index, line in enumerate(lines) if line.strip() == tags_header), -1)
            if tags_index == -1:
                raise ValueError("Could not find '#### Page Tags' chapter in markdown file.")

            lines.insert(tags_index, "")
            lines.insert(tags_index + 1, history_header)
            history_index = tags_index + 1

        insert_index = history_index + 1
        lines.insert(insert_index, history_entry)
        updated_content = "\n".join(lines)
        if current_content.endswith("\n"):
            updated_content += "\n"

        return updated_content, True

    def _find_section_index(self, lines: list[str], section_header: str) -> int:
        return next((i for i, line in enumerate(lines) if line.strip() == section_header), -1)

    def _strip_resources_section(self, content: str) -> str:
        lines = content.splitlines()
        resources_idx = self._find_section_index(lines, "## Zusätzliche Ressourcen")
        if resources_idx == -1:
            return content

        trimmed = lines[:resources_idx]
        result = "\n".join(trimmed).rstrip()
        if content.endswith("\n") and result:
            result += "\n"
        return result

    def _section_end_index(self, lines: list[str], section_start: int) -> int:
        for index in range(section_start + 1, len(lines)):
            if lines[index].strip().startswith("#### "):
                return index
        return len(lines)

    def _create_missing_sections(self, lines: list[str], missing_sections: list[str]) -> list[str]:
        page_history_idx = self._find_section_index(lines, "#### Page History")
        page_tags_idx = self._find_section_index(lines, "#### Page Tags")
        external_refs_idx = self._find_section_index(lines, "#### Externe Referenzen")
        resources_idx = self._find_section_index(lines, "## Zusätzliche Ressourcen")

        if "#### Erklärvideo" in missing_sections:
            insert_at = external_refs_idx if external_refs_idx != -1 else (page_history_idx if page_history_idx != -1 else (page_tags_idx if page_tags_idx != -1 else len(lines)))
            lines[insert_at:insert_at] = ["#### Erklärvideo", ""]

        if "#### Externe Referenzen" in missing_sections:
            page_history_idx = self._find_section_index(lines, "#### Page History")
            page_tags_idx = self._find_section_index(lines, "#### Page Tags")
            insert_at = page_history_idx if page_history_idx != -1 else (page_tags_idx if page_tags_idx != -1 else len(lines))
            lines[insert_at:insert_at] = ["#### Externe Referenzen", ""]

        if "#### Page Tags" in missing_sections:
            insert_at = len(lines)
            if resources_idx != -1:
                insert_at = self._section_end_index(lines, resources_idx)
            lines[insert_at:insert_at] = ["#### Page Tags", ""]

        return lines

    def _update_link_section(self, lines: list[str], section_header: str, link_map: dict[str, str]) -> list[str]:
        section_idx = self._find_section_index(lines, section_header)
        if section_idx == -1:
            return lines

        section_end = self._section_end_index(lines, section_idx)
        replacement = [f"[{description}]({link})" for link, description in link_map.items()]
        if not replacement:
            replacement = [""]

        lines[section_idx + 1:section_end] = replacement
        return lines

    def _update_tags_section(self, lines: list[str], tags_to_add: list[str], tags_to_remove: list[str]) -> list[str]:
        section_idx = self._find_section_index(lines, "#### Page Tags")
        if section_idx == -1:
            return lines

        section_end = self._section_end_index(lines, section_idx)
        block = lines[section_idx + 1:section_end]
        editable_block = block
        preserved_suffix: list[str] = []

        for index, line in enumerate(block):
            if line.strip().startswith(">"):
                editable_block = block[:index]
                preserved_suffix = block[index:]
                break

        existing_tags = re.findall(r"(?<!\w)#[-\w]+", "\n".join(editable_block))
        deduped_existing = list(dict.fromkeys(existing_tags))

        kept_tags = [tag for tag in deduped_existing if tag not in tags_to_remove]
        for tag in tags_to_add:
            if tag not in kept_tags:
                kept_tags.append(tag)

        replacement = [" ".join(kept_tags).strip()] if kept_tags else [""]
        replacement.extend(preserved_suffix)
        lines[section_idx + 1:section_end] = replacement
        return lines
