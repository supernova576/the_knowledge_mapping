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

    def __init__(self, todo_file_path: str) -> None:
        self.todo_path = Path(todo_file_path)

    def _ensure_file_exists(self) -> None:
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
        header = "| Note                             | Type       | Progress             | last Update |\n"
        separator = "| -------------------------------- | ---------- | -------------------- | ----------- |\n"
        rows = []

        for todo in todos:
            note = todo.get("note", "").strip()
            todo_type = todo.get("type", [])
            progress = todo.get("progress", "Not Started")
            last_update = todo.get("last_update", self._today_dd_mm())

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
            rows.append(f"| {note_clean} | {type_text} | {progress_icon} | {last_update} |\n")

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
