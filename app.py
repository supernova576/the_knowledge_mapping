import html
import json
import os
import re
import stat
import traceback
from urllib.parse import urlencode, urlparse
from datetime import datetime, timedelta
from pathlib import Path

from flask import Flask, flash, jsonify, redirect, render_template, request, send_file, session, url_for
from markupsafe import Markup
from werkzeug.exceptions import HTTPException

from src.DatabaseConnector import db
from src.DocsAIFeedback import DocsAIFeedback
from src.DocsParser import DocsParser
from src.DocsVersionHandler import DocsVersionHandler
from src.DocsWriter import DocsWriter
from src.DocsExporter import DocsExporter
from src.logger import get_logger
from src.timezone_utils import now_in_zurich, now_in_zurich_str


app = Flask(__name__)
app.secret_key = "knowledge-mapping-secret"

logger = get_logger(__name__)


SW_STATUS_OPTIONS = ["", "Not Started", "In Progress", "Done", "Not Needed"]
TODO_PRIORITY_OPTIONS = ["Low", "Medium", "High"]
TODO_PRIORITY_ORDER = {"High": 0, "Medium": 1, "Low": 2}
DEADLINE_STATUS_OPTIONS = ["Not Started", "In Progress", "Done"]
INDEX_PROGRESS_WEIGHTS = {
    "under_construction": 0.2,
    "incompliant": 0.2,
    "todos": 0.2,
    "deadlines": 0.2,
    "ai_gap": 0.2,
}
UNFINISHED_SW_STATUSES = {"", "Not Started", "In Progress"}
FINISHED_SW_STATUSES = {"Done", "Not Needed"}


def _normalize_sw_status(value: str | None) -> str:
    normalized = str(value or "").strip()
    return normalized if normalized in SW_STATUS_OPTIONS else ""


def _entry_indicator_for_sw_status(*statuses: str | None) -> str:
    normalized_statuses = [_normalize_sw_status(status) for status in statuses]
    if any(status in UNFINISHED_SW_STATUSES for status in normalized_statuses):
        return "⚠️"
    if any(status in FINISHED_SW_STATUSES for status in normalized_statuses):
        return "✅"
    return "⚠️"


def _normalize_todo_priority(value: str | None) -> str:
    normalized = str(value or "").strip().casefold()
    if normalized not in {"low", "medium", "high"}:
        return "Medium"
    return normalized.capitalize()


def _sort_todos_by_priority(todos: list[dict]) -> list[dict]:
    return sorted(
        todos,
        key=lambda todo: (
            TODO_PRIORITY_ORDER.get(_normalize_todo_priority(todo.get("priority")), len(TODO_PRIORITY_ORDER)),
            str(todo.get("note", "")).casefold(),
        ),
    )


def _parse_sync_timestamp(sync_time: str | None) -> datetime | None:
    sync_label = str(sync_time or "").strip()
    if not sync_label or sync_label.lower() == "never":
        return None

    try:
        return datetime.strptime(sync_label, "%Y-%m-%d %H:%M:%S").replace(tzinfo=now_in_zurich().tzinfo)
    except ValueError:
        logger.warning("Unexpected sync timestamp format: %s", sync_label)
        return None


def _format_sync_time_relative_to_now(sync_time: str | None) -> str:
    sync_label = str(sync_time or "").strip()
    synced_at = _parse_sync_timestamp(sync_label)
    if synced_at is None:
        return sync_label or "Never"

    elapsed_seconds = int((now_in_zurich() - synced_at).total_seconds())
    if elapsed_seconds < 0:
        elapsed_seconds = 0

    days, remainder = divmod(elapsed_seconds, 24 * 60 * 60)
    hours, remainder = divmod(remainder, 60 * 60)
    minutes, _ = divmod(remainder, 60)

    return f"{sync_label} ({days} days, {hours} hours, {minutes} minutes ago)"


def _sync_banner_state(sync_time: str | None) -> str:
    sync_label = str(sync_time or "").strip()
    if not sync_label or sync_label.lower() == "never":
        return "danger"

    synced_at = _parse_sync_timestamp(sync_label)
    if synced_at is None:
        return "danger"

    elapsed = now_in_zurich() - synced_at
    if elapsed.total_seconds() < 0:
        return "secondary"
    if elapsed.days < 1:
        return "secondary"
    if elapsed.days < 7:
        return "warning"
    return "danger"


def _safe_redirect_target(target: str | None, fallback_endpoint: str) -> str:
    redirect_target = str(target or "").strip()
    if redirect_target.startswith("/"):
        return redirect_target
    return url_for(fallback_endpoint)


def _render_hslu_inline_markdown(value: str) -> str:
    text = str(value or "")

    def _render_fragment(fragment: str) -> str:
        pattern = re.compile(r"(\*\*(.+?)\*\*|==(.+?)==|<br\s*/?>)", flags=re.IGNORECASE)
        parts: list[str] = []
        last_end = 0
        for match in pattern.finditer(fragment):
            parts.append(html.escape(fragment[last_end:match.start()]))
            if re.fullmatch(r"<br\s*/?>", match.group(0), flags=re.IGNORECASE):
                parts.append("<br>")
                last_end = match.end()
                continue

            bold_content = match.group(2)
            mark_content = match.group(3)
            if bold_content is not None:
                parts.append(f"<strong>{_render_fragment(bold_content)}</strong>")
            else:
                parts.append(
                    f'<b><span style="color: white; background-color: #D4B039;">{_render_fragment(mark_content or "")}</span></b>'
                )
            last_end = match.end()

        parts.append(html.escape(fragment[last_end:]))
        return "".join(parts)

    return _render_fragment(text)


def _render_ai_feedback_markdown(value: str) -> str:
    raw_markdown = str(value or "").strip()
    if not raw_markdown:
        return ""

    def _escape_inline(text: str) -> str:
        return html.escape(str(text or ""))

    def _safe_href(raw_href: str) -> str:
        candidate = str(raw_href or "").strip()
        if re.match(r"^(https?://|mailto:)", candidate, flags=re.IGNORECASE):
            return html.escape(candidate, quote=True)
        return ""

    def _render_inline(text: str) -> str:
        rendered = _escape_inline(text)
        rendered = re.sub(r"`([^`]+)`", lambda match: f"<code>{match.group(1)}</code>", rendered)
        rendered = re.sub(r"\*\*([^*]+)\*\*", lambda match: f"<strong>{match.group(1)}</strong>", rendered)
        rendered = re.sub(r"(?<!\*)\*([^*]+)\*(?!\*)", lambda match: f"<em>{match.group(1)}</em>", rendered)
        rendered = re.sub(
            r"\[([^\]]+)\]\(([^)]+)\)",
            lambda match: (
                f'<a href="{safe_href}" target="_blank" rel="noopener noreferrer nofollow">{match.group(1)}</a>'
                if (safe_href := _safe_href(match.group(2)))
                else match.group(1)
            ),
            rendered,
        )
        return rendered

    def _render_table(block_lines: list[str]) -> str | None:
        if len(block_lines) < 2:
            return None

        def _split_row(row: str) -> list[str]:
            stripped = row.strip()
            if not (stripped.startswith("|") and stripped.endswith("|")):
                return []
            return [cell.strip() for cell in stripped.strip("|").split("|")]

        header = _split_row(block_lines[0])
        separator = _split_row(block_lines[1])
        if not header or len(header) != len(separator):
            return None
        if not all(re.fullmatch(r":?-{3,}:?", cell.replace(" ", "")) for cell in separator):
            return None

        body_rows: list[list[str]] = []
        for line in block_lines[2:]:
            cells = _split_row(line)
            if len(cells) != len(header):
                return None
            body_rows.append(cells)

        head_html = "".join(f"<th>{_render_inline(cell)}</th>" for cell in header)
        body_html = "".join(
            "<tr>" + "".join(f"<td>{_render_inline(cell)}</td>" for cell in row) + "</tr>"
            for row in body_rows
        )
        return f"<table><thead><tr>{head_html}</tr></thead><tbody>{body_html}</tbody></table>"

    lines = raw_markdown.splitlines()
    blocks: list[str] = []
    paragraph_buffer: list[str] = []
    list_stack: list[str] = []
    table_buffer: list[str] = []
    in_code_block = False
    code_lines: list[str] = []

    def _flush_paragraph() -> None:
        nonlocal paragraph_buffer
        if paragraph_buffer:
            blocks.append(f"<p>{'<br>'.join(_render_inline(line) for line in paragraph_buffer)}</p>")
            paragraph_buffer = []

    def _flush_lists() -> None:
        nonlocal list_stack
        while list_stack:
            blocks.append(f"</{list_stack.pop()}>")

    def _flush_table() -> None:
        nonlocal table_buffer
        if not table_buffer:
            return
        table_html = _render_table(table_buffer)
        if table_html is None:
            for row in table_buffer:
                paragraph_buffer.append(row)
            _flush_paragraph()
        else:
            blocks.append(table_html)
        table_buffer = []

    for line in lines:
        stripped = line.strip()

        if stripped.startswith("```"):
            _flush_paragraph()
            _flush_table()
            _flush_lists()
            if in_code_block:
                blocks.append(f"<pre><code>{_escape_inline(chr(10).join(code_lines))}</code></pre>")
                code_lines = []
                in_code_block = False
            else:
                in_code_block = True
            continue

        if in_code_block:
            code_lines.append(line)
            continue

        if stripped.startswith("|") and stripped.endswith("|"):
            _flush_paragraph()
            _flush_lists()
            table_buffer.append(line)
            continue
        _flush_table()

        if not stripped:
            _flush_paragraph()
            _flush_lists()
            continue

        heading_match = re.match(r"^(#{1,6})\s+(.*)$", stripped)
        if heading_match:
            _flush_paragraph()
            _flush_lists()
            level = len(heading_match.group(1))
            blocks.append(f"<h{level}>{_render_inline(heading_match.group(2))}</h{level}>")
            continue

        if re.fullmatch(r"---+|\*\*\*+", stripped):
            _flush_paragraph()
            _flush_lists()
            blocks.append("<hr>")
            continue

        blockquote_match = re.match(r"^>\s?(.*)$", stripped)
        if blockquote_match:
            _flush_paragraph()
            _flush_lists()
            blocks.append(f"<blockquote><p>{_render_inline(blockquote_match.group(1))}</p></blockquote>")
            continue

        ordered_match = re.match(r"^\d+\.\s+(.*)$", stripped)
        unordered_match = re.match(r"^[-*]\s+(.*)$", stripped)
        if ordered_match or unordered_match:
            _flush_paragraph()
            list_tag = "ol" if ordered_match else "ul"
            if not list_stack or list_stack[-1] != list_tag:
                _flush_lists()
                blocks.append(f"<{list_tag}>")
                list_stack.append(list_tag)
            item_content = ordered_match.group(1) if ordered_match else unordered_match.group(1)
            blocks.append(f"<li>{_render_inline(item_content)}</li>")
            continue

        paragraph_buffer.append(line)

    if in_code_block:
        blocks.append(f"<pre><code>{_escape_inline(chr(10).join(code_lines))}</code></pre>")
    _flush_paragraph()
    _flush_table()
    _flush_lists()
    return "\n".join(blocks)


def _normalize_value(value):
    if isinstance(value, str):
        stripped = value.strip()
        if stripped.startswith("[") and stripped.endswith("]"):
            try:
                parsed = json.loads(stripped)
                if isinstance(parsed, list):
                    return parsed
            except json.JSONDecodeError:
                pass
    return value


def _to_display_list(value):
    normalized = _normalize_value(value)
    if isinstance(normalized, list):
        return normalized
    if normalized in (None, "", "N/A"):
        return []
    return [str(normalized)]


def _is_valid_http_url(value: str) -> bool:
    parsed = urlparse(str(value or "").strip())
    return parsed.scheme in {"http", "https"} and bool(parsed.netloc)


def _parse_link_map(value) -> dict[str, str]:
    if isinstance(value, dict):
        parsed_dict = value
    elif isinstance(value, list):
        parsed_dict = {str(item).strip(): str(item).strip() for item in value if str(item).strip()}
    else:
        raw = str(value or "").strip()
        if not raw or raw == "N/A":
            return {}
        try:
            loaded = json.loads(raw)
        except json.JSONDecodeError:
            loaded = None

        if isinstance(loaded, dict):
            parsed_dict = loaded
        elif isinstance(loaded, list):
            parsed_dict = {str(item).strip(): str(item).strip() for item in loaded if str(item).strip()}
        elif _is_valid_http_url(raw):
            parsed_dict = {raw: raw}
        else:
            return {}

    normalized: dict[str, str] = {}
    for raw_link, raw_description in parsed_dict.items():
        link = str(raw_link or "").strip()
        description = re.sub(r"\s+", " ", str(raw_description or "").strip())
        if not link or not _is_valid_http_url(link):
            continue
        normalized[link] = description or link

    return normalized


def _link_map_to_items(value) -> list[dict[str, str]]:
    link_map = _parse_link_map(value)
    return [{"link": link, "description": description} for link, description in link_map.items()]




def _compliance_tag_class(doc: dict) -> str:
    if str(doc.get("is_under_construction", "false")).lower() == "true":
        return "text-bg-info"

    if doc.get("is_compliant") == "true":
        return "compliance-tag-compliant"

    return "compliance-tag-not-compliant"

def _load_docs(database: db, parser: DocsParser, view: str, query: str) -> dict:

    if view == "name" and query:
        return database.get_docs_by_name(query, exact_match=False)

    if view == "description" and query:
        matching_titles = parser.get_doc_titles_by_description_query(query)
        if not matching_titles:
            return {}

        all_docs = database.get_all_docs()
        return {
            doc_id: doc_data
            for doc_id, doc_data in all_docs.items()
            if str(doc_data.get("title", "")).strip() in matching_titles
        }

    if view == "tag" and query:
        return database.get_docs_by_tag(query)

    if view == "incompliant":
        return database.get_non_compliant_docs()

    if view == "compliant":
        return database.get_compliant_docs()

    if view == "under_construction":
        return database.get_under_construction_docs()

    return database.get_all_docs()


def _load_conf() -> dict:
    conf_path = Path(__file__).resolve().parent / "conf.json"
    with open(conf_path, "r", encoding="utf-8") as conf_file:
        return json.loads(conf_file.read())


def _today_dd_mm() -> str:
    return datetime.now().strftime("%d.%m")


def _today_dd_mm_yyyy() -> str:
    return datetime.now().strftime("%d.%m.%Y")


def _normalize_md_filename(file_name: str) -> str:
    sanitized = str(file_name or "").strip().replace("\\", "/")
    if not sanitized:
        return ""

    if "/" in sanitized:
        return ""

    if not sanitized.lower().endswith(".md"):
        sanitized = f"{sanitized}.md"

    return sanitized




def _normalize_tag_value(tag: str) -> str:
    cleaned = str(tag or "").strip()
    if not cleaned:
        return ""
    return cleaned if cleaned.startswith("#") else f"#{cleaned}"


def _parse_multiline_values(value: str) -> list[str]:
    values: list[str] = []
    for line in str(value or "").splitlines():
        item = line.strip()
        if item:
            values.append(item)
    return values


def _parse_multiline_tags(value: str) -> list[str]:
    parsed: list[str] = []
    for line in str(value or "").splitlines():
        normalized = _normalize_tag_value(line)
        if normalized:
            parsed.append(normalized)
    return parsed


def _parse_json_array(value):
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]

    raw = str(value or "").strip()
    if not raw or raw == "N/A":
        return []

    if raw.startswith("[") and raw.endswith("]"):
        try:
            parsed = json.loads(raw)
            if isinstance(parsed, list):
                return [str(item).strip() for item in parsed if str(item).strip()]
        except json.JSONDecodeError:
            return []

    return [raw]


def _normalize_export_title(raw_title: str) -> str:
    cleaned = re.sub(r"\s+", " ", str(raw_title or "").strip())
    return cleaned[:150]


def _normalize_export_description(raw_description: str) -> str:
    cleaned = re.sub(r"[\x00-\x1f\x7f]", "", str(raw_description or "")).strip()
    return cleaned[:500]

def _load_template_options() -> dict[str, Path]:
    template_dir = Path("/the-knowledge/03_TEMPLATES")
    if not template_dir.exists():
        return {}

    templates: dict[str, Path] = {}
    for template_path in template_dir.glob("0 -*.md"):
        stem = template_path.stem.strip()
        if stem == "0 - Vorlage Note (Neu)":
            templates["new"] = template_path
        elif stem == "0 - Vorlage Note (Ergänzung)":
            templates["update"] = template_path

    return templates


def _render_doc_template(template_content: str) -> str:
    today = _today_dd_mm_yyyy()
    rendered = re.sub(r"\{\{\s*date\s*\}\}", today, template_content, flags=re.IGNORECASE)
    rendered = re.sub(
        r"(?im)^(>[^\S\r\n]*Erstellt[^\S\r\n]*:[^\S\r\n]*)\{\{[^\S\r\n]*date[^\S\r\n]*\}\}[^\S\r\n]*$",
        lambda match: f"{match.group(1).rstrip()} {today}",
        rendered,
    )
    rendered = re.sub(
        r"(?im)^(>[^\S\r\n]*Erstellt[^\S\r\n]*:[^\S\r\n]*)$",
        lambda match: f"{match.group(1).rstrip()} {today}",
        rendered,
    )
    if not re.search(r"(?im)^>\s*Erstellt\s*:\s*\d{2}\.\d{2}\.\d{4}\s*$", rendered):
        if rendered and not rendered.endswith("\n"):
            rendered += "\n"
        rendered += f"> Erstellt: {today}\n"
    return rendered
def _set_rw_permissions_for_all_users(path: Path) -> None:
    if os.name == "nt":
        # On Windows, chmod mainly controls read-only flag.
        os.chmod(path, stat.S_IREAD | stat.S_IWRITE)
        return

    os.chmod(path, 0o666)


def _set_todo_in_progress(todo_id: str, file_name: str = "") -> None:
    parser = DocsParser()
    database = db()
    todos = parser.parse_todos_from_markdown()

    matched_todo = False
    target_stem = Path(file_name).stem.strip().casefold()

    for index, todo in enumerate(todos, start=1):
        todo_note = str(todo.get("note", "")).strip()
        note_stem = Path(todo_note).stem.strip().casefold()
        id_matches = str(index) == str(todo_id)
        note_matches = bool(target_stem) and note_stem == target_stem

        if id_matches or note_matches:
            todo["progress"] = "In Progress"
            todo["last_update"] = _today_dd_mm()
            matched_todo = True
            break

    if not matched_todo:
        logger.warning("Could not match todo for progress update. todo_id=%s file_name=%s", todo_id, file_name)

    conf = _load_conf()
    writer = DocsWriter(conf.get("todo", {}).get("full_path_to_todo_file", ""))
    writer.write_todos_table(todos)


def _append_todo(note: str, todo_type: str, progress: str, priority: str = "Medium") -> None:
    parser = DocsParser()
    todos = parser.parse_todos_from_markdown()
    todos.append(
        {
            "note": note,
            "type": json.dumps([value.strip() for value in todo_type.split("/") if value.strip()], ensure_ascii=False),
            "progress": progress,
            "last_update": _today_dd_mm(),
            "priority": _normalize_todo_priority(priority),
        }
    )

    conf = _load_conf()
    writer = DocsWriter(conf.get("todo", {}).get("full_path_to_todo_file", ""))
    writer.write_todos_table(todos)


def _normalize_todo_types(value):
    normalized = _normalize_value(value)
    if isinstance(normalized, list):
        return normalized
    if not normalized:
        return []
    return [str(normalized)]


def _load_todos(parser: DocsParser, query: str) -> list[dict]:
    todos = parser.parse_todos_from_markdown()
    processed_rows: list[dict] = []
    normalized_query = query.casefold()

    for index, row in enumerate(todos, start=1):
        prepared = dict(row)
        prepared["id"] = index
        prepared["type_list"] = _normalize_todo_types(prepared.get("type"))
        prepared["priority"] = _normalize_todo_priority(prepared.get("priority"))
        if normalized_query and normalized_query not in str(prepared.get("note", "")).casefold():
            continue
        processed_rows.append(prepared)

    return _sort_todos_by_priority(processed_rows)


def _parse_deadline_date(value: str) -> datetime | None:
    try:
        return datetime.strptime(str(value or "").strip(), "%d.%m.%Y")
    except ValueError:
        return None


def _parse_deadline_time(value: str):
    time_value = str(value or "").strip()
    if not time_value or time_value == "-":
        return None
    try:
        return datetime.strptime(time_value, "%H:%M").time()
    except ValueError:
        return None


def _deadline_sort_key(deadline: dict) -> tuple[int, datetime, datetime.time]:
    parsed_date = _parse_deadline_date(deadline.get("date", ""))
    if parsed_date is None:
        return (0, datetime.min, datetime.min.time())

    parsed_time = _parse_deadline_time(deadline.get("time", ""))
    return (1, parsed_date, parsed_time or datetime.max.time())


def _deadline_row_class(deadline: dict) -> str:
    date_value = _parse_deadline_date(deadline.get("date", ""))
    if date_value is None:
        return ""

    time_value = _parse_deadline_time(deadline.get("time", ""))
    if time_value is not None:
        target = datetime.combine(date_value.date(), time_value)
    else:
        target = datetime.combine(date_value.date(), datetime.min.time()) + timedelta(hours=23, minutes=59)

    days_remaining = (target - datetime.now()).total_seconds() / 86400
    if days_remaining < 3:
        return "table-danger"
    if days_remaining < 7:
        return "table-warning-deep"
    if days_remaining < 14:
        return "table-warning"
    return ""


def _load_deadlines(parser: DocsParser, include_description: bool = False) -> list[dict]:
    rows = parser.parse_deadlines_from_markdown(include_description=include_description)
    processed_rows: list[dict] = []
    for source_index, row in enumerate(rows, start=1):
        prepared = dict(row)
        prepared["id"] = source_index
        prepared["status"] = prepared.get("status") if prepared.get("status") in DEADLINE_STATUS_OPTIONS else "Not Started"
        prepared["row_class"] = _deadline_row_class(prepared)
        processed_rows.append(prepared)

    return sorted(processed_rows, key=_deadline_sort_key)


def _count_open_todos(todos: list[dict]) -> int:
    return len([todo for todo in todos if str(todo.get("progress", "")).strip() != "Done"])


def _count_upcoming_deadlines(deadlines: list[dict], days_window: int = 7) -> int:
    now = datetime.now()
    cutoff = now + timedelta(days=days_window)
    upcoming_count = 0

    for deadline in deadlines:
        if str(deadline.get("status", "")).strip() == "Done":
            continue

        deadline_date = _parse_deadline_date(deadline.get("date", ""))
        if deadline_date is None:
            continue

        deadline_time = _parse_deadline_time(deadline.get("time", ""))
        deadline_at = (
            datetime.combine(deadline_date.date(), deadline_time)
            if deadline_time is not None
            else datetime.combine(deadline_date.date(), datetime.max.time())
        )
        if now <= deadline_at < cutoff:
            upcoming_count += 1

    return upcoming_count


def _count_all_deadlines(deadlines: list[dict]) -> int:
    return len(deadlines)


def _normalize_ratio(value: int, total: int) -> float:
    if total <= 0:
        return 0.0
    return max(0.0, min(1.0, value / total))


def _normalize_count(value: int) -> float:
    if value <= 0:
        return 0.0
    return value / (value + 1)


def _calculate_latest_ai_feedback_average(database: db) -> float | None:
    all_feedback_rows = _load_ai_feedback_rows(database, "", "")
    latest_row_ids = _latest_ai_feedback_row_ids(all_feedback_rows)
    scores = [
        row["score_value"]
        for row in all_feedback_rows
        if int(row.get("id", 0)) in latest_row_ids and row.get("score_value") is not None
    ]
    return sum(scores) / len(scores) if scores else None


def _calculate_index_progress(
    *,
    total_docs: int,
    under_construction_count: int,
    incompliant_docs: int,
    open_todos_count: int,
    total_deadlines_count: int,
    average_ai_score: float | None,
) -> dict:
    safe_average_ai_score = 100.0 if average_ai_score is None else max(0.0, min(100.0, average_ai_score))
    under_construction_norm = _normalize_ratio(under_construction_count, total_docs)
    incompliant_norm = _normalize_ratio(incompliant_docs, total_docs)
    todos_norm = _normalize_count(open_todos_count)
    deadlines_norm = _normalize_count(total_deadlines_count)
    ai_gap_norm = (100.0 - safe_average_ai_score) / 100.0

    weighted_penalty = (
        INDEX_PROGRESS_WEIGHTS["under_construction"] * under_construction_norm
        + INDEX_PROGRESS_WEIGHTS["incompliant"] * incompliant_norm
        + INDEX_PROGRESS_WEIGHTS["todos"] * todos_norm
        + INDEX_PROGRESS_WEIGHTS["deadlines"] * deadlines_norm
        + INDEX_PROGRESS_WEIGHTS["ai_gap"] * ai_gap_norm
    )
    progress_value = round(max(0.0, min(100.0, 100.0 * (1.0 - weighted_penalty))))

    if progress_value >= 80:
        progress_color = "bg-success"
    elif progress_value >= 60:
        progress_color = "bg-info"
    elif progress_value >= 40:
        progress_color = "bg-warning"
    else:
        progress_color = "bg-danger"

    return {
        "value": progress_value,
        "color_class": progress_color,
        "color": _progress_bar_color(progress_value),
        "average_ai_score": safe_average_ai_score,
    }


def _load_hslu_overview(parser: DocsParser, database: db, semester: str, module: str, sw: str) -> tuple[list[str], str, list[str], str, str, list[dict], str]:
    rows = parser.parse_hslu_sw_overview()
    semesters = sorted(
        {str(row.get("semester", "")).strip() for row in rows if str(row.get("semester", "")).strip()},
        key=str.casefold,
    )
    standard_semester = database.get_hslu_standard_semester()

    default_semester = standard_semester if standard_semester in semesters else (semesters[0] if semesters else "")
    selected_semester = semester if semester in semesters else default_semester

    modules = sorted(
        {
            str(row.get("module", "")).strip()
            for row in rows
            if str(row.get("semester", "")).strip() == selected_semester and str(row.get("module", "")).strip()
        },
        key=str.casefold,
    )
    selected_module = module if module in modules else ""
    selected_sw = sw if sw.isdigit() else ""

    filtered_rows = [row for row in rows if str(row.get("semester", "")).strip() == selected_semester] if selected_semester else []
    if selected_module:
        filtered_rows = [row for row in filtered_rows if str(row.get("module", "")).strip() == selected_module]
    if selected_sw:
        filtered_rows = [row for row in filtered_rows if str(row.get("SW", "")).strip() == selected_sw]

    prepared_rows: list[dict] = []
    for row in filtered_rows:
        prepared_row = dict(row)
        prepared_row["entry_indicator"] = _entry_indicator_for_sw_status(
            prepared_row.get("downloaded"),
            prepared_row.get("documented"),
        )
        prepared_rows.append(prepared_row)

    return semesters, selected_semester, modules, selected_module, selected_sw, prepared_rows, standard_semester



def _load_hslu_checklist(parser: DocsParser, semester: str, sw: str, sections: list[str]) -> tuple[list[str], str, str, list[str], list[str], dict[str, list[dict]]]:
    rows = parser.parse_hslu_semester_checklist()
    semesters = sorted(
        {str(row.get("semester", "")).strip() for row in rows if str(row.get("semester", "")).strip()},
        key=str.casefold,
    )
    selected_semester = semester if semester in semesters else (semesters[0] if semesters else "")
    selected_sw = sw.zfill(2) if sw.isdigit() else ""
    filtered_rows = [row for row in rows if str(row.get("semester", "")).strip() == selected_semester] if selected_semester else []
    if selected_sw:
        filtered_rows = [row for row in filtered_rows if (str(row.get("sw", "")).strip() == selected_sw or not str(row.get("sw", "")).strip())]

    available_sections: list[str] = []
    for row in filtered_rows:
        section_name = str(row.get("section") or "").strip()
        if section_name and section_name not in available_sections:
            available_sections.append(section_name)

    selected_sections = [section for section in sections if section in available_sections]
    if not selected_sections:
        default_sections = ["Kontaktstudium", "während Lernblocker"]
        selected_sections = [section for section in default_sections if section in available_sections]

    section_rows = [row for row in filtered_rows if str(row.get("section") or "").strip() in selected_sections]

    deduplicated_rows: list[dict] = []
    seen = set()
    for row in section_rows:
        sw_value = str(row.get("sw") or "").strip()
        checklist_row = str(row.get("checklist_row") or "").strip()
        checklist_item = str(row.get("checklist_item") or "").strip()

        if not sw_value and not checklist_row and not checklist_item:
            continue

        unique_key = (
            str(row.get("semester") or "").strip().casefold(),
            str(row.get("section") or "").strip().casefold(),
            sw_value.casefold(),
            checklist_row.casefold(),
            checklist_item.casefold(),
            str(row.get("file_path") or "").strip(),
        )
        if unique_key in seen:
            continue
        seen.add(unique_key)
        prepared_row = dict(row)
        prepared_row["entry_indicator"] = _entry_indicator_for_sw_status(prepared_row.get("status"))
        deduplicated_rows.append(prepared_row)

    rows_by_section: dict[str, list[dict]] = {section: [] for section in selected_sections}
    for row in deduplicated_rows:
        section_name = str(row.get("section") or "").strip()
        rows_by_section.setdefault(section_name, []).append(row)

    return semesters, selected_semester, selected_sw, available_sections, selected_sections, rows_by_section

def _docs_alpha_sort_key(doc: dict) -> tuple[int, str]:
    title = str(doc.get("title") or "").strip()
    starts_with_digit = title[:1].isdigit()
    return (0 if starts_with_digit else 1, title.casefold())


def _parse_doc_date(value: str | None) -> datetime | None:
    date_label = str(value or "").strip()
    if not date_label or date_label.upper() == "N/A":
        return None

    for date_format in ("%d.%m.%Y", "%Y-%m-%d", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(date_label, date_format)
        except ValueError:
            continue

    return None


def _sort_docs(processed_docs: list[dict], sort_by: str) -> None:
    if sort_by == "title_desc":
        processed_docs.sort(key=_docs_alpha_sort_key, reverse=True)
        return

    if sort_by == "created_newest":
        processed_docs.sort(
            key=lambda doc: (_parse_doc_date(doc.get("created_at")) is None, _parse_doc_date(doc.get("created_at")) or datetime.min),
            reverse=True,
        )
        return

    if sort_by == "created_oldest":
        processed_docs.sort(key=lambda doc: (_parse_doc_date(doc.get("created_at")) is None, _parse_doc_date(doc.get("created_at")) or datetime.max))
        return

    if sort_by == "changed_newest":
        processed_docs.sort(
            key=lambda doc: (_parse_doc_date(doc.get("changed_at")) is None, _parse_doc_date(doc.get("changed_at")) or datetime.min),
            reverse=True,
        )
        return

    if sort_by == "changed_oldest":
        processed_docs.sort(key=lambda doc: (_parse_doc_date(doc.get("changed_at")) is None, _parse_doc_date(doc.get("changed_at")) or datetime.max))
        return

    processed_docs.sort(key=_docs_alpha_sort_key)


def _parse_feedback_score(value) -> float | None:
    try:
        score = float(value)
    except (TypeError, ValueError):
        return None

    if score != score:
        return None
    return score


def _format_feedback_score(value) -> str:
    score = _parse_feedback_score(value)
    if score is None:
        return "N/A"
    if score.is_integer():
        return str(int(score))
    return f"{score:.2f}".rstrip("0").rstrip(".")


def _interpolate_rgb(start: tuple[int, int, int], end: tuple[int, int, int], factor: float) -> tuple[int, int, int]:
    bounded_factor = max(0.0, min(1.0, factor))
    return tuple(round(start[index] + (end[index] - start[index]) * bounded_factor) for index in range(3))


def _progress_bar_color(value: float) -> str:
    bounded_value = max(0.0, min(100.0, float(value)))
    color_stops = [
        (0.0, (109, 56, 117)),
        (30.0, (219, 31, 31)),
        (50.0, (219, 106, 31)),
        (75.0, (219, 191, 31)),
        (90.0, (153, 219, 31)),
        (100.0, (81, 120, 10)),
    ]

    for index in range(1, len(color_stops)):
        end_position, end_color = color_stops[index]
        start_position, start_color = color_stops[index - 1]
        if bounded_value <= end_position:
            segment_span = end_position - start_position
            factor = 0.0 if segment_span == 0 else (bounded_value - start_position) / segment_span
            rgb = _interpolate_rgb(start_color, end_color, factor)
            return "#{:02X}{:02X}{:02X}".format(*rgb)

    return "#{:02X}{:02X}{:02X}".format(*color_stops[-1][1])


def _feedback_score_color(value) -> str:
    score = _parse_feedback_score(value)
    if score is None:
        return "#6c757d"

    bounded_score = max(0.0, min(100.0, score))
    low_color = (220, 53, 69)
    mid_color = (245, 173, 39)
    high_color = (25, 135, 84)

    if bounded_score <= 60:
        rgb = _interpolate_rgb(low_color, mid_color, bounded_score / 60.0 if 60 else 0.0)
    else:
        rgb = _interpolate_rgb(mid_color, high_color, (bounded_score - 60.0) / 40.0)

    return "#{:02X}{:02X}{:02X}".format(*rgb)


def _extract_feedback_body(content: str) -> str:
    match = re.search(r"(?ims)^##\s+Feedback\s*$\n(.*?)(?=\Z)", str(content or ""))
    return match.group(1).strip() if match else str(content or "").strip()


def _ensure_doc_can_receive_ai_feedback(database: db, selected_doc: str) -> None:
    matching_docs = database.get_docs_by_name(selected_doc, exact_match=True)
    if not matching_docs:
        return

    doc_row = next(iter(matching_docs.values()))
    if str(doc_row.get("is_under_construction", "false")).lower() == "true":
        raise ValueError(
            "The note is under construction. The feedback was thus not requested. Change the note status and try again."
        )


def _load_ai_feedback_rows(database: db, name_query: str, score_query: str) -> list[dict]:
    name_filter = str(name_query or "").strip().casefold()
    score_filter = str(score_query or "").strip()
    filtered_rows: list[dict] = []

    for row in database.get_all_ai_feedback():
        prepared = dict(row)
        prepared["score_value"] = _parse_feedback_score(prepared.get("score"))
        prepared["score_display"] = _format_feedback_score(prepared.get("score"))
        prepared["score_color"] = _feedback_score_color(prepared.get("score"))
        prepared["version_display"] = str(prepared.get("version", "N/A"))
        prepared["creation_date"] = str(prepared.get("creation_date", "N/A")).strip() or "N/A"

        file_name = str(prepared.get("file_name", "")).strip()
        if name_filter and name_filter not in file_name.casefold():
            continue

        if score_filter:
            try:
                target_score = int(score_filter)
            except ValueError:
                continue

            rounded_score = round(prepared["score_value"]) if prepared["score_value"] is not None else None
            if rounded_score != target_score:
                continue

        filtered_rows.append(prepared)

    return filtered_rows


def _latest_ai_feedback_row_ids(rows: list[dict]) -> set[int]:
    latest_ids: set[int] = set()
    latest_by_file: dict[str, tuple[int, int]] = {}

    for row in rows:
        file_name = str(row.get("file_name", "")).strip()
        if not file_name:
            continue

        row_id = int(row.get("id", 0))
        version = int(row.get("version", 0))
        file_key = file_name.casefold()

        current_best = latest_by_file.get(file_key)
        candidate = (version, row_id)
        if current_best is None or candidate > current_best:
            latest_by_file[file_key] = candidate

    for _, (_, row_id) in latest_by_file.items():
        latest_ids.add(row_id)

    return latest_ids


def _sync_ai_feedback_and_openrouter_credits(database: db) -> tuple[list[dict], str | None]:
    parser = DocsParser()
    synced_rows = parser.sync_ai_feedback_to_db()

    try:
        credits_left = DocsAIFeedback(_load_conf()).fetch_openrouter_credits_left()
        credits_left_label = f"{credits_left:.4f}".rstrip("0").rstrip(".")
        database.upsert_setting("openrouter_credits_left", credits_left_label)
    except Exception:
        logger.warning("Could not sync OpenRouter credits left\n%s", traceback.format_exc())
        credits_left_label = None

    return synced_rows, credits_left_label


def _load_learning_conf(conf: dict) -> dict:
    return conf.get("learning", {})


def _sanitize_learning_questions(raw_questions: list[dict]) -> list[dict]:
    sanitized: list[dict] = []
    allowed_types = {"MULTIPLE_CHOICE", "SINGLE_CHOICE", "FREETEXT"}
    for index, question in enumerate(raw_questions, start=1):
        if not isinstance(question, dict):
            continue
        qid = str(question.get("id", "")).strip() or f"Q{index:03d}"
        qtype = str(question.get("type", "FREETEXT")).strip().upper()
        if qtype not in allowed_types:
            qtype = "FREETEXT"
        text = re.sub(r"\s+", " ", str(question.get("text", "")).strip())
        if not text:
            continue
        options = question.get("options", [])
        if not isinstance(options, list):
            options = []
        options = [re.sub(r"\s+", " ", str(item).strip()) for item in options if str(item).strip()]
        if qtype == "FREETEXT":
            options = []
        sanitized.append({"id": qid, "type": qtype, "text": text, "options": options})
    return sanitized


def _sanitize_learning_answers(raw_answers: list[dict], question_ids: set[str]) -> list[dict]:
    sanitized: list[dict] = []
    for answer in raw_answers:
        if not isinstance(answer, dict):
            continue
        question_id = str(answer.get("question_id", "")).strip()
        if not question_id or question_id not in question_ids:
            continue
        correct_answers = answer.get("correct_answers", [])
        if not isinstance(correct_answers, list):
            correct_answers = [str(correct_answers)]
        cleaned_answers = [re.sub(r"\s+", " ", str(item).strip()) for item in correct_answers if str(item).strip()]
        sanitized.append({"question_id": question_id, "correct_answers": cleaned_answers})
    return sanitized


def _sync_openrouter_credits_only(database: db) -> str | None:
    try:
        credits_left = DocsAIFeedback(_load_conf()).fetch_openrouter_credits_left()
        credits_left_label = f"{credits_left:.4f}".rstrip("0").rstrip(".")
        database.upsert_setting("openrouter_credits_left", credits_left_label)
        return credits_left_label
    except Exception:
        logger.warning("Could not refresh OpenRouter credits\n%s", traceback.format_exc())
        return None


@app.route("/", methods=["GET"])
def index():
    view = request.args.get("view", "all")
    query = request.args.get("q", "").strip()
    parser = DocsParser()
    sort_by = request.args.get("sort", "title_asc").strip()
    if sort_by not in {"title_asc", "title_desc", "created_newest", "created_oldest", "changed_newest", "changed_oldest"}:
        sort_by = "title_asc"

    database = db()
    docs = _load_docs(database, parser, view, query)
    under_construction_docs = database.get_under_construction_docs()

    total_docs = len(docs)
    compliant_docs = len([d for d in docs.values() if d.get("is_compliant") == "true"])
    incompliant_docs = len([d for d in docs.values() if d.get("is_compliant") == "false"])

    processed_docs = []
    for item in docs.values():
        row = dict(item)
        row["tags_list"] = _to_display_list(row.get("tags"))
        row["noncompliance_reason_list"] = _to_display_list(row.get("noncompliance_reason"))
        row["changed_at_list"] = _to_display_list(row.get("changed_at"))
        row["is_under_construction"] = str(row.get("is_under_construction", "false")).lower()
        row["display_title"] = f"🚧 {row.get('title', '')}" if row["is_under_construction"] == "true" else row.get("title", "")
        if row["is_under_construction"] == "true":
            row["is_compliant"] = "Not Determined"
            row["noncompliance_reason_list"] = []
        row["compliance_tag_class"] = _compliance_tag_class(row)
        processed_docs.append(row)

    _sort_docs(processed_docs, sort_by)
    last_sync_time = database.get_last_sync_time()
    open_todos_count = _count_open_todos(_load_todos(parser, query=""))
    deadlines = _load_deadlines(parser, include_description=False)
    upcoming_deadlines_count = _count_upcoming_deadlines(deadlines)
    total_deadlines_count = _count_all_deadlines(deadlines)

    under_construction_count = len(under_construction_docs)
    average_ai_score = _calculate_latest_ai_feedback_average(database)
    index_progress = _calculate_index_progress(
        total_docs=total_docs,
        under_construction_count=under_construction_count,
        incompliant_docs=incompliant_docs,
        open_todos_count=open_todos_count,
        total_deadlines_count=total_deadlines_count,
        average_ai_score=average_ai_score,
    )
    selectable_docs = sorted(
        [{"id": str(item.get("id", "")).strip(), "title": str(item.get("title", "")).strip()} for item in database.get_all_docs().values()],
        key=lambda value: value["title"].casefold(),
    )

    return render_template(
        "index.html",
        docs=processed_docs,
        total_docs=total_docs,
        compliant_docs=compliant_docs,
        incompliant_docs=incompliant_docs,
        selected_view=view,
        query=query,
        selected_sort=sort_by,
        last_sync_time=_format_sync_time_relative_to_now(last_sync_time),
        last_sync_alert=_sync_banner_state(last_sync_time),
        under_construction_count=under_construction_count,
        open_todos_count=open_todos_count,
        upcoming_deadlines_count=upcoming_deadlines_count,
        total_progress=index_progress["value"],
        total_progress_color_class=index_progress["color_class"],
        total_progress_color=index_progress["color"],
        selectable_docs=selectable_docs,
    )


@app.route("/version_control", methods=["GET"])
def version_control_overview():
    try:
        version_status = DocsVersionHandler().get_status_snapshot()
        synced_at = now_in_zurich_str()
        error_message = ""
    except Exception as exc:
        version_status = {"has_changes": False, "changes": [], "untracked_files": []}
        synced_at = "Never"
        error_message = str(exc)
        flash(f"Failed to load local git status: {exc}", "danger")

    return render_template(
        "version_control.html",
        has_changes=version_status.get("has_changes", False),
        changes=version_status.get("changes", []),
        untracked_files=version_status.get("untracked_files", []),
        synced_at=_format_sync_time_relative_to_now(synced_at),
        synced_at_alert=_sync_banner_state(synced_at),
        status_error=error_message,
    )


@app.route("/version_control/sync", methods=["POST"])
def version_control_sync():
    try:
        snapshot = DocsVersionHandler().get_status_snapshot()
        change_count = len(snapshot.get("changes", []))
        untracked_count = len(snapshot.get("untracked_files", []))
        synced_at = now_in_zurich_str()
        flash(
            f"Local git status refreshed at {synced_at}. {change_count} changed files and {untracked_count} newly created/deleted files detected across the repository.",
            "success",
        )
    except Exception as exc:
        flash(f"Failed to refresh local git status: {exc}", "danger")

    return redirect(url_for("version_control_overview"))


@app.route("/version_control/revert", methods=["POST"])
def version_control_revert_file():
    file_path = request.form.get("file_path", "").strip()
    if not file_path:
        flash("A file path is required to revert changes.", "warning")
        return redirect(url_for("version_control_overview"))

    try:
        version_handler = DocsVersionHandler()
        version_handler.revert_file(file_path)
        flash(f"Reverted changes for: {file_path}", "success")
    except Exception as exc:
        flash(f"Failed to revert changes for {file_path}: {exc}", "danger")

    return redirect(url_for("version_control_overview"))


@app.route("/api/version_control/status", methods=["GET"])
def version_control_status_api():
    try:
        snapshot = DocsVersionHandler().get_status_snapshot()
        snapshot["synced_at"] = now_in_zurich_str()
        return jsonify(snapshot)
    except Exception as exc:
        return jsonify({"has_changes": False, "changes": [], "untracked_files": [], "synced_at": "Never", "error": str(exc)}), 500


@app.route("/export", methods=["GET"])
def export_page():
    database = db()
    docs = sorted(database.get_all_docs().values(), key=lambda row: str(row.get("title", "")).casefold())
    tags = database.get_all_tags()
    return render_template("export.html", docs=docs, tags=tags)


@app.route("/export", methods=["POST"])
def export_docs_pdf():
    export_title = _normalize_export_title(request.form.get("title", ""))
    export_description = _normalize_export_description(request.form.get("description", ""))
    export_mode = str(request.form.get("export_mode", "")).strip().lower()

    if not export_title:
        flash("A title is required for the export.", "danger")
        return redirect(url_for("export_page"))

    if export_mode not in {"name", "tag"}:
        flash("Export mode must be either 'name' or 'tag'.", "danger")
        return redirect(url_for("export_page"))

    database = db()
    selected_docs: dict[int, dict] = {}

    if export_mode == "name":
        selected_titles = [str(value).strip() for value in request.form.getlist("selected_docs") if str(value).strip()]
        if not selected_titles:
            flash("Please select at least one document.", "warning")
            return redirect(url_for("export_page"))

        allowed_titles = {str(doc.get("title", "")).strip() for doc in database.get_all_docs().values()}
        valid_titles = [title for title in selected_titles if title in allowed_titles]
        if len(valid_titles) != len(selected_titles):
            flash("One or more selected documents are invalid.", "danger")
            return redirect(url_for("export_page"))

        for title in valid_titles:
            docs_by_name = database.get_docs_by_name(title, exact_match=True)
            selected_docs.update(docs_by_name)
    else:
        selected_tags = [_normalize_tag_value(tag) for tag in request.form.getlist("selected_tags")]
        selected_tags = [tag for tag in selected_tags if tag]

        if not selected_tags:
            flash("Please select at least one tag.", "warning")
            return redirect(url_for("export_page"))

        allowed_tags = set(database.get_all_tags())
        if any(tag not in allowed_tags for tag in selected_tags):
            flash("One or more selected tags are invalid.", "danger")
            return redirect(url_for("export_page"))

        for tag in selected_tags:
            selected_docs.update(database.get_docs_by_tag(tag))

    if not selected_docs:
        flash("No matching documents found for export.", "warning")
        return redirect(url_for("export_page"))

    try:
        exporter = DocsExporter()
        output_pdf = exporter.export_docs_to_pdf(
            export_title=export_title,
            docs=list(selected_docs.values()),
            user_description=export_description,
        )
    except Exception as exc:
        logger.error("Failed to create export PDF\n%s", traceback.format_exc())
        flash(f"Export failed: {exc}", "danger")
        return redirect(url_for("export_page"))

    return send_file(output_pdf, as_attachment=True, download_name=output_pdf.name, mimetype="application/pdf")


@app.route("/scan", methods=["POST"])
def scan_docs():
    try:
        logger.info("UI requested full scan")
        parser = DocsParser()
        parser.parse_and_add_ALL_docs_to_db()
        logger.info("UI full scan completed")
        flash("Scan completed successfully.", "success")
    except BaseException as exc:
        if isinstance(exc, SystemExit):
            logger.error("Scan failed due to parser SystemExit")
            flash(
                "Scan failed: parser exited early. Check docs path in conf.json and parser/database logs.",
                "danger",
            )
        else:
            logger.error("Scan failed with unhandled exception\n%s", traceback.format_exc())
            flash(traceback.format_exc(), "danger")

    return redirect(url_for("index"))




@app.route("/todo", methods=["GET"])
def todo_overview():
    query = request.args.get("q", "").strip()
    parser = DocsParser()

    try:
        todos = _load_todos(parser, query)
    except BaseException as exc:
        if isinstance(exc, SystemExit):
            flash("Todo parsing failed. Check conf.json todo path and parser logs.", "danger")
        else:
            flash("Automatic todo parsing failed.", "warning")
        todos = []

    database = db()
    all_tags = database.get_all_tags()

    templates = _load_template_options()
    template_labels = {
        "new": "New",
        "update": "Update",
    }
    template_options = [
        {"key": key, "label": template_labels[key]}
        for key in ["new", "update"]
        if key in templates
    ]

    return render_template(
        "todo.html",
        todos=todos,
        query=query,
        template_options=template_options,
        all_tags=all_tags,
        create_doc_state={
            "missing_history": request.args.get("missing_history", "").strip() == "1",
            "todo_id": request.args.get("todo_id", "").strip(),
            "template_name": request.args.get("template_name", "").strip(),
            "file_name": request.args.get("file_name", "").strip(),
            "reason": request.args.get("reason", "").strip(),
        },
    )


@app.route("/todo/sync", methods=["POST"])
def sync_todos():
    try:
        parsed = DocsParser().parse_todos_from_markdown()
        flash(f"Reloaded {len(parsed)} todos from markdown.", "success")
    except BaseException as exc:
        if isinstance(exc, SystemExit):
            flash("Todo parsing failed. Check conf.json todo path and parser logs.", "danger")
        else:
            flash(traceback.format_exc(), "danger")

    return redirect(url_for("todo_overview"))


@app.route("/deadlines", methods=["GET"])
def deadlines_overview():
    parser = DocsParser()
    try:
        deadlines = _load_deadlines(parser, include_description=False)
    except BaseException as exc:
        if isinstance(exc, SystemExit):
            flash("Deadline parsing failed. Check conf.json deadlines path and parser logs.", "danger")
        else:
            flash("Automatic deadline parsing failed.", "warning")
        deadlines = []

    return render_template(
        "deadlines.html",
        deadlines=deadlines,
        total_deadlines=len(deadlines),
        deadline_status_options=DEADLINE_STATUS_OPTIONS,
    )


@app.route("/deadlines/add", methods=["POST"])
def add_deadline():
    name = re.sub(r"\s+", " ", request.form.get("name", "").strip())
    description = re.sub(r"\s+", " ", request.form.get("description", "").strip())
    date = request.form.get("date", "").strip()
    time = request.form.get("time", "").strip() or "-"
    status = request.form.get("status", "Not Started").strip()

    if not name:
        flash("Deadline name is required.", "warning")
        return redirect(url_for("deadlines_overview"))
    if _parse_deadline_date(date) is None:
        flash("Deadline date must match DD.MM.YYYY.", "warning")
        return redirect(url_for("deadlines_overview"))
    if _parse_deadline_time(time) is None and time != "-":
        flash("Deadline time must match HH:MM or '-'.", "warning")
        return redirect(url_for("deadlines_overview"))
    if status not in DEADLINE_STATUS_OPTIONS:
        flash("Invalid status selection.", "danger")
        return redirect(url_for("deadlines_overview"))

    try:
        parser = DocsParser()
        conf = _load_conf()
        current_deadlines = parser.parse_deadlines_from_markdown(include_description=True)
        current_deadlines.append(
            {
                "name": name,
                "description": description,
                "date": date,
                "time": time,
                "status": status,
            }
        )
        writer = DocsWriter(deadlines_file_path=conf.get("deadlines", {}).get("full_path_to_deadlines_file", ""))
        writer.write_deadlines_table(current_deadlines)
        flash("Deadline added successfully.", "success")
    except BaseException:
        flash("Failed to add deadline. Check logs and markdown format.", "danger")

    return redirect(url_for("deadlines_overview"))


@app.route("/deadlines/delete", methods=["POST"])
def delete_deadline():
    deadline_id = request.form.get("deadline_id", "").strip()
    if not deadline_id.isdigit():
        flash("Deadline id is required.", "warning")
        return redirect(url_for("deadlines_overview"))

    try:
        parser = DocsParser()
        conf = _load_conf()
        current_deadlines = parser.parse_deadlines_from_markdown(include_description=True)
        kept_deadlines = [deadline for index, deadline in enumerate(current_deadlines, start=1) if str(index) != deadline_id]
        writer = DocsWriter(deadlines_file_path=conf.get("deadlines", {}).get("full_path_to_deadlines_file", ""))
        writer.write_deadlines_table(kept_deadlines)
        flash("Deadline deleted.", "success")
    except BaseException:
        flash("Failed to delete deadline.", "danger")

    return redirect(url_for("deadlines_overview"))


@app.route("/deadlines/edit", methods=["GET"])
def edit_deadline_page():
    deadline_id = request.args.get("deadline_id", "").strip()
    if not deadline_id.isdigit():
        flash("Deadline id is required.", "warning")
        return redirect(url_for("deadlines_overview"))

    parser = DocsParser()
    deadlines = parser.parse_deadlines_from_markdown(include_description=True)
    selected_deadline = None
    for index, deadline in enumerate(deadlines, start=1):
        if str(index) == deadline_id:
            selected_deadline = dict(deadline)
            selected_deadline["id"] = index
            selected_deadline["status"] = selected_deadline.get("status") if selected_deadline.get("status") in DEADLINE_STATUS_OPTIONS else "Not Started"
            break

    if selected_deadline is None:
        flash("Deadline not found.", "warning")
        return redirect(url_for("deadlines_overview"))

    return render_template("deadline_edit.html", deadline=selected_deadline, deadline_status_options=DEADLINE_STATUS_OPTIONS)


@app.route("/deadlines/edit", methods=["POST"])
def update_deadline():
    deadline_id = request.form.get("deadline_id", "").strip()
    name = re.sub(r"\s+", " ", request.form.get("name", "").strip())
    description = re.sub(r"\s+", " ", request.form.get("description", "").strip())
    date = request.form.get("date", "").strip()
    time = request.form.get("time", "").strip() or "-"
    status = request.form.get("status", "Not Started").strip()

    if not deadline_id.isdigit():
        flash("Deadline id is required.", "warning")
        return redirect(url_for("deadlines_overview"))
    if not name:
        flash("Deadline name is required.", "warning")
        return redirect(url_for("edit_deadline_page", deadline_id=deadline_id))
    if _parse_deadline_date(date) is None:
        flash("Deadline date must match DD.MM.YYYY.", "warning")
        return redirect(url_for("edit_deadline_page", deadline_id=deadline_id))
    if _parse_deadline_time(time) is None and time != "-":
        flash("Deadline time must match HH:MM or '-'.", "warning")
        return redirect(url_for("edit_deadline_page", deadline_id=deadline_id))
    if status not in DEADLINE_STATUS_OPTIONS:
        flash("Invalid status selection.", "danger")
        return redirect(url_for("edit_deadline_page", deadline_id=deadline_id))

    try:
        parser = DocsParser()
        conf = _load_conf()
        current_deadlines = parser.parse_deadlines_from_markdown(include_description=True)

        updated = False
        for index, deadline in enumerate(current_deadlines, start=1):
            if str(index) == deadline_id:
                deadline["name"] = name
                deadline["description"] = description
                deadline["date"] = date
                deadline["time"] = time
                deadline["status"] = status
                updated = True
                break

        if not updated:
            flash("Deadline not found.", "warning")
            return redirect(url_for("deadlines_overview"))

        writer = DocsWriter(deadlines_file_path=conf.get("deadlines", {}).get("full_path_to_deadlines_file", ""))
        writer.write_deadlines_table(current_deadlines)
        flash("Deadline updated.", "success")
    except BaseException:
        flash("Failed to update deadline.", "danger")
        return redirect(url_for("edit_deadline_page", deadline_id=deadline_id))

    return redirect(url_for("deadlines_overview"))


@app.route("/todo/add", methods=["POST"])
def add_todo():
    note = request.form.get("note", "").strip()
    todo_type = request.form.get("type", "").strip()
    progress = request.form.get("progress", "Not Started").strip()
    priority = _normalize_todo_priority(request.form.get("priority", "Medium"))

    if not note or not todo_type:
        flash("Todo note and type are required.", "warning")
        return redirect(url_for("todo_overview"))

    try:
        _append_todo(note=note, todo_type=todo_type, progress=progress, priority=priority)
        flash("Todo added successfully.", "success")
    except BaseException:
        flash("Failed to add todo. Check logs and markdown format.", "danger")

    return redirect(url_for("todo_overview"))


@app.route("/todo/delete", methods=["POST"])
def delete_todo():
    todo_id = request.form.get("todo_id", "").strip()
    if not todo_id.isdigit():
        flash("Todo id is required.", "warning")
        return redirect(url_for("todo_overview"))

    try:
        current_todos = DocsParser().parse_todos_from_markdown()
        kept_todos = [todo for index, todo in enumerate(current_todos, start=1) if str(index) != todo_id]

        conf = _load_conf()
        writer = DocsWriter(conf.get("todo", {}).get("full_path_to_todo_file", ""))
        writer.write_todos_table(kept_todos)
        flash("Todo deleted.", "success")
    except BaseException:
        flash("Failed to delete todo.", "danger")

    return redirect(url_for("todo_overview"))


@app.route("/todo/progress", methods=["POST"])
def update_todo_progress():
    todo_id = request.form.get("todo_id", "").strip()
    progress = request.form.get("progress", "Not Started").strip()

    if not todo_id.isdigit():
        flash("Todo id is required.", "warning")
        return redirect(url_for("todo_overview"))

    try:
        current_todos = DocsParser().parse_todos_from_markdown()

        for index, todo in enumerate(current_todos, start=1):
            if str(index) == todo_id:
                todo["progress"] = progress
                todo["last_update"] = _today_dd_mm()
                break

        conf = _load_conf()
        writer = DocsWriter(conf.get("todo", {}).get("full_path_to_todo_file", ""))
        writer.write_todos_table(current_todos)
        flash("Todo progress updated.", "success")
    except BaseException:
        flash("Failed to update todo progress.", "danger")

    return redirect(url_for("todo_overview"))


@app.route("/todo/priority", methods=["POST"])
def update_todo_priority():
    todo_id = request.form.get("todo_id", "").strip()
    priority = _normalize_todo_priority(request.form.get("priority", "Medium"))

    if not todo_id.isdigit():
        flash("Todo id is required.", "warning")
        return redirect(url_for("todo_overview"))

    try:
        current_todos = DocsParser().parse_todos_from_markdown()

        for index, todo in enumerate(current_todos, start=1):
            if str(index) == todo_id:
                todo["priority"] = priority
                todo["last_update"] = _today_dd_mm()
                break

        conf = _load_conf()
        writer = DocsWriter(conf.get("todo", {}).get("full_path_to_todo_file", ""))
        writer.write_todos_table(current_todos)
        flash("Todo priority updated.", "success")
    except BaseException:
        flash("Failed to update todo priority.", "danger")

    return redirect(url_for("todo_overview"))


@app.route("/todo/create-doc", methods=["POST"])
def create_doc_from_todo_template():
    todo_id = request.form.get("todo_id", "").strip()
    template_key = request.form.get("template_name", "").strip().lower()
    file_name = request.form.get("file_name", "").strip()
    reason = request.form.get("reason", "").strip()
    create_history = request.form.get("create_history", "false").strip().lower() == "true"

    from_index = request.form.get("from_index", "false").strip().lower() == "true"
    selected_doc = request.form.get("selected_doc", "").strip()

    if from_index:
        template_key = "update"
        file_name = selected_doc

    if template_key not in {"new", "update"}:
        flash("Invalid template action request.", "warning")
        return redirect(url_for("todo_overview"))

    normalized_file_name = _normalize_md_filename(file_name)
    if not normalized_file_name:
        flash("Invalid file name. Please use a valid markdown file name.", "danger")
        return redirect(url_for("index") if from_index else url_for("todo_overview"))

    template_options = _load_template_options()
    template_path = template_options.get(template_key)
    if template_path is None or not template_path.exists():
        flash("Template file not found. Please verify templates in /the-knowledge/03_TEMPLATES.", "danger")
        return redirect(url_for("index") if from_index else url_for("todo_overview"))

    conf = _load_conf()
    docs_dir = Path(conf.get("docs", {}).get("full_path_to_docs", "")).resolve()
    writer = DocsWriter(conf.get("todo", {}).get("full_path_to_todo_file", ""))
    target_path = docs_dir / normalized_file_name

    try:
        template_content = _render_doc_template(template_path.read_text(encoding="utf-8"))
    except OSError:
        flash("Failed to read template file.", "danger")
        return redirect(url_for("index") if from_index else url_for("todo_overview"))

    if template_key == "new":
        if target_path.exists():
            flash("A note with this file name already exists. Please choose another file name.", "danger")
            return redirect(url_for("todo_overview"))

        try:
            writer.create_note_from_template(target_path, template_content)
            _set_rw_permissions_for_all_users(target_path)
            _set_todo_in_progress(todo_id, normalized_file_name)
            flash("New note created from template successfully.", "success")
        except BaseException:
            flash("Failed to create note from template.", "danger")
        return redirect(url_for("todo_overview"))

    if not reason:
        flash("Reason is required for update template.", "warning")
        return redirect(url_for("index") if from_index else url_for("todo_overview"))

    if not target_path.exists():
        flash("Note file not found in 02_DOCS. Please provide an existing file name.", "danger")
        return redirect(url_for("index") if from_index else url_for("todo_overview"))

    success, missing_sections = writer.prepend_template_to_existing_note(
        target_path=target_path,
        template_content=template_content,
        reason=reason,
        create_history=create_history,
    )
    if not success and from_index and "#### Page History" in missing_sections:
        success, missing_sections = writer.prepend_template_to_existing_note(
            target_path=target_path,
            template_content=template_content,
            reason=reason,
            create_history=True,
        )

    if not success and "#### Page History" in missing_sections:
        flash("'#### Page History' not found. Confirm automatic creation to continue.", "warning")
        return redirect(
            url_for(
                "todo_overview",
                missing_history="1",
                todo_id=todo_id,
                template_name=template_key,
                file_name=normalized_file_name,
                reason=reason,
            )
        )

    try:
        _set_rw_permissions_for_all_users(target_path)
        if from_index:
            _append_todo(
                note=f"{Path(normalized_file_name).stem} ({reason})",
                todo_type="Update",
                progress="In Progress",
                priority="Medium",
            )
            flash("Update note request created and todo added.", "success")
            return redirect(url_for("index"))

        _set_todo_in_progress(todo_id, normalized_file_name)
        flash("Note updated from template successfully.", "success")
    except BaseException:
        flash("Failed to update note from template.", "danger")

    return redirect(url_for("index") if from_index else url_for("todo_overview"))


@app.route("/docs/<int:doc_id>/edit", methods=["GET"])
def edit_doc_resources(doc_id: int):
    database = db()
    doc_map = database.get_docs_by_id(doc_id)
    if not doc_map:
        flash("Document not found.", "warning")
        return redirect(url_for("index"))

    doc = next(iter(doc_map.values()))
    all_tags = database.get_all_tags()
    pending_resource_updates = session.pop(f"pending_doc_resource_updates_{doc_id}", None)

    return render_template(
        "doc_edit.html",
        doc=doc,
        all_tags=all_tags,
        current_resources={
            "tags": _to_display_list(doc.get("tags")),
            "links": _link_map_to_items(doc.get("links")),
            "video_links": _link_map_to_items(doc.get("video_links")),
        },
        edit_state={
            "missing_sections": request.args.get("missing_sections", "").strip(),
            "pending_updates": pending_resource_updates or {},
        },
    )


@app.route("/docs/<int:doc_id>/edit", methods=["POST"])
def save_doc_resources(doc_id: int):
    database = db()
    doc_map = database.get_docs_by_id(doc_id)
    if not doc_map:
        flash("Document not found.", "warning")
        return redirect(url_for("index"))

    doc = next(iter(doc_map.values()))
    doc_title = str(doc.get("title", "")).strip()
    conf = _load_conf()
    docs_dir = Path(conf.get("docs", {}).get("full_path_to_docs", "")).resolve()
    doc_path = docs_dir / f"{doc_title}.md"
    if not doc_path.exists():
        flash("Markdown file for selected document was not found.", "danger")
        return redirect(url_for("edit_doc_resources", doc_id=doc_id))

    tags_to_add = _parse_multiline_tags(request.form.get("tags_to_add", ""))
    tags_to_remove = _parse_multiline_tags("\n".join(request.form.getlist("selected_tags_to_remove")))
    existing_links_original = request.form.getlist("existing_links_original")
    existing_links_description = request.form.getlist("existing_links_description")
    existing_links_link = request.form.getlist("existing_links_link")
    selected_links_to_remove = set(request.form.getlist("selected_links_to_remove"))

    existing_video_links_original = request.form.getlist("existing_video_links_original")
    existing_video_links_description = request.form.getlist("existing_video_links_description")
    existing_video_links_link = request.form.getlist("existing_video_links_link")
    selected_video_links_to_remove = set(request.form.getlist("selected_video_links_to_remove"))

    new_links_description = request.form.getlist("new_links_description")
    new_links_link = request.form.getlist("new_links_link")
    new_video_links_description = request.form.getlist("new_video_links_description")
    new_video_links_link = request.form.getlist("new_video_links_link")
    create_missing_sections = request.form.get("create_missing_sections", "false").strip().lower() == "true"

    def _collect_link_map(
        original_values: list[str],
        description_values: list[str],
        link_values: list[str],
        removed_originals: set[str],
        append_descriptions: list[str] | None = None,
        append_links: list[str] | None = None,
    ) -> dict[str, str]:
        collected: dict[str, str] = {}
        for original, description, link in zip(original_values, description_values, link_values):
            original_clean = str(original or "").strip()
            if original_clean and original_clean in removed_originals:
                continue

            candidate_link = str(link or "").strip()
            candidate_description = re.sub(r"\s+", " ", str(description or "").strip())
            if not candidate_link:
                continue
            if not _is_valid_http_url(candidate_link):
                continue
            collected[candidate_link] = candidate_description or candidate_link

        if append_descriptions is None or append_links is None:
            return collected

        for description, link in zip(append_descriptions, append_links):
            candidate_link = str(link or "").strip()
            candidate_description = re.sub(r"\s+", " ", str(description or "").strip())
            if not candidate_link:
                continue
            if not _is_valid_http_url(candidate_link):
                continue
            collected[candidate_link] = candidate_description or candidate_link

        return collected

    links_map = _collect_link_map(
        original_values=existing_links_original,
        description_values=existing_links_description,
        link_values=existing_links_link,
        removed_originals=selected_links_to_remove,
        append_descriptions=new_links_description,
        append_links=new_links_link,
    )
    video_links_map = _collect_link_map(
        original_values=existing_video_links_original,
        description_values=existing_video_links_description,
        link_values=existing_video_links_link,
        removed_originals=selected_video_links_to_remove,
        append_descriptions=new_video_links_description,
        append_links=new_video_links_link,
    )

    writer = DocsWriter(conf.get("todo", {}).get("full_path_to_todo_file", ""))
    success, missing_sections = writer.update_doc_resources(
        doc_path=doc_path,
        tags_to_add=tags_to_add,
        tags_to_remove=tags_to_remove,
        links_map=links_map,
        video_links_map=video_links_map,
        create_missing_sections=create_missing_sections,
    )

    if not success:
        session[f"pending_doc_resource_updates_{doc_id}"] = {
            "tags_to_add": request.form.get("tags_to_add", ""),
            "tags_to_remove": "\n".join(request.form.getlist("selected_tags_to_remove")),
        }
        flash(
            f"Missing chapter(s): {', '.join(missing_sections)}. Confirm creation to continue.",
            "warning",
        )
        return redirect(url_for("edit_doc_resources", doc_id=doc_id, missing_sections="|".join(missing_sections)))

    try:
        _set_rw_permissions_for_all_users(doc_path)
        parser = DocsParser()
        parser.parse_and_add_ALL_docs_to_db()
        flash("Document resources updated successfully.", "success")
    except BaseException:
        flash("Document was updated but sync failed. Please run a full scan.", "warning")

    return redirect(url_for("index"))


@app.route("/ai_feedback", methods=["GET"])
def ai_feedback_overview():
    database = db()
    name_query = request.args.get("name", "").strip()
    score_query = request.args.get("score", "").strip()
    all_feedback_rows = _load_ai_feedback_rows(database, "", "")
    feedback_rows = _load_ai_feedback_rows(database, name_query, score_query)
    latest_row_ids = _latest_ai_feedback_row_ids(all_feedback_rows)

    for row in all_feedback_rows:
        row["included_in_average"] = int(row.get("id", 0)) in latest_row_ids

    for row in feedback_rows:
        row["included_in_average"] = int(row.get("id", 0)) in latest_row_ids

    feedback_docs_present = {
        str(row.get("file_name", "")).strip().casefold()
        for row in all_feedback_rows
        if str(row.get("file_name", "")).strip()
    }

    available_docs = sorted(
        [
            {
                "id": str(item.get("id", "")).strip(),
                "title": str(item.get("title", "")).strip(),
                "has_feedback": str(item.get("title", "")).strip().casefold() in feedback_docs_present,
            }
            for item in database.get_all_docs().values()
            if str(item.get("title", "")).strip()
        ],
        key=lambda item: item["title"].casefold(),
    )

    average_score = _calculate_latest_ai_feedback_average(database)
    openrouter_credits_left = str(database.get_setting("openrouter_credits_left", "N/A") or "").strip() or "N/A"

    return render_template(
        "ai_feedback.html",
        feedback_rows=feedback_rows,
        total_reports=len(all_feedback_rows),
        average_score=_format_feedback_score(average_score) if average_score is not None else "N/A",
        average_score_color=_feedback_score_color(average_score),
        selected_name=name_query,
        selected_score=score_query,
        available_docs=available_docs,
        openrouter_credits_left=openrouter_credits_left,
    )


@app.route("/ai_feedback/sync", methods=["POST"])
def ai_feedback_sync():
    try:
        synced_rows, credits_left_label = _sync_ai_feedback_and_openrouter_credits(db())
        if credits_left_label is None:
            flash(
                f"Synced {len(synced_rows)} AI feedback file(s). OpenRouter credits could not be refreshed.",
                "warning",
            )
        else:
            flash(
                f"Synced {len(synced_rows)} AI feedback file(s). OpenRouter credits left: ${credits_left_label}.",
                "success",
            )
        return redirect(url_for("ai_feedback_overview"))
    except Exception as exc:
        logger.error("AI feedback sync failed\n%s", traceback.format_exc())
        return render_template("500.html", error_message=str(exc)), 500


@app.route("/ai_feedback/generate", methods=["POST"])
def generate_ai_feedback():
    selected_doc = request.form.get("selected_doc", "").strip()
    redirect_to = _safe_redirect_target(request.form.get("redirect_to"), "ai_feedback_overview")
    if not selected_doc:
        flash("Please select a document for AI feedback.", "warning")
        return redirect(redirect_to)

    try:
        conf = _load_conf()
        database = db()
        _ensure_doc_can_receive_ai_feedback(database, selected_doc)
        parser = DocsParser()
        _sync_ai_feedback_and_openrouter_credits(database)
        ai_feedback_service = DocsAIFeedback(conf)
        selected_doc_note_name = Path(selected_doc).stem.strip()
        latest_feedback_context = None
        latest_feedback_for_context = database.get_latest_ai_feedback_for_file(selected_doc_note_name)
        if latest_feedback_for_context and latest_feedback_for_context.get("path_to_feedback"):
            parsed_latest_feedback = parser.parse_ai_feedback_file(latest_feedback_for_context["path_to_feedback"])
            latest_feedback_context = {
                "version": parsed_latest_feedback.get("version"),
                "score": parsed_latest_feedback.get("score"),
                "creation_date": parsed_latest_feedback.get("creation_date"),
                "feedback": parsed_latest_feedback.get("feedback"),
            }

        feedback_payload = ai_feedback_service.generate_feedback(
            selected_doc,
            previous_feedback=latest_feedback_context,
        )

        latest_feedback = database.get_latest_ai_feedback_for_file(feedback_payload["note_name"])
        next_version = int(latest_feedback.get("version", 0)) + 1 if latest_feedback else 1

        writer = DocsWriter(conf.get("todo", {}).get("full_path_to_todo_file", ""))
        rendered_feedback = writer.render_ai_feedback_template(
            template_content=feedback_payload["feedback_template"],
            note_name=feedback_payload["note_name"],
            version=next_version,
            creation_date=feedback_payload["creation_date"],
            score=_format_feedback_score(feedback_payload["score"]),
            feedback=feedback_payload["feedback"],
        )
        output_path = writer.write_ai_feedback_file(
            output_dir=ai_feedback_service.output_path,
            note_name=feedback_payload["note_name"],
            version=next_version,
            rendered_content=rendered_feedback,
        )
        _set_rw_permissions_for_all_users(output_path)

        _sync_ai_feedback_and_openrouter_credits(database)
        flash(f"AI feedback created successfully for {feedback_payload['note_name']}.", "success")
        return redirect(redirect_to)
    except (ValueError, RuntimeError, FileNotFoundError) as exc:
        logger.error("AI feedback generation failed\n%s", traceback.format_exc())
        flash(str(exc), "warning")
        return redirect(redirect_to)
    except SystemExit:
        logger.error("AI feedback generation aborted by SystemExit\n%s", traceback.format_exc())
        flash("AI feedback generation failed due to a file, database, or parser exit. Check logs and paths.", "danger")
        return redirect(redirect_to)
    except BaseException as exc:
        logger.error("AI feedback generation failed with BaseException\n%s", traceback.format_exc())
        flash(f"AI feedback generation failed unexpectedly: {exc}", "danger")
        return redirect(redirect_to)


@app.route("/ai_feedback/<int:feedback_id>", methods=["GET"])
def ai_feedback_detail(feedback_id: int):
    database = db()
    feedback_row = database.get_ai_feedback_by_id(feedback_id)
    if not feedback_row:
        flash("AI feedback entry not found.", "warning")
        return redirect(url_for("ai_feedback_overview"))

    feedback_path = Path(str(feedback_row.get("path_to_feedback", "")).strip())
    if not feedback_path.exists() or not feedback_path.is_file():
        return render_template("500.html", error_message=f"Feedback markdown file not found: {feedback_path}"), 500

    try:
        feedback_content = feedback_path.read_text(encoding="utf-8")
    except OSError as exc:
        return render_template("500.html", error_message=str(exc)), 500

    prepared_feedback = dict(feedback_row)
    prepared_feedback["score_display"] = _format_feedback_score(prepared_feedback.get("score"))
    prepared_feedback["score_color"] = _feedback_score_color(prepared_feedback.get("score"))
    prepared_feedback["feedback_text"] = _extract_feedback_body(feedback_content)
    prepared_feedback["path_to_feedback"] = str(feedback_path)

    return render_template("ai_feedback_detail.html", feedback=prepared_feedback)


@app.route("/ai_feedback/<int:feedback_id>/delete", methods=["POST"])
def ai_feedback_delete(feedback_id: int):
    redirect_to = _safe_redirect_target(request.form.get("redirect_to"), "ai_feedback_overview")
    database = db()
    feedback_row = database.get_ai_feedback_by_id(feedback_id)
    if not feedback_row:
        flash("AI feedback entry not found.", "warning")
        return redirect(redirect_to)

    feedback_path = Path(str(feedback_row.get("path_to_feedback", "")).strip())
    feedback_name = feedback_path.name or str(feedback_row.get("file_name") or "the feedback").strip()

    try:
        if feedback_path.exists():
            if not feedback_path.is_file():
                raise ValueError(f"Feedback path is not a file: {feedback_path}")
            feedback_path.unlink()
        else:
            flash(f"Feedback file was already missing: {feedback_name}. Removed database entry.", "warning")

        database.delete_ai_feedback_by_id(feedback_id)
        _sync_ai_feedback_and_openrouter_credits(database)
        flash(f"AI feedback deleted successfully: {feedback_name}.", "success")
        return redirect(redirect_to)
    except Exception as exc:
        logger.error("AI feedback delete failed\n%s", traceback.format_exc())
        return render_template("500.html", error_message=str(exc)), 500


@app.route("/learning", methods=["GET"])
def learning_overview():
    database = db()
    name_query = request.args.get("name", "").strip().casefold()
    learning_rows = database.get_all_learnings()
    if name_query:
        learning_rows = [row for row in learning_rows if name_query in str(row.get("file_name", "")).casefold()]
    available_docs = sorted(
        [str(item.get("title", "")).strip() for item in database.get_all_docs().values() if str(item.get("title", "")).strip()],
        key=lambda value: value.casefold(),
    )
    openrouter_credits_left = str(database.get_setting("openrouter_credits_left", "N/A") or "").strip() or "N/A"
    return render_template(
        "learning.html",
        learning_rows=learning_rows,
        total_learnings=len(database.get_all_learnings()),
        selected_name=request.args.get("name", "").strip(),
        available_docs=available_docs,
        openrouter_credits_left=openrouter_credits_left,
    )


@app.route("/learning/sync", methods=["POST"])
def learning_sync():
    parser = DocsParser()
    synced_rows = parser.sync_learning_to_db()
    credits_left_label = _sync_openrouter_credits_only(db())
    if credits_left_label is None:
        flash(f"Synced {len(synced_rows)} learning file(s). OpenRouter credits could not be refreshed.", "warning")
    else:
        flash(f"Synced {len(synced_rows)} learning file(s). OpenRouter credits left: ${credits_left_label}.", "success")
    return redirect(url_for("learning_overview"))


@app.route("/learning/create", methods=["POST"])
def learning_create():
    selected_doc = str(request.form.get("selected_doc", "")).strip()
    normalized_doc = _normalize_md_filename(selected_doc)
    if not normalized_doc:
        flash("Please select a valid markdown note.", "warning")
        return redirect(url_for("learning_overview"))

    conf = _load_conf()
    learning_conf = _load_learning_conf(conf)
    docs_dir = Path(conf.get("docs", {}).get("full_path_to_docs", "")).resolve()
    selected_doc_path = (docs_dir / normalized_doc).resolve()
    if docs_dir not in selected_doc_path.parents or not selected_doc_path.exists():
        flash("Selected markdown note does not exist.", "warning")
        return redirect(url_for("learning_overview"))

    template_path = Path(learning_conf.get("learning_template_path", "/the-knowledge/03_TEMPLATES/2 - New Learning")).resolve()
    output_dir = Path(learning_conf.get("learning_path", "/the-knowledge/07_LEARNINGS")).resolve()
    try:
        template_content = template_path.read_text(encoding="utf-8")
        writer = DocsWriter()
        note_name = selected_doc_path.stem.strip()
        rendered = writer.render_learning_template(
            template_content=template_content,
            note_name=note_name,
            creation_date=_today_dd_mm_yyyy(),
            questions_payload={"questions": []},
            answers_payload={"answers": []},
        )
        learning_path = writer.write_learning_file(output_dir=output_dir, note_name=note_name, rendered_content=rendered)
        _set_rw_permissions_for_all_users(learning_path)
        DocsParser().sync_learning_to_db()
        flash(f"Learning file created: {learning_path.name}", "success")
    except Exception as exc:
        flash(str(exc), "warning")
    return redirect(url_for("learning_overview"))


@app.route("/learning/<int:learning_id>", methods=["GET"])
def learning_detail(learning_id: int):
    database = db()
    learning_row = database.get_learning_by_id(learning_id)
    if not learning_row:
        flash("Learning entry not found.", "warning")
        return redirect(url_for("learning_overview"))
    parsed_learning = DocsParser().parse_learning_file(learning_row.get("path_to_learning", ""))
    attempts = database.get_learning_exam_attempts(learning_id)
    selected_attempt_id = request.args.get("attempt_id", "").strip()
    selected_attempt = None
    selected_answers = {}
    if selected_attempt_id.isdigit():
        selected_attempt = database.get_learning_exam_attempt_by_id(int(selected_attempt_id))
        if selected_attempt and int(selected_attempt.get("learning_id", 0)) == learning_id:
            try:
                selected_answers = json.loads(selected_attempt.get("answers_json", "{}"))
            except json.JSONDecodeError:
                selected_answers = {}
    grouped_questions = {"FREETEXT": [], "MULTIPLE_CHOICE": [], "SINGLE_CHOICE": []}
    for question in parsed_learning.get("questions", []):
        grouped_questions.setdefault(str(question.get("type", "FREETEXT")).upper(), []).append(question)
    return render_template(
        "learning_detail.html",
        learning=learning_row,
        parsed_learning=parsed_learning,
        grouped_questions=grouped_questions,
        attempts=attempts,
        selected_attempt=selected_attempt,
        selected_answers=selected_answers if isinstance(selected_answers, dict) else {},
    )


@app.route("/learning/<int:learning_id>/save", methods=["POST"])
def learning_save(learning_id: int):
    database = db()
    learning_row = database.get_learning_by_id(learning_id)
    if not learning_row:
        flash("Learning entry not found.", "warning")
        return redirect(url_for("learning_overview"))

    parsed_existing = DocsParser().parse_learning_file(learning_row.get("path_to_learning", ""))
    questions_raw = request.form.get("questions_payload", "").strip()
    answers_raw = request.form.get("answers_payload", "").strip()
    try:
        questions_json = json.loads(questions_raw) if questions_raw else {"questions": parsed_existing.get("questions", [])}
        answers_json = json.loads(answers_raw) if answers_raw else {"answers": parsed_existing.get("answers", [])}
    except json.JSONDecodeError:
        flash("Questions/answers JSON is invalid.", "warning")
        return redirect(url_for("learning_detail", learning_id=learning_id))

    questions = _sanitize_learning_questions(questions_json.get("questions", []))
    answers = _sanitize_learning_answers(answers_json.get("answers", []), {item["id"] for item in questions})
    writer = DocsWriter()
    learning_path = Path(str(learning_row.get("path_to_learning", "")).strip()).resolve()
    writer.update_learning_file_questions_answers(
        learning_path=learning_path,
        questions_payload={"questions": questions},
        answers_payload={"answers": answers},
    )
    flash("Learning questions saved.", "success")
    return redirect(url_for("learning_detail", learning_id=learning_id))


@app.route("/learning/<int:learning_id>/generate", methods=["POST"])
def learning_generate_questions(learning_id: int):
    database = db()
    learning_row = database.get_learning_by_id(learning_id)
    if not learning_row:
        flash("Learning entry not found.", "warning")
        return redirect(url_for("learning_overview"))
    try:
        conf = _load_conf()
        learning_conf = _load_learning_conf(conf)
        docs_dir = Path(conf.get("docs", {}).get("full_path_to_docs", "")).resolve()
        source_note_name = str(learning_row.get("source_note_name", "")).strip()
        source_doc_path = (docs_dir / _normalize_md_filename(source_note_name)).resolve()
        if docs_dir not in source_doc_path.parents or not source_doc_path.exists():
            raise FileNotFoundError(f"Source note not found: {source_note_name}")
        prompt_path = Path(learning_conf.get("learning_ai_prompt_path", "/the-knowledge/03_TEMPLATES/2 - Learning AI Prompt.md")).resolve()
        prompt_content = prompt_path.read_text(encoding="utf-8")
        ai_service = DocsAIFeedback(conf)
        generated = ai_service.generate_learning_questions(
            note_name=source_note_name,
            note_content=source_doc_path.read_text(encoding="utf-8"),
            prompt_content=prompt_content,
        )
        questions = _sanitize_learning_questions(generated.get("questions", []))
        answers = _sanitize_learning_answers(generated.get("answers", []), {item["id"] for item in questions})
        DocsWriter().update_learning_file_questions_answers(
            learning_path=Path(learning_row["path_to_learning"]),
            questions_payload={"questions": questions},
            answers_payload={"answers": answers},
        )
        _sync_openrouter_credits_only(database)
        flash("Learning questions generated successfully.", "success")
    except Exception as exc:
        logger.error("Learning question generation failed\n%s", traceback.format_exc())
        flash(str(exc), "warning")
    return redirect(url_for("learning_detail", learning_id=learning_id))


@app.route("/learning/<int:learning_id>/delete", methods=["POST"])
def learning_delete(learning_id: int):
    database = db()
    learning_row = database.get_learning_by_id(learning_id)
    if not learning_row:
        flash("Learning entry not found.", "warning")
        return redirect(url_for("learning_overview"))
    learning_path = Path(str(learning_row.get("path_to_learning", "")).strip()).resolve()
    if learning_path.exists() and learning_path.is_file():
        learning_path.unlink()
    database.delete_learning_by_id(learning_id)
    flash("Learning deleted, including related drafts and attempts.", "success")
    return redirect(url_for("learning_overview"))


def _extract_user_answers_from_form(form_data, questions: list[dict]) -> dict[str, list[str]]:
    answers: dict[str, list[str]] = {}
    for question in questions:
        qid = str(question.get("id", "")).strip()
        if not qid:
            continue
        if str(question.get("type")) == "MULTIPLE_CHOICE":
            raw_values = form_data.getlist(f"answer_{qid}")
        else:
            raw_values = [form_data.get(f"answer_{qid}", "")]
        cleaned = [re.sub(r"\s+", " ", str(item).strip()) for item in raw_values if str(item).strip()]
        answers[qid] = cleaned
    return answers


@app.route("/learning/<int:learning_id>/mode", methods=["GET"])
def learning_mode(learning_id: int):
    database = db()
    learning_row = database.get_learning_by_id(learning_id)
    if not learning_row:
        flash("Learning entry not found.", "warning")
        return redirect(url_for("learning_overview"))
    parsed_learning = DocsParser().parse_learning_file(learning_row.get("path_to_learning", ""))
    draft = database.get_learning_exam_draft(learning_id)
    draft_answers = {}
    if draft:
        try:
            draft_answers = json.loads(draft.get("answers_json", "{}"))
        except json.JSONDecodeError:
            draft_answers = {}
    return render_template("learning_mode.html", learning=learning_row, parsed_learning=parsed_learning, draft_answers=draft_answers, review_attempt=None)


@app.route("/learning/<int:learning_id>/mode/save", methods=["POST"])
def learning_mode_save(learning_id: int):
    database = db()
    learning_row = database.get_learning_by_id(learning_id)
    if not learning_row:
        flash("Learning entry not found.", "warning")
        return redirect(url_for("learning_overview"))
    parsed_learning = DocsParser().parse_learning_file(learning_row.get("path_to_learning", ""))
    answers = _extract_user_answers_from_form(request.form, parsed_learning.get("questions", []))
    database.upsert_learning_exam_draft(learning_id, json.dumps(answers, ensure_ascii=False), now_in_zurich_str())
    flash("Learning progress saved.", "success")
    return redirect(url_for("learning_mode", learning_id=learning_id))


@app.route("/learning/<int:learning_id>/mode/finish", methods=["POST"])
def learning_mode_finish(learning_id: int):
    database = db()
    learning_row = database.get_learning_by_id(learning_id)
    if not learning_row:
        flash("Learning entry not found.", "warning")
        return redirect(url_for("learning_overview"))
    parsed_learning = DocsParser().parse_learning_file(learning_row.get("path_to_learning", ""))
    questions = parsed_learning.get("questions", [])
    answers_map = {str(item.get("question_id", "")): item.get("correct_answers", []) for item in parsed_learning.get("answers", []) if isinstance(item, dict)}
    user_answers = _extract_user_answers_from_form(request.form, questions)
    correct = 0
    scored_questions = 0
    for question in questions:
        if str(question.get("type", "FREETEXT")).strip().upper() == "FREETEXT":
            continue
        qid = str(question.get("id", "")).strip()
        expected = sorted([str(item).strip() for item in answers_map.get(qid, []) if str(item).strip()])
        actual = sorted([str(item).strip() for item in user_answers.get(qid, []) if str(item).strip()])
        scored_questions += 1
        if expected == actual:
            correct += 1
    attempt_id = database.create_learning_exam_attempt(
        learning_id=learning_id,
        answers_json=json.dumps(user_answers, ensure_ascii=False),
        score=float(correct),
        total_questions=scored_questions,
        created_at=now_in_zurich_str(),
    )
    database.delete_learning_exam_draft(learning_id)
    attempt = database.get_learning_exam_attempt_by_id(attempt_id)
    flash(f"Exam finished. Score: {correct}/{scored_questions}. Free-text questions are shown for review but are not scored.", "success")
    return render_template("learning_mode.html", learning=learning_row, parsed_learning=parsed_learning, draft_answers=user_answers, review_attempt=attempt)


@app.errorhandler(404)
def handle_not_found_error(error):
    logger.info("Page not found: %s", request.path)
    return (
        render_template(
            "404.html",
            error_message="The page you requested does not exist or may have been moved.",
        ),
        404,
    )


@app.errorhandler(500)
def handle_internal_server_error(error):
    logger.error("Internal server error on %s\n%s", request.path, traceback.format_exc())
    original_error = getattr(error, "original_exception", None)
    error_message = str(original_error or error or "Something went wrong while loading this page. Please try again.")
    return (
        render_template(
            "500.html",
            error_message=error_message,
        ),
        500,
    )


@app.errorhandler(Exception)
def handle_unexpected_error(error):
    if isinstance(error, HTTPException):
        return error

    logger.error("Unhandled exception on %s\n%s", request.path, traceback.format_exc())
    return (
        render_template(
            "500.html",
            error_message=str(error) or "Something went wrong while loading this page. Please try again.",
        ),
        500,
    )


@app.route("/hslu/semester_overview", methods=["GET"])
def hslu_semester_overview():
    database = db()
    parser = DocsParser()

    semester = request.args.get("semester", "").strip()
    module = request.args.get("module", "").strip()
    sw = request.args.get("sw", "").strip()
    semesters, selected_semester, modules, selected_module, selected_sw, overview_rows, standard_semester = _load_hslu_overview(parser, database, semester, module, sw)

    return render_template(
        "hslu_semester_overview.html",
        semesters=semesters,
        selected_semester=selected_semester,
        modules=modules,
        selected_module=selected_module,
        overview_rows=overview_rows,
        selected_sw=selected_sw,
        standard_semester=standard_semester,
        last_sync_time="Live markdown view",
        sw_status_options=SW_STATUS_OPTIONS,
    )




@app.route("/hslu/semester_overview/standard_semester", methods=["POST"])
def hslu_semester_overview_standard_semester():
    semester = request.form.get("semester", "").strip()

    database = db()
    semesters = sorted(
        {
            str(row.get("semester", "")).strip()
            for row in DocsParser().parse_hslu_sw_overview()
            if str(row.get("semester", "")).strip()
        },
        key=str.casefold,
    )

    if not semester or semester not in semesters:
        flash("Please select a valid semester before setting it as standard.", "warning")
        return redirect(url_for("hslu_semester_overview"))

    database.set_hslu_standard_semester(semester)
    flash(f"Standard semester set to '{semester}'.", "success")
    return redirect(url_for("hslu_semester_overview", semester=semester))

@app.route("/hslu/semester_overview/status", methods=["POST"])
def hslu_semester_overview_update_status():
    semester = request.form.get("semester", "").strip()
    module = request.form.get("module", "").strip()
    kw = request.form.get("kw", "").strip()
    sw = request.form.get("sw", "").strip()
    field = request.form.get("field", "").strip().lower()
    status = request.form.get("status", "").strip()
    sw_filter = request.form.get("sw_filter", "").strip()
    module_filter = request.form.get("module_filter", "").strip()

    if not semester or not module or not kw or not sw:
        flash("Missing row identifiers for status update.", "warning")
        return redirect(url_for("hslu_semester_overview", semester=semester, module=module_filter, sw=sw_filter))

    if field not in ("downloaded", "documented"):
        flash("Invalid status target field.", "danger")
        return redirect(url_for("hslu_semester_overview", semester=semester, module=module_filter, sw=sw_filter))

    if status not in SW_STATUS_OPTIONS:
        flash("Invalid status selection.", "danger")
        return redirect(url_for("hslu_semester_overview", semester=semester, module=module_filter, sw=sw_filter))

    try:
        parser = DocsParser()
        parser.update_hslu_sw_status(semester, module, kw, sw, field, status)
        flash(f"Updated {field} status for KW {kw} / SW {sw}.", "success")
    except SystemExit:
        flash("Failed to update markdown status. Check logs and file mapping.", "danger")

    return redirect(url_for("hslu_semester_overview", semester=semester, module=module_filter, sw=sw_filter))


@app.route("/hslu/semester_overview/sync", methods=["POST"])
def hslu_semester_overview_sync():
    semester = request.form.get("semester", "").strip()
    module = request.form.get("module", "").strip()
    sw = request.form.get("sw", "").strip()

    try:
        rows = DocsParser().parse_hslu_sw_overview()
        flash(f"Reloaded {len(rows)} semester overview rows from markdown.", "success")
    except SystemExit:
        flash("HSLU parsing failed. Check logs and folder mapping.", "danger")
    except Exception:
        logger.error("HSLU sync endpoint failed\n%s", traceback.format_exc())
        flash("HSLU parsing failed unexpectedly.", "danger")

    if semester:
        if module:
            return redirect(url_for("hslu_semester_overview", semester=semester, module=module, sw=sw))
        return redirect(url_for("hslu_semester_overview", semester=semester, sw=sw))
    return redirect(url_for("hslu_semester_overview"))


@app.route("/hslu/semester_checklist", methods=["GET"])
def hslu_semester_checklist():
    parser = DocsParser()

    semester = request.args.get("semester", "").strip()
    sw = request.args.get("sw", "").strip()
    sections = [item.strip() for item in request.args.getlist("section") if item.strip()]
    semesters, selected_semester, selected_sw, available_sections, selected_sections, checklist_rows_by_section = _load_hslu_checklist(
        parser, semester, sw, sections
    )

    return render_template(
        "hslu_semester_checklist.html",
        semesters=semesters,
        selected_semester=selected_semester,
        selected_sw=selected_sw,
        available_sections=available_sections,
        selected_sections=selected_sections,
        checklist_rows_by_section=checklist_rows_by_section,
        last_sync_time="Live markdown view",
        sw_status_options=SW_STATUS_OPTIONS,
    )


@app.route("/hslu/semester_checklist/status", methods=["POST"])
def hslu_semester_checklist_update_status():
    semester = request.form.get("semester", "").strip()
    sw_filter = request.form.get("sw_filter", "").strip()
    section_filters = [section.strip() for section in request.form.getlist("section_filters") if section.strip()]
    status = request.form.get("status", "").strip()
    target = {
        "section": request.form.get("section", "").strip(),
        "sw": request.form.get("sw", "").strip(),
        "checklist_row": request.form.get("checklist_row", "").strip(),
        "checklist_item": request.form.get("checklist_item", "").strip(),
        "file_path": request.form.get("file_path", "").strip(),
    }

    def _redirect_with_filters():
        query = urlencode({"semester": semester, "sw": sw_filter, "section": section_filters}, doseq=True)
        return redirect(f"{url_for('hslu_semester_checklist')}?{query}")

    if not target["section"] or not target["checklist_item"] or not target["file_path"]:
        flash("Missing checklist row identifier.", "warning")
        return _redirect_with_filters()

    if status not in SW_STATUS_OPTIONS:
        flash("Invalid status selection.", "danger")
        return _redirect_with_filters()

    try:
        parser = DocsParser()
        parser.update_hslu_semester_checklist_status(target, status)
        flash("Checklist status updated.", "success")
    except SystemExit:
        flash("Failed to update checklist markdown status. Check logs and file mapping.", "danger")

    return _redirect_with_filters()


@app.route("/hslu/semester_checklist/sync", methods=["POST"])
def hslu_semester_checklist_sync():
    semester = request.form.get("semester", "").strip()
    sw = request.form.get("sw", "").strip()

    try:
        rows = DocsParser().parse_hslu_semester_checklist()
        flash(f"Reloaded {len(rows)} semester checklist rows from markdown.", "success")
    except SystemExit:
        flash("Semester checklist parsing failed. Check logs and folder mapping.", "danger")
    except Exception:
        logger.error("Semester checklist sync endpoint failed\n%s", traceback.format_exc())
        flash("Semester checklist parsing failed unexpectedly.", "danger")

    return redirect(url_for("hslu_semester_checklist", semester=semester, sw=sw))

@app.template_filter("render_hslu_inline_markdown")
def render_hslu_inline_markdown_filter(value: str) -> Markup:
    return Markup(_render_hslu_inline_markdown(value))


@app.template_filter("render_ai_feedback_markdown")
def render_ai_feedback_markdown_filter(value: str) -> Markup:
    return Markup(_render_ai_feedback_markdown(value))


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
