import html
import json
import os
import re
import stat
import traceback
from urllib.parse import urlencode, urlparse
from datetime import datetime
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




def _normalize_manual_override(value: str | None) -> str:
    return "true" if str(value).strip().lower() == "true" else "false"


def _compliance_tag_class(doc: dict) -> str:
    if str(doc.get("is_under_construction", "false")).lower() == "true":
        return "text-bg-info"

    is_compliant = doc.get("is_compliant") == "true"
    manual_override = _normalize_manual_override(doc.get("manual_compliant_override")) == "true"

    if manual_override:
        return "compliance-tag-manual"
    if is_compliant:
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


def _append_todo(note: str, todo_type: str, progress: str) -> None:
    parser = DocsParser()
    todos = parser.parse_todos_from_markdown()
    todos.append(
        {
            "note": note,
            "type": json.dumps([value.strip() for value in todo_type.split("/") if value.strip()], ensure_ascii=False),
            "progress": progress,
            "last_update": _today_dd_mm(),
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
        if normalized_query and normalized_query not in str(prepared.get("note", "")).casefold():
            continue
        processed_rows.append(prepared)

    return processed_rows


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

    return semesters, selected_semester, modules, selected_module, selected_sw, filtered_rows, standard_semester



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
        deduplicated_rows.append(row)

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
        row["links_list"] = _link_map_to_items(row.get("links"))
        row["video_links_list"] = _link_map_to_items(row.get("video_links"))
        row["noncompliance_reason_list"] = _to_display_list(row.get("noncompliance_reason"))
        row["changed_at_list"] = _to_display_list(row.get("changed_at"))
        row["manual_compliant_override"] = _normalize_manual_override(row.get("manual_compliant_override"))
        row["is_under_construction"] = str(row.get("is_under_construction", "false")).lower()
        row["display_title"] = f"🚧 {row.get('title', '')}" if row["is_under_construction"] == "true" else row.get("title", "")
        if row["is_under_construction"] == "true":
            row["is_compliant"] = "Not Determined"
            row["noncompliance_reason_list"] = []
        row["compliance_tag_class"] = _compliance_tag_class(row)
        processed_docs.append(row)

    _sort_docs(processed_docs, sort_by)
    last_sync_time = database.get_last_sync_time()

    under_construction_count = len(under_construction_docs)
    manual_compliance_docs = sorted(
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
        manual_compliance_docs=manual_compliance_docs,
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


@app.route("/compliance/manual", methods=["POST"])
def set_manual_compliance():
    doc_id = request.form.get("doc_id", "").strip()
    doc_title = request.form.get("doc_title", "").strip()
    manual_override = _normalize_manual_override(request.form.get("manual_compliant_override", "false"))

    if manual_override not in ("true", "false"):
        logger.warning("Invalid manual compliance value for id=%s value=%s", doc_id, manual_override)
        flash("Manual compliance value must be true or false.", "danger")
        return redirect(url_for("index"))

    try:
        database = db()
        if not doc_id and doc_title:
            matched_docs = database.get_docs_by_name(doc_title, exact_match=True)
            if not matched_docs:
                flash(f"Document title not found: {doc_title}", "warning")
                return redirect(url_for("index"))
            doc_id = str(next(iter(matched_docs.values())).get("id", "")).strip()

        if not doc_id:
            logger.warning("Manual compliance update requested without doc_id/doc_title")
            flash("Please select a document.", "warning")
            return redirect(url_for("index"))

        database.update_manual_compliance_by_id(int(doc_id), manual_override)
        if manual_override == "true":
            flash(f"Document id={doc_id} is now manually marked as compliant.", "success")
        else:
            flash(f"Manual compliance override removed for id={doc_id}.", "success")
    except ValueError:
        logger.warning("Manual compliance update failed due to non-numeric id: %s", doc_id)
        flash("ID must be numeric.", "danger")

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


@app.route("/todo/add", methods=["POST"])
def add_todo():
    note = request.form.get("note", "").strip()
    todo_type = request.form.get("type", "").strip()
    progress = request.form.get("progress", "Not Started").strip()

    if not note or not todo_type:
        flash("Todo note and type are required.", "warning")
        return redirect(url_for("todo_overview"))

    try:
        _append_todo(note=note, todo_type=todo_type, progress=progress)
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

    scores = [
        row["score_value"]
        for row in all_feedback_rows
        if row.get("included_in_average") and row.get("score_value") is not None
    ]
    average_score = sum(scores) / len(scores) if scores else None

    return render_template(
        "ai_feedback.html",
        feedback_rows=feedback_rows,
        total_reports=len(all_feedback_rows),
        average_score=_format_feedback_score(average_score) if average_score is not None else "N/A",
        average_score_color=_feedback_score_color(average_score),
        selected_name=name_query,
        selected_score=score_query,
        available_docs=available_docs,
    )


@app.route("/ai_feedback/sync", methods=["POST"])
def ai_feedback_sync():
    try:
        parser = DocsParser()
        synced_rows = parser.sync_ai_feedback_to_db()
        flash(f"Synced {len(synced_rows)} AI feedback file(s).", "success")
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
        parser.sync_ai_feedback_to_db()
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

        parser.sync_ai_feedback_to_db()
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
        parser = DocsParser()
        parser.sync_ai_feedback_to_db()
        flash(f"AI feedback deleted successfully: {feedback_name}.", "success")
        return redirect(redirect_to)
    except Exception as exc:
        logger.error("AI feedback delete failed\n%s", traceback.format_exc())
        return render_template("500.html", error_message=str(exc)), 500


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
