import html
import json
import os
import errno
import re
import stat
import traceback
from urllib.parse import urlencode, urlparse
from datetime import date, datetime, timedelta
from pathlib import Path

from flask import Flask, flash, jsonify, redirect, render_template, request, send_file, send_from_directory, session, url_for
from markupsafe import Markup
from werkzeug.exceptions import HTTPException

from src.DatabaseConnector import db
from src.DocsAIFeedback import DocsAIFeedback, OpenRouterImageNotSupportedError
from src.DocsParser import DocsParser
from src.DocsPlaybook import DocsPlaybook, PlaybookValidationError
from src.DocsVersionHandler import DocsVersionHandler
from src.DocsWriter import DocsWriter
from src.DocsExporter import DocsExporter
from src.DocsViewer import DocsViewer
from src.logger import get_logger
from src.timezone_utils import now_in_zurich, now_in_zurich_str


app = Flask(__name__)
app.secret_key = "knowledge-mapping-secret"

logger = get_logger(__name__)


SW_STATUS_OPTIONS = ["", "Not Started", "In Progress", "Done", "Not Needed"]
TODO_PROGRESS_OPTIONS = ["Not Started", "In Progress", "Done"]
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


def _normalize_todo_progress(value: str | None, *, allow_empty: bool = False) -> str:
    normalized = str(value or "").strip()
    if allow_empty and not normalized:
        return ""
    if normalized not in TODO_PROGRESS_OPTIONS:
        raise ValueError(
            f"Todo progress must be one of: {', '.join(TODO_PROGRESS_OPTIONS)}."
        )
    return normalized


def _parse_todo_last_update(value: str | None, reference_date: date | None = None) -> date | None:
    raw_value = str(value or "").strip()
    if not raw_value:
        return None

    today = reference_date or now_in_zurich().date()
    for value_format in ("%d.%m.%Y", "%d.%m"):
        try:
            parsed = datetime.strptime(raw_value, value_format).date()
            if value_format == "%d.%m":
                parsed = parsed.replace(year=today.year)
                if parsed > today:
                    parsed = parsed.replace(year=today.year - 1)
            return parsed
        except ValueError:
            continue

    return None


def _todo_last_update_is_stale(todo: dict, reference_date: date | None = None) -> bool:
    priority_to_threshold = {
        "High": 5,
        "Medium": 10,
        "Low": 15,
    }
    priority = _normalize_todo_priority(todo.get("priority"))
    threshold_days = priority_to_threshold.get(priority, 0)

    parsed_last_update = _parse_todo_last_update(todo.get("last_update"), reference_date=reference_date)
    if parsed_last_update is None:
        return True

    today = reference_date or now_in_zurich().date()
    days_since_update = (today - parsed_last_update).days
    if days_since_update < 0:
        days_since_update = 0
    return days_since_update > threshold_days


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




def _openrouter_media_support(conf: dict) -> dict[str, bool]:
    modality_aliases: dict[str, set[str]] = {
        "text": {"text"},
        "image": {"image"},
        "file": {"file", "document", "pdf"},
        "audio": {"audio", "input_audio"},
        "video": {"video"},
    }
    supported_modalities: set[str] = set()

    try:
        supported_modalities = DocsAIFeedback(conf).fetch_openrouter_input_modalities()
    except Exception:
        logger.warning("Unable to fetch OpenRouter input modalities for settings page.\n%s", traceback.format_exc())

    normalized_modalities = {str(modality).strip().lower() for modality in supported_modalities if str(modality).strip()}

    return {
        key: any(alias in normalized_modalities for alias in aliases)
        for key, aliases in modality_aliases.items()
    }

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


def _save_conf(conf_data: dict) -> None:
    conf_path = Path(__file__).resolve().parent / "conf.json"
    conf_path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = conf_path.with_suffix(".json.tmp")
    serialized = json.dumps(conf_data, indent=4)
    payload = f"{serialized}\n"

    try:
        with open(temp_path, "w", encoding="utf-8") as conf_file:
            conf_file.write(payload)
            conf_file.flush()
            os.fsync(conf_file.fileno())
        os.replace(temp_path, conf_path)
        return
    except OSError as exc:
        if exc.errno not in {errno.EBUSY, errno.EXDEV, errno.EPERM, errno.EACCES}:
            raise
    finally:
        if temp_path.exists():
            temp_path.unlink(missing_ok=True)

    # Docker bind mounts can reject atomic replacement even after the temp file is written.
    with open(conf_path, "r+", encoding="utf-8") as conf_file:
        conf_file.seek(0)
        conf_file.write(payload)
        conf_file.truncate()
        conf_file.flush()
        os.fsync(conf_file.fileno())


def _sanitize_conf_text(value: str | None, field_name: str, max_length: int = 500) -> str:
    sanitized = str(value or "").strip()
    if not sanitized:
        raise ValueError(f"{field_name} is required.")
    if len(sanitized) > max_length:
        raise ValueError(f"{field_name} is too long.")
    if any(char in sanitized for char in ("\x00", "\r", "\n")):
        raise ValueError(f"{field_name} contains invalid characters.")
    return sanitized


def _parse_provider_list(raw_value: str | None) -> list[str]:
    candidate = str(raw_value or "").strip()
    if not candidate:
        raise ValueError("Openrouter Provider is required.")

    parts = [item.strip() for item in re.split(r"[\n,]+", candidate) if item.strip()]
    normalized = []
    for provider in parts:
        if len(provider) > 100:
            raise ValueError("Openrouter Provider values must be 100 characters or fewer.")
        if any(char in provider for char in ("\x00", "\r", "\n")):
            raise ValueError("Openrouter Provider contains invalid characters.")
        if provider not in normalized:
            normalized.append(provider)

    if not normalized:
        raise ValueError("Openrouter Provider is required.")
    return normalized


def _parse_checkbox_bool(raw_value: str | None) -> bool:
    return str(raw_value or "").strip().casefold() in {"1", "true", "on", "yes"}


def _sanitize_non_negative_int(value: str | None, field_name: str, minimum: int = 0, maximum: int = 1000000) -> int:
    raw = str(value or "").strip()
    if not raw:
        raise ValueError(f"{field_name} is required.")
    if not re.fullmatch(r"\d+", raw):
        raise ValueError(f"{field_name} must be a whole number.")
    parsed = int(raw)
    if parsed < minimum:
        raise ValueError(f"{field_name} must be at least {minimum}.")
    if parsed > maximum:
        raise ValueError(f"{field_name} is too large.")
    return parsed


def _parse_multiline_conf_strings(raw_value: str | None, field_name: str, max_items: int = 50) -> list[str]:
    values = [line.strip() for line in str(raw_value or "").splitlines() if line.strip()]
    if not values:
        raise ValueError(f"{field_name} requires at least one value.")
    if len(values) > max_items:
        raise ValueError(f"{field_name} allows at most {max_items} values.")
    cleaned: list[str] = []
    for value in values:
        if len(value) > 200:
            raise ValueError(f"{field_name} entries must be 200 characters or fewer.")
        if any(char in value for char in ("\x00", "\r")):
            raise ValueError(f"{field_name} entries contain invalid characters.")
        cleaned.append(value)
    return cleaned


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


def _docs_root_path_from_conf(conf: dict | None = None) -> Path:
    loaded_conf = conf if isinstance(conf, dict) else _load_conf()
    return Path(loaded_conf.get("docs", {}).get("full_path_to_docs", "")).resolve()


def _docs_note_exists(note_name: str, conf: dict | None = None) -> bool:
    normalized_doc = _normalize_md_filename(note_name)
    if not normalized_doc:
        return False

    docs_dir = _docs_root_path_from_conf(conf)
    target_path = (docs_dir / normalized_doc).resolve()
    return docs_dir in target_path.parents and target_path.exists() and target_path.is_file()


def _list_existing_doc_note_names(conf: dict | None = None) -> list[str]:
    docs_dir = _docs_root_path_from_conf(conf)
    if not docs_dir.exists() or not docs_dir.is_dir():
        return []

    note_names = {
        doc_path.stem.strip()
        for doc_path in docs_dir.rglob("*.md")
        if doc_path.stem.strip()
    }
    return sorted(note_names, key=lambda value: value.casefold())




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


def _apply_doc_template(
    template_key: str,
    file_name: str,
    *,
    reason: str = "",
    create_history: bool = False,
    auto_create_history_if_missing: bool = False,
) -> dict:
    normalized_template_key = str(template_key or "").strip().lower()
    if normalized_template_key not in {"new", "update"}:
        raise ValueError("Invalid template action request.")

    normalized_file_name = _normalize_md_filename(file_name)
    if not normalized_file_name:
        raise ValueError("Invalid file name. Please use a valid markdown file name.")

    template_options = _load_template_options()
    template_path = template_options.get(normalized_template_key)
    if template_path is None or not template_path.exists():
        raise FileNotFoundError("Template file not found. Please verify templates in /the-knowledge/03_TEMPLATES.")

    conf = _load_conf()
    docs_dir = _docs_root_path_from_conf(conf)
    writer = DocsWriter(conf.get("todo", {}).get("full_path_to_todo_file", ""))
    target_path = (docs_dir / normalized_file_name).resolve()

    if docs_dir not in target_path.parents:
        raise ValueError("Invalid note path.")

    try:
        template_content = _render_doc_template(template_path.read_text(encoding="utf-8"))
    except OSError as exc:
        raise RuntimeError("Failed to read template file.") from exc

    if normalized_template_key == "new":
        if target_path.exists():
            return {"status": "exists", "path": target_path, "file_name": normalized_file_name}

        writer.create_note_from_template(target_path, template_content)
        _set_rw_permissions_for_all_users(target_path)
        return {"status": "created", "path": target_path, "file_name": normalized_file_name}

    normalized_reason = _normalize_update_reason(reason)
    if not normalized_reason:
        raise ValueError("Reason is required for update template.")

    if not target_path.exists():
        raise FileNotFoundError("Note file not found in 02_DOCS. Please provide an existing file name.")

    success, missing_sections = writer.prepend_template_to_existing_note(
        target_path=target_path,
        template_content=template_content,
        reason=normalized_reason,
        create_history=create_history,
    )
    if not success and auto_create_history_if_missing and "#### Page History" in missing_sections:
        success, missing_sections = writer.prepend_template_to_existing_note(
            target_path=target_path,
            template_content=template_content,
            reason=normalized_reason,
            create_history=True,
        )

    if not success:
        if "#### Page History" in missing_sections:
            raise ValueError("'#### Page History' not found in the target note.")
        raise RuntimeError("Failed to update note from template.")

    _set_rw_permissions_for_all_users(target_path)
    return {"status": "updated", "path": target_path, "file_name": normalized_file_name}


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


def _validate_note_name(note_name: str) -> str:
    normalized = str(note_name or "").strip()
    if not normalized:
        raise ValueError("note_name is required.")
    if "/" in normalized or "\\" in normalized:
        raise ValueError("note_name must not include path separators.")
    return normalized


def _find_todo_index_by_note_name(todos: list[dict], note_name: str) -> int:
    target_key = Path(str(note_name).strip()).stem.strip().casefold()
    if not target_key:
        return -1
    for index, todo in enumerate(todos):
        todo_key = Path(str(todo.get("note", "")).strip()).stem.strip().casefold()
        if todo_key == target_key:
            return index
    return -1


def _find_todo_index_by_id(todos: list[dict], todo_id: str) -> int:
    if not str(todo_id).strip().isdigit():
        return -1
    target_id = str(int(str(todo_id).strip()))
    for index, _todo in enumerate(todos, start=1):
        if str(index) == target_id:
            return index - 1
    return -1


def _update_todo_entry(
    *,
    todo_id: str | None = None,
    note_name: str | None = None,
    progress: str | None = None,
    priority: str | None = None,
) -> bool:
    parser = DocsParser()
    current_todos = parser.parse_todos_from_markdown()

    todo_index = -1
    if todo_id is not None and str(todo_id).strip():
        todo_index = _find_todo_index_by_id(current_todos, str(todo_id))
    elif note_name is not None and str(note_name).strip():
        todo_index = _find_todo_index_by_note_name(current_todos, str(note_name))

    if todo_index == -1:
        return False

    todo = current_todos[todo_index]
    changed = False

    if priority is not None and str(priority).strip():
        todo["priority"] = _normalize_todo_priority(priority)
        changed = True

    if progress is not None:
        normalized_progress = _normalize_todo_progress(progress, allow_empty=True)
        if normalized_progress:
            todo["progress"] = normalized_progress
            changed = True

    if not changed:
        return True

    todo["last_update"] = _today_dd_mm()
    conf = _load_conf()
    writer = DocsWriter(conf.get("todo", {}).get("full_path_to_todo_file", ""))
    writer.write_todos_table(current_todos)
    return True


def _normalize_update_reason(raw_reason: str) -> str:
    cleaned = re.sub(r"[\x00-\x1f\x7f]", " ", str(raw_reason or ""))
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned[:300]


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
    today = now_in_zurich().date()

    for index, row in enumerate(todos, start=1):
        prepared = dict(row)
        prepared["id"] = index
        prepared["type_list"] = _normalize_todo_types(prepared.get("type"))
        prepared["priority"] = _normalize_todo_priority(prepared.get("priority"))
        prepared["last_update_is_stale"] = _todo_last_update_is_stale(prepared, reference_date=today)
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


def _load_latest_feedback_context(database: db, parser: DocsParser, selected_doc_note_name: str) -> dict | None:
    latest_feedback_for_context = database.get_latest_ai_feedback_for_file(selected_doc_note_name)
    if not latest_feedback_for_context or not latest_feedback_for_context.get("path_to_feedback"):
        return None

    parsed_latest_feedback = parser.parse_ai_feedback_file(latest_feedback_for_context["path_to_feedback"])
    return {
        "version": parsed_latest_feedback.get("version"),
        "score": parsed_latest_feedback.get("score"),
        "creation_date": parsed_latest_feedback.get("creation_date"),
        "feedback": parsed_latest_feedback.get("feedback"),
    }


def _create_ai_feedback_for_document(selected_doc: str, include_images: bool) -> dict:
    conf = _load_conf()
    database = db()
    _ensure_doc_can_receive_ai_feedback(database, selected_doc)
    parser = DocsParser()
    _sync_ai_feedback_and_openrouter_credits(database)
    ai_feedback_service = DocsAIFeedback(conf)
    selected_doc_note_name = Path(selected_doc).stem.strip()
    latest_feedback_context = _load_latest_feedback_context(database, parser, selected_doc_note_name)

    feedback_payload = ai_feedback_service.generate_feedback(
        selected_doc,
        previous_feedback=latest_feedback_context,
        include_images=include_images,
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
    return feedback_payload


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
        prepared["doc_preview_url"] = _ai_feedback_doc_preview_url(prepared)
        prepared["title_icon"] = "✅" if prepared["doc_preview_url"] else "❓"

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


def _load_playbooks_conf(conf_data: dict) -> dict:
    defaults = {
        "enabled": True,
        "path": "/the-knowledge/08_PLAYBOOKS",
        "max_depth": 30,
        "dry_run": False,
    }
    raw = conf_data.get("playbooks", {}) if isinstance(conf_data.get("playbooks", {}), dict) else {}
    return {
        "enabled": bool(raw.get("enabled", defaults["enabled"])),
        "path": str(raw.get("path", defaults["path"])).strip() or defaults["path"],
        "max_depth": max(1, int(raw.get("max_depth", defaults["max_depth"]))),
        "dry_run": bool(raw.get("dry_run", defaults["dry_run"])),
    }


def _append_deadline(name: str, date_label: str, time_label: str, status: str) -> None:
    parser = DocsParser()
    current_deadlines = parser.parse_deadlines_from_markdown(include_description=True)
    current_deadlines.append(
        {
            "name": str(name).strip(),
            "description": "Created by Playbook",
            "date": str(date_label).strip(),
            "time": str(time_label).strip(),
            "status": str(status).strip() if str(status).strip() in DEADLINE_STATUS_OPTIONS else "Not Started",
        }
    )
    writer = DocsWriter(deadlines_file_path=_load_conf().get("deadlines", {}).get("full_path_to_deadlines_file", ""))
    writer.write_deadlines_table(current_deadlines)


def _perform_full_scan() -> None:
    logger.info("UI requested full scan")
    parser = DocsParser()
    parser.parse_and_add_ALL_docs_to_db()
    logger.info("UI full scan completed")


def _playbook_action_handlers() -> dict:
    def _resolve_note_path_for_playbook(note_name: str) -> Path:
        parser = DocsParser()
        return parser.find_note_path(_validate_note_name(note_name))

    def _upsert_todo(note_name: str, todo_type: str, progress: str, priority: str, create_if_missing: bool) -> str:
        parser = DocsParser()
        todos = parser.parse_todos_from_markdown()
        matched_index = _find_todo_index_by_note_name(todos, note_name)
        progress = _normalize_todo_progress(progress)
        normalized_priority = _normalize_todo_priority(priority)

        if matched_index == -1:
            if not create_if_missing:
                raise ValueError("Todo not found for note_name.")
            todos.append(
                {
                    "note": note_name,
                    "type": json.dumps([value.strip() for value in todo_type.split("/") if value.strip()], ensure_ascii=False),
                    "progress": progress,
                    "last_update": _today_dd_mm(),
                    "priority": normalized_priority,
                }
            )
            result_status = "created"
        else:
            current = todos[matched_index]
            current["note"] = note_name
            current["type"] = json.dumps([value.strip() for value in todo_type.split("/") if value.strip()], ensure_ascii=False)
            current["progress"] = progress
            current["priority"] = normalized_priority
            current["last_update"] = _today_dd_mm()
            result_status = "updated"

        conf = _load_conf()
        writer = DocsWriter(conf.get("todo", {}).get("full_path_to_todo_file", ""))
        writer.write_todos_table(todos)
        return result_status

    def _action_create_note(action_input: dict, _: dict) -> dict:
        note_name = str(action_input.get("note_name", "")).strip()
        logger.info("Playbook handler create_note called: note_name=%s", note_name)
        if not note_name:
            raise ValueError("create_note requires note_name.")
        result = _apply_doc_template(template_key="new", file_name=note_name)
        return {
            "status": result["status"],
            "note_name": Path(result["file_name"]).stem,
        }

    def _action_update_note(action_input: dict, _: dict) -> dict:
        note_name = str(action_input.get("note_name", "")).strip()
        reason = str(action_input.get("reason", "")).strip()
        logger.info("Playbook handler update_note called: note_name=%s", note_name)
        if not note_name or not reason:
            raise ValueError("update_note requires note_name and reason.")
        result = _apply_doc_template(
            template_key="update",
            file_name=note_name,
            reason=reason,
            auto_create_history_if_missing=True,
        )
        return {
            "status": result["status"],
            "note_name": Path(result["file_name"]).stem,
        }

    def _action_create_learning(action_input: dict, _: dict) -> dict:
        note_name = str(action_input.get("note_name", "")).strip()
        logger.info("Playbook handler create_learning called: note_name=%s", note_name)
        if not note_name:
            raise ValueError("create_learning requires note_name.")
        normalized_doc = _normalize_md_filename(note_name)
        if not normalized_doc:
            raise ValueError("create_learning requires a valid note_name.")
        _create_learning_for_doc(normalized_doc)
        return {"status": "created"}

    def _action_generate_ai_questions(action_input: dict, _: dict) -> dict:
        note_name = str(action_input.get("note_name", "")).strip()
        logger.info("Playbook handler generate_ai_questions called: note_name=%s", note_name)
        if not note_name:
            raise ValueError("generate_ai_questions requires note_name.")
        normalized_doc = _normalize_md_filename(note_name)
        if not normalized_doc:
            raise ValueError("generate_ai_questions requires a valid note_name.")
        generated = _generate_learning_questions_for_doc(normalized_doc)
        return {
            "status": "generated",
            "learning_name": str(generated.get("learning_row", {}).get("file_name", "")).strip(),
            "questions_count": int(generated.get("questions_count", 0)),
        }

    def _action_generate_ai_feedback(action_input: dict, _: dict) -> dict:
        note_name = str(action_input.get("note_name", "")).strip()
        logger.info("Playbook handler generate_ai_feedback called: note_name=%s", note_name)
        if not note_name:
            raise ValueError("generate_ai_feedback requires note_name.")
        feedback_payload = _create_ai_feedback_for_document(note_name, include_images=False)
        return {"status": "created", "ai_feedback_score": feedback_payload.get("score")}

    def _action_create_todo(action_input: dict, _: dict) -> dict:
        note_name = _validate_note_name(action_input.get("note_name", ""))
        todo_type = str(action_input.get("type", "Update")).strip() or "Update"
        progress = str(action_input.get("progress", "Not Started")).strip()
        priority = _normalize_todo_priority(str(action_input.get("priority", "Medium")))
        logger.info(
            "Playbook handler create_todo called: note_name=%s type=%s progress=%s priority=%s",
            note_name,
            todo_type,
            progress,
            priority,
        )
        status = _upsert_todo(
            note_name=note_name,
            todo_type=todo_type,
            progress=progress,
            priority=priority,
            create_if_missing=True,
        )
        return {"status": status}

    def _action_update_todo(action_input: dict, _: dict) -> dict:
        note_name = _validate_note_name(action_input.get("note_name", ""))
        raw_priority = str(action_input.get("priority", "")).strip()
        raw_progress = action_input.get("progress")
        progress = str(raw_progress).strip() if raw_progress is not None else ""
        logger.info(
            "Playbook handler update_todo called: note_name=%s progress=%s priority=%s",
            note_name,
            progress,
            raw_priority,
        )

        updated = _update_todo_entry(
            note_name=note_name,
            progress=progress if raw_progress is not None else None,
            priority=raw_priority if raw_priority else None,
        )
        if not updated:
            raise ValueError("Todo not found for note_name.")
        return {"status": "updated"}

    def _action_delete_todo(action_input: dict, _: dict) -> dict:
        note_name = _validate_note_name(action_input.get("note_name", ""))
        logger.info("Playbook handler delete_todo called: note_name=%s", note_name)

        parser = DocsParser()
        todos = parser.parse_todos_from_markdown()
        todo_index = _find_todo_index_by_note_name(todos, note_name)
        if todo_index == -1:
            raise ValueError("Todo not found for note_name.")
        del todos[todo_index]

        conf = _load_conf()
        writer = DocsWriter(conf.get("todo", {}).get("full_path_to_todo_file", ""))
        writer.write_todos_table(todos)
        return {"status": "deleted"}

    def _action_add_note_tags(action_input: dict, _: dict) -> dict:
        note_name = _validate_note_name(action_input.get("note_name", ""))
        tag_list = action_input.get("tag_list", [])
        if isinstance(tag_list, str):
            raw_tag_list = tag_list.strip()
            if raw_tag_list.startswith("[") and raw_tag_list.endswith("]"):
                try:
                    parsed_tag_list = json.loads(raw_tag_list)
                    tag_list = parsed_tag_list if isinstance(parsed_tag_list, list) else []
                except json.JSONDecodeError as exc:
                    raise ValueError("add_note_tags tag_list must be valid JSON array.") from exc
            elif raw_tag_list:
                tag_list = [entry.strip() for entry in raw_tag_list.splitlines() if entry.strip()]
            else:
                tag_list = []
        if not isinstance(tag_list, list):
            raise ValueError("add_note_tags requires tag_list as array.")
        normalized_tags: list[str] = []
        for raw_tag in tag_list:
            tag = str(raw_tag or "").strip()
            if not tag:
                continue
            if not tag.startswith("#"):
                raise ValueError("Each tag in tag_list must start with '#'.")
            if not re.match(r"^#[-\w]+$", tag):
                raise ValueError("Tags may only contain letters, numbers, underscores, and dashes.")
            if tag not in normalized_tags:
                normalized_tags.append(tag)
        if not normalized_tags:
            raise ValueError("add_note_tags requires at least one valid tag.")

        logger.info("Playbook handler add_note_tags called: note_name=%s tags=%s", note_name, normalized_tags)
        note_path = _resolve_note_path_for_playbook(note_name)
        writer = DocsWriter()
        success, missing_sections = writer.add_tags_to_note(doc_path=note_path, tags_to_add=normalized_tags)
        if not success:
            raise ValueError(f"Missing chapter(s): {', '.join(missing_sections)}")
        return {"status": "updated", "tags_added": len(normalized_tags)}

    def _action_create_deadline(action_input: dict, _: dict) -> dict:
        name = str(action_input.get("deadline_name", "")).strip()
        days_in_advance_raw = str(action_input.get("days_in_advance", "0")).strip() or "0"
        hours_in_advance_raw = str(action_input.get("hours_in_advance", "0")).strip() or "0"
        status = str(action_input.get("status", "Not Started")).strip()
        logger.info(
            "Playbook handler create_deadline called: deadline_name=%s days_in_advance=%s hours_in_advance=%s status=%s",
            name,
            days_in_advance_raw,
            hours_in_advance_raw,
            status,
        )
        if not name:
            raise ValueError("create_deadline requires deadline_name.")
        try:
            days_in_advance = int(days_in_advance_raw)
            hours_in_advance = int(hours_in_advance_raw)
        except ValueError as exc:
            raise ValueError("create_deadline requires numeric days_in_advance and hours_in_advance.") from exc
        if days_in_advance < 0 or hours_in_advance < 0:
            raise ValueError("create_deadline requires non-negative days_in_advance and hours_in_advance.")
        due_at = now_in_zurich() + timedelta(days=days_in_advance, hours=hours_in_advance)
        date_label = due_at.strftime("%d.%m.%Y")
        time_label = due_at.strftime("%H:%M")
        _append_deadline(name=name, date_label=date_label, time_label=time_label, status=status)
        return {"status": "created"}

    def _action_inform_user(action_input: dict, _: dict) -> dict:
        message = str(action_input.get("message", "")).strip()
        user_choice = str(action_input.get("user_response", action_input.get("decision", ""))).strip().casefold()
        logger.info("Playbook handler inform_user called: message_length=%s choice=%s", len(message), user_choice)
        if not message:
            raise ValueError("inform_user requires a message.")
        if user_choice in {"abort", "no", "cancel", "false", "0"}:
            return {"status": "aborted", "prompt_message": message, "control": "abort"}
        if user_choice in {"confirm", "yes", "continue", "ok", "true", "1"}:
            return {"status": "confirmed", "prompt_message": message}
        return {
            "status": "awaiting_user_response",
            "prompt_message": message,
            "control": "pause",
        }

    def _action_perform_note_sync(_: dict, __: dict) -> dict:
        logger.info("Playbook handler perform_note_sync called")
        _perform_full_scan()
        return {"status": "synced"}

    def _action_check_note_exists(action_input: dict, _: dict) -> dict:
        note_name = _validate_note_name(action_input.get("note_name", ""))
        logger.info("Playbook handler check_note_exists called: note_name=%s", note_name)
        parser = DocsParser()
        exists = False
        try:
            parser.find_note_path(note_name)
            exists = True
        except FileNotFoundError:
            exists = False
        return {"status": "checked", "check_note_exists": exists}

    def _action_check_todo_exists(action_input: dict, _: dict) -> dict:
        note_name = _validate_note_name(action_input.get("note_name", ""))
        logger.info("Playbook handler check_todo_exists called: note_name=%s", note_name)
        parser = DocsParser()
        todos = parser.parse_todos_from_markdown()
        todo_exists = _find_todo_index_by_note_name(todos, note_name) != -1
        return {"status": "checked", "check_todo_exists": todo_exists}

    def _action_check_note_compliant(action_input: dict, _: dict) -> dict:
        note_name = _validate_note_name(action_input.get("note_name", ""))
        logger.info("Playbook handler check_note_compliant called: note_name=%s", note_name)
        note_key = Path(note_name).stem.strip().casefold()
        note_doc = next(
            (
                item
                for item in db().get_all_docs().values()
                if Path(str(item.get("title", "")).strip()).stem.strip().casefold() == note_key
            ),
            None,
        )
        is_compliant = False
        if note_doc:
            is_flagged_under_construction = str(note_doc.get("is_under_construction", "false")).strip().casefold() == "true"
            is_flagged_compliant = str(note_doc.get("is_compliant", "false")).strip().casefold() == "true"
            is_compliant = is_flagged_compliant and not is_flagged_under_construction
        return {"status": "checked", "check_note_compliant": is_compliant}

    def _action_check_ai_feedback_min_score(_: dict, __: dict) -> dict:
        logger.info("Playbook handler check_ai_feedback_min_score called")
        conf = _load_conf()
        compliance_conf = conf.get("compliance_check", {}) if isinstance(conf.get("compliance_check", {}), dict) else {}
        if not compliance_conf:
            compliance_conf = conf.get("conpliance_check", {}) if isinstance(conf.get("conpliance_check", {}), dict) else {}
        ai_feedback_conf = compliance_conf.get("ai_feedback", {}) if isinstance(compliance_conf.get("ai_feedback", {}), dict) else {}
        minimum_score = ai_feedback_conf.get("min", DocsParser.DEFAULT_COMPLIANCE_CHECK["ai_feedback"]["min"])
        try:
            minimum_score = int(minimum_score)
        except (TypeError, ValueError):
            minimum_score = int(DocsParser.DEFAULT_COMPLIANCE_CHECK["ai_feedback"]["min"])
        return {"status": "checked", "check_ai_feedback_min_score": minimum_score}

    return {
        "create_note": _action_create_note,
        "update_note": _action_update_note,
        "create_learning": _action_create_learning,
        "generate_ai_questions": _action_generate_ai_questions,
        "generate_ai_feedback": _action_generate_ai_feedback,
        "add_note_tags": _action_add_note_tags,
        "create_todo": _action_create_todo,
        "update_todo": _action_update_todo,
        "delete_todo": _action_delete_todo,
        "create_deadline": _action_create_deadline,
        "inform_user": _action_inform_user,
        "perform_note_sync": _action_perform_note_sync,
        "check_note_exists": _action_check_note_exists,
        "check_todo_exists": _action_check_todo_exists,
        "check_note_compliant": _action_check_note_compliant,
        "check_ai_feedback_min_score": _action_check_ai_feedback_min_score,
    }


def _playbook_service() -> DocsPlaybook:
    conf = _load_conf()
    conf["playbooks"] = _load_playbooks_conf(conf)
    return DocsPlaybook(conf=conf, action_handlers=_playbook_action_handlers())


def _doc_addon_flag_enabled(value: object) -> bool:
    return str(value or "").strip().casefold() == "true"


def _find_learning_for_doc(database: db, doc_title: str) -> dict | None:
    normalized_doc = _normalize_md_filename(doc_title)
    if not normalized_doc:
        return None

    doc_stem = Path(normalized_doc).stem.strip()
    if not doc_stem:
        return None

    doc_key = doc_stem.casefold()
    matching_rows = [
        row
        for row in database.get_all_learnings()
        if str(row.get("source_note_name", "")).strip().replace(".md", "").casefold() == doc_key
        or str(row.get("file_name", "")).strip().replace(" - Learning", "").casefold() == doc_key
    ]
    if not matching_rows:
        return None

    return max(matching_rows, key=lambda row: int(row.get("id", 0)))


def _find_latest_ai_feedback_for_doc(database: db, doc_title: str) -> dict | None:
    normalized_doc = _normalize_md_filename(doc_title)
    if not normalized_doc:
        return None

    doc_stem = Path(normalized_doc).stem.strip()
    if not doc_stem:
        return None

    doc_key = doc_stem.casefold()
    matching_rows = [
        row
        for row in database.get_all_ai_feedback()
        if Path(str(row.get("file_name", "")).strip()).stem.casefold() == doc_key
    ]
    if not matching_rows:
        return None

    return max(
        matching_rows,
        key=lambda row: (int(row.get("version", 0)), int(row.get("id", 0))),
    )


def _create_learning_for_doc(normalized_doc: str) -> dict:
    conf = _load_conf()
    learning_conf = _load_learning_conf(conf)
    docs_dir = Path(conf.get("docs", {}).get("full_path_to_docs", "")).resolve()
    selected_doc_path = (docs_dir / normalized_doc).resolve()
    if docs_dir not in selected_doc_path.parents or not selected_doc_path.exists():
        raise ValueError("Selected markdown note does not exist.")

    template_path = Path(learning_conf.get("learning_template_path", "/the-knowledge/03_TEMPLATES/2 - New Learning")).resolve()
    output_dir = Path(learning_conf.get("learning_path", "/the-knowledge/07_LEARNINGS")).resolve()
    template_content = template_path.read_text(encoding="utf-8")
    writer = DocsWriter()
    note_name = selected_doc_path.stem.strip()
    rendered = writer.render_learning_template(
        template_content=template_content,
        note_name=note_name,
        creation_date=_today_dd_mm_yyyy(),
        last_modified_date="N/A",
        questions_payload={"questions": []},
        answers_payload={"answers": []},
    )
    learning_path = writer.write_learning_file(output_dir=output_dir, note_name=note_name, rendered_content=rendered)
    _set_rw_permissions_for_all_users(learning_path)
    DocsParser().sync_learning_to_db()

    learning_row = _find_learning_for_doc(db(), normalized_doc)
    if not learning_row:
        raise RuntimeError("Learning file was created but could not be loaded from the database.")

    return learning_row


def _generate_learning_questions_for_doc(normalized_doc: str) -> dict:
    conf = _load_conf()
    database = db()

    learning_row = _find_learning_for_doc(database, normalized_doc)
    if not learning_row:
        learning_row = _create_learning_for_doc(normalized_doc)

    learning_conf = _load_learning_conf(conf)
    docs_dir = Path(conf.get("docs", {}).get("full_path_to_docs", "")).resolve()
    source_note_name = str(learning_row.get("source_note_name", "")).strip() or Path(normalized_doc).stem.strip()
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
    if not questions:
        raise ValueError(
            "Question generation returned no valid questions. Please review the prompt/model output and try again."
        )
    answers = _sanitize_learning_answers(generated.get("answers", []), {item["id"] for item in questions})
    DocsWriter().update_learning_file_questions_answers(
        learning_path=Path(learning_row["path_to_learning"]),
        last_modified_date=_today_dd_mm_yyyy(),
        questions_payload={"questions": questions},
        answers_payload={"answers": answers},
    )
    _sync_openrouter_credits_only(database)
    return {
        "learning_row": learning_row,
        "questions_count": len(questions),
    }


def _learning_status_icon(learning_row: dict) -> str:
    source_note_name = str(learning_row.get("source_note_name", "")).strip()
    if source_note_name and not _docs_note_exists(source_note_name):
        return "❓"

    learning_path = str(learning_row.get("path_to_learning", "")).strip()
    if not learning_path:
        return "⚠️"

    try:
        parsed_learning = DocsParser().parse_learning_file(learning_path)
    except Exception:
        logger.warning("Could not parse learning for status display: %s", learning_path)
        return "⚠️"

    questions = parsed_learning.get("questions", [])
    answers = parsed_learning.get("answers", [])
    question_count = len(questions) if isinstance(questions, list) else 0
    answer_count = len(answers) if isinstance(answers, list) else 0

    if question_count >= 1 and answer_count >= 1:
        return "✅"
    return "⚠️"


def _learning_doc_preview_url(learning_row: dict) -> str | None:
    source_note_name = str(learning_row.get("source_note_name", "")).strip()
    return _doc_preview_url_from_note_name(source_note_name)


def _doc_preview_url_from_note_name(note_name: str) -> str | None:
    normalized_doc = _normalize_md_filename(note_name)
    if not normalized_doc:
        return None
    if not _docs_note_exists(normalized_doc):
        return None

    return url_for("view_doc_by_path", relative_path=normalized_doc)


def _ai_feedback_doc_preview_url(feedback_row: dict) -> str | None:
    return _doc_preview_url_from_note_name(str(feedback_row.get("file_name", "")).strip())


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
        row["has_learning"] = _doc_addon_flag_enabled(row.get("has_learning", "false"))
        row["has_ai_feedback"] = _doc_addon_flag_enabled(row.get("has_ai_feedback", "false"))
        row["learning"] = _find_learning_for_doc(database, str(row.get("title", "")).strip()) if row["has_learning"] else None
        row["latest_ai_feedback"] = (
            _find_latest_ai_feedback_for_doc(database, str(row.get("title", "")).strip()) if row["has_ai_feedback"] else None
        )
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
    try:
        playbooks = _playbook_service().list_playbooks()
    except Exception:
        playbooks = []

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
        playbooks=playbooks,
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


@app.route("/playbooks", methods=["GET"])
def playbooks_page():
    service = _playbook_service()
    if not service.enabled:
        flash("Playbooks are disabled in configuration.", "warning")
    playbooks = service.list_playbooks() if service.enabled else []
    return render_template("playbooks.html", playbooks=playbooks)


@app.route("/api/playbooks", methods=["GET"])
def api_list_playbooks():
    service = _playbook_service()
    return jsonify({"ok": True, "playbooks": service.list_playbooks()})


@app.route("/api/playbooks/<name>", methods=["GET"])
def api_get_playbook(name: str):
    try:
        item = _playbook_service().get_playbook(name)
        return jsonify({"ok": True, "playbook": item})
    except FileNotFoundError:
        return jsonify({"ok": False, "error": "Playbook not found."}), 404
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 400


@app.route("/api/playbooks/validate", methods=["POST"])
def api_validate_playbook():
    payload = request.get_json(silent=True) or {}
    try:
        validated = _playbook_service().validate_schema(payload)
        return jsonify({"ok": True, "validated": validated})
    except PlaybookValidationError as exc:
        return jsonify({"ok": False, "error": str(exc)}), 400


@app.route("/api/playbooks", methods=["POST"])
def api_save_playbook():
    payload = request.get_json(silent=True) or {}
    try:
        saved = _playbook_service().save_playbook(payload)
        return jsonify({"ok": True, "playbook": saved})
    except PlaybookValidationError as exc:
        return jsonify({"ok": False, "error": str(exc)}), 400
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.route("/api/playbooks/<name>", methods=["DELETE"])
def api_delete_playbook(name: str):
    try:
        _playbook_service().delete_playbook(name)
        return jsonify({"ok": True})
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 400


@app.route("/api/playbooks/<name>/execute", methods=["POST"])
def api_execute_playbook(name: str):
    payload = request.get_json(silent=True) or {}
    context = payload.get("context", {}) if isinstance(payload.get("context", {}), dict) else {}
    resume = payload.get("resume", {}) if isinstance(payload.get("resume", {}), dict) else {}
    user_choice = str(payload.get("user_choice", "")).strip()
    try:
        if resume:
            result = _playbook_service().resume_playbook(name, resume=resume, user_choice=user_choice)
        else:
            result = _playbook_service().execute_playbook(name, context=context)
        return jsonify({
            "ok": True,
            "result": {
                "name": result.name,
                "success": result.success,
                "paused": result.paused,
                "prompt_message": result.prompt_message,
                "resume": result.resume,
                "logs": result.logs,
            }
        })
    except Exception as exc:
        return jsonify({
            "ok": False,
            "error": str(exc),
            "result": {"name": name, "success": False, "paused": False, "prompt_message": "", "resume": None, "logs": []}
        }), 400


@app.route("/playbooks/<name>/run", methods=["POST"])
def playbook_run_from_index(name: str):
    note_name = str(request.form.get("note_name", "")).strip()
    redirect_to = _safe_redirect_target(request.form.get("redirect_to"), "index")
    try:
        result = _playbook_service().execute_playbook(name, context={"note_name": note_name})
        category = "success" if result.success else "warning"
        flash(f"Playbook '{name}' executed. {len(result.logs)} step log(s) generated.", category)
    except Exception as exc:
        flash(f"Failed to execute playbook: {exc}", "danger")
    return redirect(redirect_to)


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


@app.route("/settings", methods=["GET"])
def settings_page():
    try:
        conf = _load_conf()
    except Exception as exc:
        flash(f"Unable to load conf.json: {exc}", "danger")
        conf = {}

    ai_conf = conf.get("ai_feedback", {}) if isinstance(conf.get("ai_feedback", {}), dict) else {}
    db_conf = conf.get("db", {}) if isinstance(conf.get("db", {}), dict) else {}
    log_conf = conf.get("log", {}) if isinstance(conf.get("log", {}), dict) else {}
    compliance_conf = conf.get("compliance_check", {}) if isinstance(conf.get("compliance_check", {}), dict) else {}
    compliance_defaults = DocsParser.DEFAULT_COMPLIANCE_CHECK
    structure_conf = compliance_conf.get("structure", {}) if isinstance(compliance_conf.get("structure", {}), dict) else {}
    created_conf = compliance_conf.get("created", {}) if isinstance(compliance_conf.get("created", {}), dict) else {}
    beschreibung_conf = compliance_conf.get("beschreibung", {}) if isinstance(compliance_conf.get("beschreibung", {}), dict) else {}
    external_links_conf = compliance_conf.get("external_links", {}) if isinstance(compliance_conf.get("external_links", {}), dict) else {}
    tags_conf = compliance_conf.get("tags", {}) if isinstance(compliance_conf.get("tags", {}), dict) else {}
    video_links_conf = compliance_conf.get("video_links", {}) if isinstance(compliance_conf.get("video_links", {}), dict) else {}
    ai_feedback_conf = compliance_conf.get("ai_feedback", {}) if isinstance(compliance_conf.get("ai_feedback", {}), dict) else {}
    provider_value = ", ".join(_parse_json_array(ai_conf.get("provider", [])))
    structure_strings = structure_conf.get("strings_to_check", compliance_defaults["structure"]["strings_to_check"])
    if not isinstance(structure_strings, list):
        structure_strings = compliance_defaults["structure"]["strings_to_check"]
    structure_strings_text = "\n".join([str(item).strip() for item in structure_strings if str(item).strip()])

    settings_form = {
        "openrouter_model": str(ai_conf.get("model", "")).strip(),
        "openrouter_provider": provider_value,
        "openrouter_api_key": str(ai_conf.get("api_key", "")).strip(),
        "db_path": str(db_conf.get("db_path", "")).strip(),
        "log_file_path": str(log_conf.get("log_file_path", "")).strip(),
        "compliance_structure_enabled": bool(
            structure_conf.get("enabled", compliance_defaults["structure"]["enabled"])
        ),
        "compliance_structure_strings_to_check": structure_strings_text,
        "compliance_created_enabled": bool(created_conf.get("enabled", compliance_defaults["created"]["enabled"])),
        "compliance_beschreibung_enabled": bool(
            beschreibung_conf.get("enabled", compliance_defaults["beschreibung"]["enabled"])
        ),
        "compliance_beschreibung_max": str(
            beschreibung_conf.get("max", compliance_defaults["beschreibung"]["max"])
        ).strip(),
        "compliance_external_links_enabled": bool(
            external_links_conf.get("enabled", compliance_defaults["external_links"]["enabled"])
        ),
        "compliance_external_links_min": str(
            external_links_conf.get("min", compliance_defaults["external_links"]["min"])
        ).strip(),
        "compliance_tags_enabled": bool(tags_conf.get("enabled", compliance_defaults["tags"]["enabled"])),
        "compliance_tags_min": str(tags_conf.get("min", compliance_defaults["tags"]["min"])).strip(),
        "compliance_video_links_enabled": bool(
            video_links_conf.get("enabled", compliance_defaults["video_links"]["enabled"])
        ),
        "compliance_video_links_char": str(
            video_links_conf.get("char", compliance_defaults["video_links"]["char"])
        ).strip(),
        "compliance_ai_feedback_enabled": bool(
            ai_feedback_conf.get("enabled", compliance_defaults["ai_feedback"]["enabled"])
        ),
        "compliance_ai_feedback_min": str(
            ai_feedback_conf.get("min", compliance_defaults["ai_feedback"]["min"])
        ).strip(),
    }
    media_support = _openrouter_media_support(conf)

    return render_template("settings.html", settings=settings_form, media_support=media_support)


@app.route("/settings", methods=["POST"])
def settings_save():
    try:
        conf = _load_conf()
    except Exception as exc:
        flash(f"Unable to load conf.json: {exc}", "danger")
        return redirect(url_for("settings_page"))

    try:
        openrouter_model = _sanitize_conf_text(request.form.get("openrouter_model"), "Openrouter Model", max_length=200)
        openrouter_api_key = _sanitize_conf_text(
            request.form.get("openrouter_api_key"), "Openrouter API Key", max_length=500
        )
        db_path = _sanitize_conf_text(request.form.get("db_path"), "DB Path", max_length=500)
        log_file_path = _sanitize_conf_text(request.form.get("log_file_path"), "Log File Path", max_length=500)
        openrouter_provider = _parse_provider_list(request.form.get("openrouter_provider"))
        compliance_structure_enabled = _parse_checkbox_bool(request.form.get("compliance_structure_enabled"))
        compliance_structure_strings_to_check = _parse_multiline_conf_strings(
            request.form.get("compliance_structure_strings_to_check"),
            "Compliance Structure Strings",
        )
        compliance_created_enabled = _parse_checkbox_bool(request.form.get("compliance_created_enabled"))
        compliance_beschreibung_enabled = _parse_checkbox_bool(request.form.get("compliance_beschreibung_enabled"))
        compliance_beschreibung_max = _sanitize_non_negative_int(
            request.form.get("compliance_beschreibung_max"),
            "Compliance Beschreibung Max",
            minimum=1,
        )
        compliance_external_links_enabled = _parse_checkbox_bool(request.form.get("compliance_external_links_enabled"))
        compliance_external_links_min = _sanitize_non_negative_int(
            request.form.get("compliance_external_links_min"),
            "Compliance External Links Min",
            minimum=0,
        )
        compliance_tags_enabled = _parse_checkbox_bool(request.form.get("compliance_tags_enabled"))
        compliance_tags_min = _sanitize_non_negative_int(
            request.form.get("compliance_tags_min"),
            "Compliance Tags Min",
            minimum=0,
        )
        compliance_video_links_enabled = _parse_checkbox_bool(request.form.get("compliance_video_links_enabled"))
        compliance_video_links_char = _sanitize_non_negative_int(
            request.form.get("compliance_video_links_char"),
            "Compliance Video Links Character Threshold",
            minimum=1,
        )
        compliance_ai_feedback_enabled = _parse_checkbox_bool(request.form.get("compliance_ai_feedback_enabled"))
        compliance_ai_feedback_min = _sanitize_non_negative_int(
            request.form.get("compliance_ai_feedback_min"),
            "Compliance AI Feedback Minimum Score",
            minimum=0,
        )
    except ValueError as validation_error:
        flash(str(validation_error), "danger")
        return redirect(url_for("settings_page"))

    ai_conf = conf.setdefault("ai_feedback", {})
    if not isinstance(ai_conf, dict):
        ai_conf = {}
        conf["ai_feedback"] = ai_conf

    db_conf = conf.setdefault("db", {})
    if not isinstance(db_conf, dict):
        db_conf = {}
        conf["db"] = db_conf

    log_conf = conf.setdefault("log", {})
    if not isinstance(log_conf, dict):
        log_conf = {}
        conf["log"] = log_conf
    compliance_conf = conf.setdefault("compliance_check", {})
    if not isinstance(compliance_conf, dict):
        compliance_conf = {}
        conf["compliance_check"] = compliance_conf

    ai_conf["model"] = openrouter_model
    ai_conf["provider"] = openrouter_provider
    ai_conf["api_key"] = openrouter_api_key
    db_conf["db_path"] = db_path
    log_conf["log_file_path"] = log_file_path
    compliance_conf["structure"] = {
        "enabled": compliance_structure_enabled,
        "strings_to_check": compliance_structure_strings_to_check,
    }
    compliance_conf["created"] = {
        "enabled": compliance_created_enabled,
    }
    compliance_conf["beschreibung"] = {
        "enabled": compliance_beschreibung_enabled,
        "max": compliance_beschreibung_max,
    }
    compliance_conf["external_links"] = {
        "enabled": compliance_external_links_enabled,
        "min": compliance_external_links_min,
    }
    compliance_conf["tags"] = {
        "enabled": compliance_tags_enabled,
        "min": compliance_tags_min,
    }
    compliance_conf["video_links"] = {
        "enabled": compliance_video_links_enabled,
        "char": compliance_video_links_char,
    }
    compliance_conf["ai_feedback"] = {
        "enabled": compliance_ai_feedback_enabled,
        "min": compliance_ai_feedback_min,
    }

    try:
        _save_conf(conf)
    except Exception as exc:
        logger.error("Failed to save settings\n%s", traceback.format_exc())
        flash(f"Unable to save settings: {exc}", "danger")
        return redirect(url_for("settings_page"))

    flash("Settings saved successfully.", "success")
    flash("Changes apply only after rebuilding the Docker container.", "warning")
    return redirect(url_for("settings_page"))


@app.route("/scan", methods=["POST"])
def scan_docs():
    try:
        _perform_full_scan()
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
        if not _update_todo_entry(todo_id=todo_id, progress=progress):
            raise ValueError("Todo not found.")
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
        if not _update_todo_entry(todo_id=todo_id, priority=priority):
            raise ValueError("Todo not found.")
        flash("Todo priority updated.", "success")
    except BaseException:
        flash("Failed to update todo priority.", "danger")

    return redirect(url_for("todo_overview"))


@app.route("/todo/create-doc", methods=["POST"])
def create_doc_from_todo_template():
    todo_id = request.form.get("todo_id", "").strip()
    template_key = request.form.get("template_name", "").strip().lower()
    file_name = request.form.get("file_name", "").strip()
    reason = _normalize_update_reason(request.form.get("reason", ""))
    priority = _normalize_todo_priority(request.form.get("priority", "Medium"))
    create_history = request.form.get("create_history", "false").strip().lower() == "true"

    from_index = request.form.get("from_index", "false").strip().lower() == "true"
    selected_doc = request.form.get("selected_doc", "").strip()

    if from_index and selected_doc:
        file_name = selected_doc

    redirect_target = url_for("index") if from_index else url_for("todo_overview")

    try:
        result = _apply_doc_template(
            template_key=template_key,
            file_name=file_name,
            reason=reason,
            create_history=create_history,
            auto_create_history_if_missing=from_index,
        )
    except ValueError as exc:
        if template_key == "update" and str(exc) == "'#### Page History' not found in the target note.":
            normalized_file_name = _normalize_md_filename(file_name)
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
        flash(str(exc), "warning" if "Invalid template action request" in str(exc) or "Reason is required" in str(exc) else "danger")
        return redirect(redirect_target)
    except FileNotFoundError as exc:
        flash(str(exc), "danger")
        return redirect(redirect_target)
    except RuntimeError as exc:
        flash(str(exc), "danger")
        return redirect(redirect_target)
    except BaseException:
        flash("Failed to create note from template." if template_key == "new" else "Failed to update note from template.", "danger")
        return redirect(redirect_target)

    normalized_file_name = result["file_name"]
    target_path = result["path"]

    if template_key == "new":
        if result["status"] == "exists":
            flash("A note with this file name already exists. Please choose another file name.", "danger")
            return redirect(redirect_target)

        if from_index:
            _append_todo(
                note=Path(normalized_file_name).stem,
                todo_type="New",
                progress="In Progress",
                priority=priority,
            )
            flash("New note created and todo added successfully.", "success")
            return redirect(url_for("index"))

        _set_todo_in_progress(todo_id, normalized_file_name)
        flash("New note created from template successfully.", "success")
        return redirect(redirect_target)

    if from_index:
        _append_todo(
            note=f"{Path(normalized_file_name).stem} ({reason})",
            todo_type="Update",
            progress="In Progress",
            priority=priority,
        )
        flash("Update note request created and todo added.", "success")
        return redirect(url_for("index"))

    _set_todo_in_progress(todo_id, normalized_file_name)
    flash("Note updated from template successfully.", "success")
    return redirect(redirect_target)




@app.route("/docs/<int:doc_id>/view", methods=["GET"])
def view_doc(doc_id: int):
    database = db()
    doc_map = database.get_docs_by_id(doc_id)
    if not doc_map:
        flash("Document not found.", "warning")
        return redirect(url_for("index"))

    doc = next(iter(doc_map.values()))
    doc_title = str(doc.get("title", "")).strip()
    if not doc_title:
        flash("Document title is missing.", "danger")
        return redirect(url_for("index"))

    try:
        viewer = DocsViewer(_load_conf())
        title, content_html = viewer.render_doc_to_html(doc_title)
    except FileNotFoundError:
        flash("Markdown file for selected document was not found.", "danger")
        return redirect(url_for("index"))
    except ValueError:
        flash("Invalid document file name.", "danger")
        return redirect(url_for("index"))
    except Exception:
        logger.error("Markdown view route failed for doc_id=%s\n%s", doc_id, traceback.format_exc())
        flash("Failed to render markdown preview.", "danger")
        return redirect(url_for("index"))

    return render_template("doc_view.html", doc=doc, title=title, content_html=content_html)


@app.route("/docs/view/by-name/<slug>", methods=["GET"])
def view_doc_by_slug(slug: str):
    try:
        viewer = DocsViewer(_load_conf())
        resolved_file_name = viewer.find_filename_by_slug(slug)
    except Exception:
        flash("Could not resolve linked note.", "warning")
        return redirect(url_for("index"))

    database = db()
    resolved_title = Path(resolved_file_name).stem
    doc_map = database.get_docs_by_name(resolved_title, exact_match=True)
    if not doc_map:
        flash("Linked note exists on disk but is missing in the index. Run full scan first.", "warning")
        return redirect(url_for("index"))

    doc_id = int(next(iter(doc_map.keys())))
    return redirect(url_for("view_doc", doc_id=doc_id))


@app.route("/docs/view/by-path/<path:relative_path>", methods=["GET"])
def view_doc_by_path(relative_path: str):
    try:
        viewer = DocsViewer(_load_conf())
        title, content_html = viewer.render_doc_to_html_by_relative_path(relative_path)
    except FileNotFoundError:
        flash("Linked note not found on disk.", "warning")
        return redirect(url_for("index"))
    except ValueError:
        flash("Invalid linked note path.", "warning")
        return redirect(url_for("index"))
    except Exception:
        logger.error("Markdown view route failed for relative_path=%s\n%s", relative_path, traceback.format_exc())
        flash("Failed to render linked note preview.", "danger")
        return redirect(url_for("index"))

    docs_root = Path(viewer.conf.get("docs", {}).get("full_path_to_docs", "")).resolve()
    resolved_doc = (docs_root / relative_path).resolve()
    resolved_title = resolved_doc.stem

    database = db()
    doc_map = database.get_docs_by_name(resolved_title, exact_match=True)
    doc_for_template = next(iter(doc_map.values())) if doc_map else {"id": None}

    return render_template("doc_view.html", doc=doc_for_template, title=title, content_html=content_html)


@app.route("/docs/pictures/<path:file_name>", methods=["GET"])
def view_doc_picture(file_name: str):
    try:
        conf = _load_conf()
        pictures_root = Path(conf.get("pictures", {}).get("full_path_to_pictures", "")).resolve()
        if not pictures_root.exists() or not pictures_root.is_dir():
            raise FileNotFoundError("Configured pictures directory does not exist.")

        requested_name = Path(file_name).name.strip()
        if not requested_name:
            raise ValueError("Invalid picture file name.")

        return send_from_directory(str(pictures_root), requested_name, as_attachment=False)
    except FileNotFoundError:
        return ("Picture not found.", 404)
    except ValueError:
        return ("Invalid picture path.", 400)
    except Exception:
        logger.error("Picture route failed for file_name=%s\n%s", file_name, traceback.format_exc())
        return ("Failed to load picture.", 500)

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
        remap_docs=_list_existing_doc_note_names(),
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
        feedback_payload = _create_ai_feedback_for_document(selected_doc, include_images=True)
        flash(f"AI feedback created successfully for {feedback_payload['note_name']}.", "success")
        return redirect(redirect_to)
    except OpenRouterImageNotSupportedError as exc:
        logger.warning("OpenRouter model does not support image input for %s\n%s", selected_doc, traceback.format_exc())
        return render_template(
            "ai_feedback_image_fallback.html",
            selected_doc=selected_doc,
            redirect_to=redirect_to,
            error_message=str(exc),
        )
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


@app.route("/ai_feedback/generate/retry_without_images", methods=["POST"])
def generate_ai_feedback_without_images():
    selected_doc = request.form.get("selected_doc", "").strip()
    redirect_to = _safe_redirect_target(request.form.get("redirect_to"), "ai_feedback_overview")
    if not selected_doc:
        flash("Please select a document for AI feedback.", "warning")
        return redirect(redirect_to)

    try:
        feedback_payload = _create_ai_feedback_for_document(selected_doc, include_images=False)
        flash(f"AI feedback created successfully for {feedback_payload['note_name']} without images.", "success")
        return redirect(redirect_to)
    except (ValueError, RuntimeError, FileNotFoundError) as exc:
        logger.error("AI feedback text-only retry failed\n%s", traceback.format_exc())
        flash(str(exc), "warning")
        return redirect(redirect_to)
    except SystemExit:
        logger.error("AI feedback text-only retry aborted by SystemExit\n%s", traceback.format_exc())
        flash("AI feedback generation failed due to a file, database, or parser exit. Check logs and paths.", "danger")
        return redirect(redirect_to)
    except BaseException as exc:
        logger.error("AI feedback text-only retry failed with BaseException\n%s", traceback.format_exc())
        flash(f"AI feedback generation failed unexpectedly: {exc}", "danger")
        return redirect(redirect_to)


@app.route("/ai_feedback/generate/cancel", methods=["POST"])
def cancel_ai_feedback_generation():
    redirect_to = _safe_redirect_target(request.form.get("redirect_to"), "ai_feedback_overview")
    flash("AI feedback generation was cancelled.", "warning")
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


@app.route("/ai_feedback/<int:feedback_id>/remap", methods=["POST"])
def ai_feedback_remap(feedback_id: int):
    redirect_to = _safe_redirect_target(request.form.get("redirect_to"), "ai_feedback_overview")
    selected_note_name = str(request.form.get("selected_note_name", "")).strip()
    normalized_doc = _normalize_md_filename(selected_note_name)
    if not normalized_doc:
        flash("Please select a valid markdown note name for remapping.", "warning")
        return redirect(redirect_to)

    database = db()
    feedback_row = database.get_ai_feedback_by_id(feedback_id)
    if not feedback_row:
        flash("AI feedback entry not found.", "warning")
        return redirect(redirect_to)

    if not _docs_note_exists(normalized_doc):
        flash("The selected note does not exist in the configured docs folder.", "warning")
        return redirect(redirect_to)

    feedback_path = Path(str(feedback_row.get("path_to_feedback", "")).strip()).resolve()
    if not feedback_path.exists() or not feedback_path.is_file():
        flash("AI feedback file not found for remapping.", "warning")
        return redirect(redirect_to)

    try:
        DocsWriter().update_ai_feedback_file_note_name(feedback_path, Path(normalized_doc).stem.strip())
        _set_rw_permissions_for_all_users(feedback_path)
        _sync_ai_feedback_and_openrouter_credits(database)
        flash("AI feedback was remapped successfully.", "success")
    except Exception as exc:
        logger.error("AI feedback remap failed\n%s", traceback.format_exc())
        flash(f"Failed to remap AI feedback: {exc}", "danger")
    return redirect(redirect_to)


@app.route("/learning", methods=["GET"])
def learning_overview():
    database = db()
    name_query = request.args.get("name", "").strip().casefold()
    learning_rows = database.get_all_learnings()
    if name_query:
        learning_rows = [row for row in learning_rows if name_query in str(row.get("file_name", "")).casefold()]
    prepared_learning_rows = []
    for row in learning_rows:
        prepared_row = dict(row)
        prepared_row["status_icon"] = _learning_status_icon(prepared_row)
        prepared_row["doc_preview_url"] = _learning_doc_preview_url(prepared_row)
        prepared_learning_rows.append(prepared_row)
    available_docs = sorted(
        [str(item.get("title", "")).strip() for item in database.get_all_docs().values() if str(item.get("title", "")).strip()],
        key=lambda value: value.casefold(),
    )
    openrouter_credits_left = str(database.get_setting("openrouter_credits_left", "N/A") or "").strip() or "N/A"
    return render_template(
        "learning.html",
        learning_rows=prepared_learning_rows,
        total_learnings=len(database.get_all_learnings()),
        selected_name=request.args.get("name", "").strip(),
        available_docs=available_docs,
        all_tags=database.get_all_tags(),
        openrouter_credits_left=openrouter_credits_left,
        remap_docs=_list_existing_doc_note_names(),
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

    try:
        learning_row = _create_learning_for_doc(normalized_doc)
        flash(f"Learning file created: {Path(str(learning_row.get('path_to_learning', '')).strip()).name}", "success")
    except Exception as exc:
        flash(str(exc), "warning")
    return redirect(url_for("learning_overview"))


@app.route("/learning/doc-action", methods=["POST"])
def learning_doc_action():
    selected_doc = str(request.form.get("selected_doc", "")).strip()
    redirect_to = _safe_redirect_target(request.form.get("redirect_to"), "index")
    normalized_doc = _normalize_md_filename(selected_doc)
    if not normalized_doc:
        flash("Please select a valid markdown note.", "warning")
        return redirect(redirect_to)

    database = db()
    existing_learning = _find_learning_for_doc(database, normalized_doc)
    if existing_learning:
        return redirect(url_for("learning_detail", learning_id=int(existing_learning["id"])))

    try:
        learning_row = _create_learning_for_doc(normalized_doc)
        flash(f"Learning file created: {Path(str(learning_row.get('path_to_learning', '')).strip()).name}", "success")
        return redirect(url_for("learning_detail", learning_id=int(learning_row["id"])))
    except Exception as exc:
        flash(str(exc), "warning")
        return redirect(redirect_to)


@app.route("/learning/<int:learning_id>/remap", methods=["POST"])
def learning_remap(learning_id: int):
    selected_note_name = str(request.form.get("selected_note_name", "")).strip()
    normalized_doc = _normalize_md_filename(selected_note_name)
    if not normalized_doc:
        flash("Please select a valid markdown note name for remapping.", "warning")
        return redirect(url_for("learning_overview"))

    if not _docs_note_exists(normalized_doc):
        flash("The selected note does not exist in the configured docs folder.", "warning")
        return redirect(url_for("learning_overview"))

    database = db()
    learning_row = database.get_learning_by_id(learning_id)
    if not learning_row:
        flash("Learning entry not found.", "warning")
        return redirect(url_for("learning_overview"))

    learning_path = Path(str(learning_row.get("path_to_learning", "")).strip()).resolve()
    if not learning_path.exists() or not learning_path.is_file():
        flash("Learning file not found for remapping.", "warning")
        return redirect(url_for("learning_overview"))

    try:
        DocsWriter().update_learning_file_note_name(learning_path, Path(normalized_doc).stem.strip())
        _set_rw_permissions_for_all_users(learning_path)
        DocsParser().sync_learning_to_db()
        flash("Learning was remapped successfully.", "success")
    except Exception as exc:
        logger.error("Learning remap failed\n%s", traceback.format_exc())
        flash(f"Failed to remap learning: {exc}", "danger")
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
    grouped_questions = {"FREETEXT": [], "MULTIPLE_CHOICE": [], "SINGLE_CHOICE": []}
    for question in parsed_learning.get("questions", []):
        grouped_questions.setdefault(str(question.get("type", "FREETEXT")).upper(), []).append(question)
    return render_template(
        "learning_detail.html",
        learning=learning_row,
        doc_preview_url=_learning_doc_preview_url(learning_row),
        parsed_learning=parsed_learning,
        grouped_questions=grouped_questions,
        attempts=attempts,
    )

@app.route("/learning/<int:learning_id>/attempts/<int:attempt_id>", methods=["GET"])
def learning_attempt_review(learning_id: int, attempt_id: int):
    database = db()
    learning_row = database.get_learning_by_id(learning_id)
    if not learning_row:
        flash("Learning entry not found.", "warning")
        return redirect(url_for("learning_overview"))

    attempt = database.get_learning_exam_attempt_by_id(attempt_id)
    if not attempt or int(attempt.get("learning_id", 0)) != learning_id:
        flash("Attempt entry not found.", "warning")
        return redirect(url_for("learning_detail", learning_id=learning_id))

    parsed_learning = DocsParser().parse_learning_file(learning_row.get("path_to_learning", ""))
    answer_key_map = {
        str(item.get("question_id", "")).strip(): [str(value).strip() for value in item.get("correct_answers", []) if str(value).strip()]
        for item in parsed_learning.get("answers", [])
        if isinstance(item, dict)
    }
    try:
        submitted_answers = json.loads(attempt.get("answers_json", "{}"))
    except json.JSONDecodeError:
        submitted_answers = {}
    if not isinstance(submitted_answers, dict):
        submitted_answers = {}

    rows: list[dict] = []
    for question in parsed_learning.get("questions", []):
        question_id = str(question.get("id", "")).strip()
        expected_answers = sorted(answer_key_map.get(question_id, []))
        given_answers = sorted([str(value).strip() for value in submitted_answers.get(question_id, []) if str(value).strip()])
        question_type = str(question.get("type", "FREETEXT")).strip().upper()
        is_scored = question_type != "FREETEXT"
        rows.append(
            {
                "id": question_id or "N/A",
                "text": str(question.get("text", "")).strip() or "N/A",
                "question_type": question_type,
                "given_answers": given_answers,
                "expected_answers": expected_answers,
                "is_scored": is_scored,
                "is_correct": (expected_answers == given_answers) if is_scored else None,
            }
        )

    return render_template(
        "learning_attempt_review.html",
        learning=learning_row,
        doc_preview_url=_learning_doc_preview_url(learning_row),
        attempt=attempt,
        comparison_rows=rows,
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
        last_modified_date=_today_dd_mm_yyyy(),
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
        source_note_name = str(learning_row.get("source_note_name", "")).strip()
        _generate_learning_questions_for_doc(_normalize_md_filename(source_note_name))
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


def _build_fused_learning_payload(learning_rows: list[dict]) -> dict:
    parser = DocsParser()
    fused_questions: list[dict] = []
    fused_answers_map: dict[str, list[str]] = {}
    included_learning_rows: list[dict] = []

    for learning_row in learning_rows:
        learning_id = int(learning_row.get("id", 0))
        if learning_id <= 0:
            continue
        parsed_learning = parser.parse_learning_file(learning_row.get("path_to_learning", ""))
        answer_map = {
            str(item.get("question_id", "")).strip(): item.get("correct_answers", [])
            for item in parsed_learning.get("answers", [])
            if isinstance(item, dict)
        }
        for question in parsed_learning.get("questions", []):
            qid = str(question.get("id", "")).strip()
            if not qid:
                continue
            fused_qid = f"L{learning_id}__{qid}"
            fused_questions.append(
                {
                    "id": fused_qid,
                    "type": str(question.get("type", "FREETEXT")).strip().upper(),
                    "text": str(question.get("text", "")).strip(),
                    "options": question.get("options", []),
                    "learning_file_name": str(learning_row.get("file_name", "")).strip(),
                }
            )
            fused_answers_map[fused_qid] = [str(item).strip() for item in answer_map.get(qid, []) if str(item).strip()]
        included_learning_rows.append(learning_row)

    return {"questions": fused_questions, "answers_map": fused_answers_map, "learning_rows": included_learning_rows}


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


@app.route("/learning/mode/fused", methods=["POST"])
def learning_mode_fused():
    database = db()
    exam_source = str(request.form.get("exam_source", "learnings")).strip().lower()
    selected_rows: list[dict] = []

    if exam_source == "tags":
        selected_tags = [_normalize_tag_value(tag) for tag in request.form.getlist("selected_tags")]
        selected_tags = sorted({tag for tag in selected_tags if tag}, key=lambda value: value.casefold())
        if not selected_tags:
            flash("Please select at least one tag.", "warning")
            return redirect(url_for("learning_overview"))
        allowed_tags = set(database.get_all_tags())
        if any(tag not in allowed_tags for tag in selected_tags):
            flash("One or more selected tags are invalid.", "danger")
            return redirect(url_for("learning_overview"))

        doc_learning_rows = database.get_learning_docs_by_tags(selected_tags)
        docs_with_learning: list[dict] = []
        docs_without_learning: list[dict] = []
        selected_learning_ids: set[int] = set()
        seen_docs_without_learning: set[int] = set()

        for row in doc_learning_rows:
            doc_id = int(row.get("doc_id", 0))
            doc_title = str(row.get("doc_title", "")).strip()
            learning_id = row.get("learning_id")
            if learning_id is None:
                if doc_id not in seen_docs_without_learning:
                    docs_without_learning.append({"id": doc_id, "title": doc_title})
                    seen_docs_without_learning.add(doc_id)
                continue
            docs_with_learning.append(
                {"id": doc_id, "title": doc_title, "learning_file_name": str(row.get("learning_file_name", "")).strip()}
            )
            selected_learning_ids.add(int(learning_id))

        if not selected_learning_ids:
            flash("No learning entries found for the selected tags.", "warning")
            return redirect(url_for("learning_overview"))

        if docs_without_learning:
            return render_template(
                "learning_tag_warning.html",
                selected_tags=selected_tags,
                docs_with_learning=sorted(docs_with_learning, key=lambda value: value["title"].casefold()),
                docs_without_learning=sorted(docs_without_learning, key=lambda value: value["title"].casefold()),
                selected_learning_ids=sorted(selected_learning_ids),
            )

        for learning_id in sorted(selected_learning_ids):
            learning_row = database.get_learning_by_id(learning_id)
            if learning_row:
                selected_rows.append(learning_row)
    else:
        raw_learning_ids = [str(value).strip() for value in request.form.getlist("selected_learning_ids")]
        selected_learning_ids = sorted({int(value) for value in raw_learning_ids if value.isdigit() and int(value) > 0})
        if not selected_learning_ids:
            flash("Please select at least one learning.", "warning")
            return redirect(url_for("learning_overview"))
        for learning_id in selected_learning_ids:
            learning_row = database.get_learning_by_id(learning_id)
            if learning_row:
                selected_rows.append(learning_row)

    fused_payload = _build_fused_learning_payload(selected_rows)
    if not fused_payload["questions"]:
        flash("No questions found in the selected learning files.", "warning")
        return redirect(url_for("learning_overview"))

    return render_template(
        "learning_mode_fused.html",
        selected_learning_rows=fused_payload["learning_rows"],
        parsed_learning={"questions": fused_payload["questions"], "answers": []},
        draft_answers={},
        review_attempt=None,
        answers_map=fused_payload["answers_map"],
    )


@app.route("/learning/mode/fused/finish", methods=["POST"])
def learning_mode_fused_finish():
    answers_map_raw = str(request.form.get("answers_map_json", "")).strip()
    selected_learning_ids = [str(value).strip() for value in request.form.getlist("selected_learning_ids")]
    selected_ids = sorted({int(value) for value in selected_learning_ids if value.isdigit() and int(value) > 0})
    if not answers_map_raw or not selected_ids:
        flash("Invalid fused exam payload.", "warning")
        return redirect(url_for("learning_overview"))

    try:
        answers_map = json.loads(answers_map_raw)
    except json.JSONDecodeError:
        flash("Invalid fused exam answer key.", "warning")
        return redirect(url_for("learning_overview"))
    if not isinstance(answers_map, dict):
        flash("Invalid fused exam answer key.", "warning")
        return redirect(url_for("learning_overview"))

    database = db()
    selected_rows = [database.get_learning_by_id(learning_id) for learning_id in selected_ids]
    selected_rows = [row for row in selected_rows if row]
    fused_payload = _build_fused_learning_payload(selected_rows)
    if not fused_payload["questions"]:
        flash("No questions found in the selected learning files.", "warning")
        return redirect(url_for("learning_overview"))

    questions = fused_payload["questions"]
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

    flash(
        f"Fused exam finished. Score: {correct}/{scored_questions}. Free-text questions are shown for review but are not scored.",
        "success",
    )
    return render_template(
        "learning_mode_fused.html",
        selected_learning_rows=fused_payload["learning_rows"],
        parsed_learning={"questions": questions, "answers": []},
        draft_answers=user_answers,
        review_attempt={"score": correct, "total_questions": scored_questions},
        answers_map=answers_map,
    )


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
