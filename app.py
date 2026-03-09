import html
import json
import re
import traceback
from datetime import datetime
from pathlib import Path

from flask import Flask, flash, jsonify, redirect, render_template, request, send_file, url_for
from markupsafe import Markup
from werkzeug.exceptions import HTTPException

from main import export_result_to_markdown
from src.DatabaseConnector import db
from src.DocsParser import DocsParser
from src.DocsVersionHandler import DocsVersionHandler
from src.DocsWriter import DocsWriter
from src.logger import get_logger


app = Flask(__name__)
app.secret_key = "knowledge-mapping-secret"

logger = get_logger(__name__)


SW_STATUS_OPTIONS = ["", "Not Started", "In Progress", "Done", "Not Needed"]


def _render_hslu_inline_markdown(value: str) -> str:
    text = str(value or "")

    def _render_fragment(fragment: str) -> str:
        pattern = re.compile(r"(\*\*(.+?)\*\*|==(.+?)==)")
        parts: list[str] = []
        last_end = 0
        for match in pattern.finditer(fragment):
            parts.append(html.escape(fragment[last_end:match.start()]))
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




def _normalize_manual_override(value: str | None) -> str:
    return "true" if str(value).strip().lower() == "true" else "false"


def _compliance_tag_class(doc: dict) -> str:
    is_compliant = doc.get("is_compliant") == "true"
    manual_override = _normalize_manual_override(doc.get("manual_compliant_override")) == "true"

    if manual_override:
        return "compliance-tag-manual"
    if is_compliant:
        return "compliance-tag-compliant"
    
    return "compliance-tag-not-compliant"

def _load_docs(database: db, view: str, query: str) -> dict:

    if view == "id" and query:
        try:
            return database.get_docs_by_id(int(query))
        except ValueError:
            logger.warning("Invalid ID query received in UI: %s", query)
            flash("ID must be a number", "danger")
            return database.get_all_docs()

    if view == "name" and query:
        return database.get_docs_by_name(query)

    if view == "tag" and query:
        return database.get_docs_by_tag(query)

    if view == "incompliant":
        return database.get_non_compliant_docs()

    if view == "compliant":
        return database.get_compliant_docs()

    return database.get_all_docs()


def _load_conf() -> dict:
    conf_path = Path(__file__).resolve().parent / "conf.json"
    with open(conf_path, "r", encoding="utf-8") as conf_file:
        return json.loads(conf_file.read())


def _today_dd_mm() -> str:
    return datetime.now().strftime("%d.%m")


def _normalize_todo_types(value):
    normalized = _normalize_value(value)
    if isinstance(normalized, list):
        return normalized
    if not normalized:
        return []
    return [str(normalized)]


def _load_todos(database: db, query: str) -> list[dict]:
    rows = database.get_todos_by_note(query) if query else database.get_all_todos()
    processed_rows = []
    for row in rows:
        prepared = dict(row)
        prepared["type_list"] = _normalize_todo_types(prepared.get("type"))
        processed_rows.append(prepared)

    return processed_rows


def _load_hslu_overview(database: db, semester: str, module: str, sw: str) -> tuple[list[str], str, list[str], str, str, list[dict], str]:
    semesters = database.get_hslu_semesters()
    standard_semester = database.get_hslu_standard_semester()

    default_semester = standard_semester if standard_semester in semesters else (semesters[0] if semesters else "")
    selected_semester = semester if semester in semesters else default_semester

    modules = database.get_hslu_modules_by_semester(selected_semester) if selected_semester else []
    selected_module = module if module in modules else ""

    selected_sw = sw if sw.isdigit() else ""

    rows = database.get_hslu_sw_overview_by_semester_and_module(selected_semester, selected_module) if selected_semester else []
    if selected_sw:
        rows = [row for row in rows if str(row.get("SW", "")).strip() == selected_sw]

    return semesters, selected_semester, modules, selected_module, selected_sw, rows, standard_semester


def _docs_alpha_sort_key(doc: dict) -> tuple[int, str]:
    title = str(doc.get("title") or "").strip()
    starts_with_digit = title[:1].isdigit()
    return (0 if starts_with_digit else 1, title.casefold())


@app.route("/", methods=["GET"])
def index():
    view = request.args.get("view", "all")
    query = request.args.get("q", "").strip()

    database = db()
    docs = _load_docs(database, view, query)

    total_docs = len(docs)
    compliant_docs = len([d for d in docs.values() if d.get("is_compliant") == "true"])
    incompliant_docs = len([d for d in docs.values() if d.get("is_compliant") == "false"])

    processed_docs = []
    for item in docs.values():
        row = dict(item)
        row["tags_list"] = _to_display_list(row.get("tags"))
        row["links_list"] = _to_display_list(row.get("links"))
        row["video_links_list"] = _to_display_list(row.get("video_links"))
        row["noncompliance_reason_list"] = _to_display_list(row.get("noncompliance_reason"))
        row["manual_compliant_override"] = _normalize_manual_override(row.get("manual_compliant_override"))
        row["compliance_tag_class"] = _compliance_tag_class(row)
        processed_docs.append(row)

    processed_docs.sort(key=_docs_alpha_sort_key)
    last_sync_time = database.get_last_sync_time()

    version_status = database.get_version_control_snapshot()

    return render_template(
        "index.html",
        docs=processed_docs,
        total_docs=total_docs,
        compliant_docs=compliant_docs,
        incompliant_docs=incompliant_docs,
        selected_view=view,
        query=query,
        last_sync_time=last_sync_time,
        has_git_changes=version_status.get("has_changes", False),
    )


@app.route("/version_control", methods=["GET"])
def version_control_overview():
    database = db()
    version_status = database.get_version_control_snapshot()

    return render_template(
        "version_control.html",
        has_changes=version_status.get("has_changes", False),
        changes=version_status.get("changes", []),
        synced_at=version_status.get("synced_at", "Never"),
    )


@app.route("/version_control/sync", methods=["POST"])
def version_control_sync():
    try:
        database = db()
        version_handler = DocsVersionHandler()
        snapshot = version_handler.get_status_snapshot()
        change_count = len(snapshot.get("changes", []))
        synced_at = database.save_version_control_snapshot(snapshot)
        flash(
            f"Git status refreshed at {synced_at}. {change_count} changed files detected in /the-knowledge/02_DOCS.",
            "success",
        )
    except Exception as exc:
        flash(f"Failed to refresh git status: {exc}", "danger")

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


@app.route("/version_control/pull", methods=["POST"])
def version_control_pull():
    try:
        version_handler = DocsVersionHandler()
        output = version_handler.pull_latest()
        flash(f"Git pull completed successfully. {output}", "success")
    except Exception as exc:
        flash(f"Failed to pull changes: {exc}", "danger")

    return redirect(url_for("version_control_overview"))


@app.route("/version_control/push", methods=["POST"])
def version_control_push():
    commit_message = request.form.get("commit_message", "").strip()
    if not commit_message:
        flash("Commit message is required before pushing.", "warning")
        return redirect(url_for("version_control_overview"))

    try:
        version_handler = DocsVersionHandler()
        output = version_handler.commit_and_push(commit_message)
        flash(f"Commit and push completed successfully. {output}", "success")
    except Exception as exc:
        flash(f"Failed to commit/push changes: {exc}", "danger")

    return redirect(url_for("version_control_overview"))


@app.route("/api/version_control/status", methods=["GET"])
def version_control_status_api():
    try:
        database = db()
        return jsonify(database.get_version_control_snapshot())
    except Exception as exc:
        return jsonify({"has_changes": False, "changes": [], "synced_at": "Never", "error": str(exc)}), 500


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
    manual_override = _normalize_manual_override(request.form.get("manual_compliant_override", "false"))

    if not doc_id:
        logger.warning("Manual compliance update requested without doc_id")
        flash("Please provide a document ID.", "warning")
        return redirect(url_for("index"))

    if manual_override not in ("true", "false"):
        logger.warning("Invalid manual compliance value for id=%s value=%s", doc_id, manual_override)
        flash("Manual compliance value must be true or false.", "danger")
        return redirect(url_for("index"))

    try:
        database = db()
        database.update_manual_compliance_by_id(int(doc_id), manual_override)
        if manual_override == "true":
            flash(f"Document id={doc_id} is now manually marked as compliant.", "success")
        else:
            flash(f"Manual compliance override removed for id={doc_id}.", "success")
    except ValueError:
        logger.warning("Manual compliance update failed due to non-numeric id: %s", doc_id)
        flash("ID must be numeric.", "danger")

    return redirect(url_for("index"))


@app.route("/delete/id", methods=["POST"])
def delete_by_id():
    doc_id = request.form.get("doc_id", "").strip()
    if not doc_id:
        logger.warning("Delete by id requested without doc_id")
        flash("Please provide a document ID.", "warning")
        return redirect(url_for("index"))

    try:
        database = db()
        database.delete_docs_by_id(int(doc_id))
        logger.info("Deleted entry by id via UI: %s", doc_id)
        flash(f"Deleted entry with id={doc_id}", "success")
    except ValueError:
        logger.warning("Delete by id failed due to non-numeric id: %s", doc_id)
        flash("ID must be numeric.", "danger")

    return redirect(url_for("index"))


@app.route("/delete/name", methods=["POST"])
def delete_by_name():
    name = request.form.get("name", "").strip()
    if not name:
        logger.warning("Delete by name requested without name")
        flash("Please provide a file name.", "warning")
        return redirect(url_for("index"))

    database = db()
    database.delete_docs_by_name(name)
    logger.info("Deleted entries by name via UI: %s", name)
    flash(f"Deleted entries with name={name}", "success")
    return redirect(url_for("index"))


@app.route("/delete/all", methods=["POST"])
def delete_all():
    database = db()
    database.delete_all_docs()
    logger.warning("Deleted all entries via UI")
    flash("Deleted all entries.", "success")
    return redirect(url_for("index"))


@app.route("/export", methods=["GET"])
def export_results():
    view = request.args.get("view", "all")
    query = request.args.get("q", "").strip()
    database = db()
    docs = _load_docs(database, view, query)

    export_path = export_result_to_markdown(docs)
    return send_file(export_path, as_attachment=True, download_name="results.md")


@app.route("/history", methods=["GET"])
def version_history():
    database = db()
    versions = database.get_latest_change_versions(10)
    selected_version = request.args.get("version", "").strip()

    if not selected_version and versions:
        selected_version = versions[0]

    if selected_version and selected_version not in versions:
        logger.warning("Requested unavailable change version: %s", selected_version)
        flash("Selected version is not available anymore.", "warning")
        selected_version = versions[0] if versions else ""

    changes = database.get_changes_by_version(selected_version) if selected_version else []

    return render_template(
        "history.html",
        versions=versions,
        selected_version=selected_version,
        changes=changes,
    )


@app.route("/todo", methods=["GET"])
def todo_overview():
    query = request.args.get("q", "").strip()

    try:
        parser = DocsParser()
        parser.sync_todos_to_db()
    except BaseException as exc:
        if isinstance(exc, SystemExit):
            flash("Todo sync failed. Check conf.json todo path and parser logs.", "danger")
        else:
            flash("Automatic todo sync failed. You can retry using 'Sync Todos'.", "warning")

    database = db()
    todos = _load_todos(database, query)
    return render_template("todo.html", todos=todos, query=query)


@app.route("/todo/sync", methods=["POST"])
def sync_todos():
    try:
        parser = DocsParser()
        synced = parser.sync_todos_to_db()
        flash(f"Synced {len(synced)} todos from markdown.", "success")
    except BaseException as exc:
        if isinstance(exc, SystemExit):
            flash("Todo sync failed. Check conf.json todo path and parser logs.", "danger")
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
        parser.sync_todos_to_db()
        flash("Todo added successfully.", "success")
    except BaseException:
        flash("Failed to add todo. Check logs and markdown format.", "danger")

    return redirect(url_for("todo_overview"))


@app.route("/todo/delete", methods=["POST"])
def delete_todo():
    todo_id = request.form.get("todo_id", "").strip()
    if not todo_id:
        flash("Todo id is required.", "warning")
        return redirect(url_for("todo_overview"))

    try:
        parser = DocsParser()
        database = db()
        current_todos = database.get_all_todos()
        kept_todos = [todo for todo in current_todos if str(todo.get("id")) != todo_id]

        conf = _load_conf()
        writer = DocsWriter(conf.get("todo", {}).get("full_path_to_todo_file", ""))
        writer.write_todos_table(kept_todos)
        parser.sync_todos_to_db()
        flash("Todo deleted.", "success")
    except BaseException:
        flash("Failed to delete todo.", "danger")

    return redirect(url_for("todo_overview"))


@app.route("/todo/progress", methods=["POST"])
def update_todo_progress():
    todo_id = request.form.get("todo_id", "").strip()
    progress = request.form.get("progress", "Not Started").strip()

    if not todo_id:
        flash("Todo id is required.", "warning")
        return redirect(url_for("todo_overview"))

    try:
        parser = DocsParser()
        database = db()
        current_todos = database.get_all_todos()

        for todo in current_todos:
            if str(todo.get("id")) == todo_id:
                todo["progress"] = progress
                todo["last_update"] = _today_dd_mm()

        conf = _load_conf()
        writer = DocsWriter(conf.get("todo", {}).get("full_path_to_todo_file", ""))
        writer.write_todos_table(current_todos)
        parser.sync_todos_to_db()
        flash("Todo progress updated.", "success")
    except BaseException:
        flash("Failed to update todo progress.", "danger")

    return redirect(url_for("todo_overview"))


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
    return (
        render_template(
            "500.html",
            error_message="Something went wrong while loading this page. Please try again.",
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
            error_message="Something went wrong while loading this page. Please try again.",
        ),
        500,
    )


@app.route("/hslu/semester_overview", methods=["GET"])
def hslu_semester_overview():
    database = db()
    parser = DocsParser()

    try:
        parser.sync_hslu_sw_overview_to_db()
    except SystemExit:
        flash("Automatic HSLU sync failed. You can retry using 'Sync now'.", "warning")

    semester = request.args.get("semester", "").strip()
    module = request.args.get("module", "").strip()
    sw = request.args.get("sw", "").strip()
    semesters, selected_semester, modules, selected_module, selected_sw, overview_rows, standard_semester = _load_hslu_overview(database, semester, module, sw)

    return render_template(
        "hslu_semester_overview.html",
        semesters=semesters,
        selected_semester=selected_semester,
        modules=modules,
        selected_module=selected_module,
        overview_rows=overview_rows,
        selected_sw=selected_sw,
        standard_semester=standard_semester,
        last_sync_time=database.get_last_sync_time(),
        sw_status_options=SW_STATUS_OPTIONS,
    )




@app.route("/hslu/semester_overview/standard_semester", methods=["POST"])
def hslu_semester_overview_standard_semester():
    semester = request.form.get("semester", "").strip()

    database = db()
    semesters = database.get_hslu_semesters()

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

    if not semester or not module or not kw or not sw:
        flash("Missing row identifiers for status update.", "warning")
        return redirect(url_for("hslu_semester_overview", semester=semester, module=module, sw=sw_filter))

    if field not in ("downloaded", "documented"):
        flash("Invalid status target field.", "danger")
        return redirect(url_for("hslu_semester_overview", semester=semester, module=module, sw=sw_filter))

    if status not in SW_STATUS_OPTIONS:
        flash("Invalid status selection.", "danger")
        return redirect(url_for("hslu_semester_overview", semester=semester, module=module, sw=sw_filter))

    try:
        parser = DocsParser()
        parser.update_hslu_sw_status(semester, module, kw, sw, field, status)
        parser.sync_hslu_sw_overview_to_db()
        flash(f"Updated {field} status for KW {kw} / SW {sw}.", "success")
    except SystemExit:
        flash("Failed to update markdown status. Check logs and file mapping.", "danger")

    return redirect(url_for("hslu_semester_overview", semester=semester, module=module, sw=sw_filter))


@app.route("/hslu/semester_overview/sync", methods=["POST"])
def hslu_semester_overview_sync():
    semester = request.form.get("semester", "").strip()
    module = request.form.get("module", "").strip()
    sw = request.form.get("sw", "").strip()

    try:
        parser = DocsParser()
        rows = parser.sync_hslu_sw_overview_to_db()
        db().update_last_sync_time()
        flash(f"Synced {len(rows)} semester overview rows.", "success")
    except SystemExit:
        flash("HSLU sync failed. Check logs and folder mapping.", "danger")
    except Exception:
        logger.error("HSLU sync endpoint failed\n%s", traceback.format_exc())
        flash("HSLU sync failed unexpectedly.", "danger")

    if semester:
        if module:
            return redirect(url_for("hslu_semester_overview", semester=semester, module=module, sw=sw))
        return redirect(url_for("hslu_semester_overview", semester=semester, sw=sw))
    return redirect(url_for("hslu_semester_overview"))


@app.template_filter("render_hslu_inline_markdown")
def render_hslu_inline_markdown_filter(value: str) -> Markup:
    return Markup(_render_hslu_inline_markdown(value))


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
