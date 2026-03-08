import json
import traceback
from datetime import datetime
from pathlib import Path

from flask import Flask, flash, jsonify, redirect, render_template, request, send_file, url_for
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


def _safe_git_snapshot() -> dict:
    try:
        version_handler = DocsVersionHandler()
        return version_handler.get_status_snapshot()
    except Exception as exc:
        logger.error("Failed to fetch git status snapshot: %s", exc)
        return {"has_changes": False, "changes": [], "error": str(exc)}


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

    version_status = _safe_git_snapshot()

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
    version_status = _safe_git_snapshot()

    return render_template(
        "version_control.html",
        has_changes=version_status.get("has_changes", False),
        changes=version_status.get("changes", []),
    )


@app.route("/version_control/sync", methods=["POST"])
def version_control_sync():
    try:
        version_handler = DocsVersionHandler()
        snapshot = version_handler.get_status_snapshot()
        change_count = len(snapshot.get("changes", []))
        flash(f"Git status refreshed. {change_count} changed files detected in /the-knowledge/02_DOCS.", "success")
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


@app.route("/api/version_control/status", methods=["GET"])
def version_control_status_api():
    try:
        version_handler = DocsVersionHandler()
        return jsonify(version_handler.get_status_snapshot())
    except Exception as exc:
        return jsonify({"has_changes": False, "changes": [], "error": str(exc)}), 500


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


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
