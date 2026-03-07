import json
import traceback
from pathlib import Path

from flask import Flask, flash, redirect, render_template, request, send_file, url_for

from main import export_result_to_markdown
from src.DatabaseConnector import db
from src.DocsParser import DocsParser


app = Flask(__name__)
app.secret_key = "knowledge-mapping-secret"


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


def _load_docs(view: str, query: str) -> dict:
    database = db()

    if view == "id" and query:
        try:
            return database.get_docs_by_id(int(query))
        except ValueError:
            flash("ID must be a number", "danger")
            return database.get_all_docs()

    if view == "name" and query:
        return database.get_docs_by_name(query)

    if view == "tag" and query:
        return database.get_docs_by_tag(query)

    if view == "incompliant":
        return database.get_non_compliant_docs()

    return database.get_all_docs()


@app.route("/", methods=["GET"])
def index():
    view = request.args.get("view", "all")
    query = request.args.get("q", "").strip()

    docs = _load_docs(view, query)

    total_docs = len(docs)
    compliant_docs = len([d for d in docs.values() if d.get("is_compliant") == "true"])
    incompliant_docs = len([d for d in docs.values() if d.get("is_compliant") == "false"])

    processed_docs = []
    for item in docs.values():
        row = dict(item)
        row["tags_list"] = _to_display_list(row.get("tags"))
        row["links_list"] = _to_display_list(row.get("links"))
        row["video_links_list"] = _to_display_list(row.get("video_links"))
        processed_docs.append(row)

    processed_docs.sort(key=lambda x: x.get("id", 0))

    return render_template(
        "index.html",
        docs=processed_docs,
        total_docs=total_docs,
        compliant_docs=compliant_docs,
        incompliant_docs=incompliant_docs,
        selected_view=view,
        query=query,
    )


@app.route("/scan", methods=["POST"])
def scan_docs():
    try:
        parser = DocsParser()
        parser.parse_and_add_ALL_docs_to_db()
        flash("Scan completed successfully.", "success")
    except BaseException as exc:
        if isinstance(exc, SystemExit):
            flash(
                "Scan failed: parser exited early. Check docs path in conf.json and parser/database logs.",
                "danger",
            )
        else:
            flash(traceback.format_exc(), "danger")

    return redirect(url_for("index"))


@app.route("/delete/id", methods=["POST"])
def delete_by_id():
    doc_id = request.form.get("doc_id", "").strip()
    if not doc_id:
        flash("Please provide a document ID.", "warning")
        return redirect(url_for("index"))

    try:
        database = db()
        database.delete_docs_by_id(int(doc_id))
        flash(f"Deleted entry with id={doc_id}", "success")
    except ValueError:
        flash("ID must be numeric.", "danger")

    return redirect(url_for("index"))


@app.route("/delete/name", methods=["POST"])
def delete_by_name():
    name = request.form.get("name", "").strip()
    if not name:
        flash("Please provide a file name.", "warning")
        return redirect(url_for("index"))

    database = db()
    database.delete_docs_by_name(name)
    flash(f"Deleted entries with name={name}", "success")
    return redirect(url_for("index"))


@app.route("/delete/all", methods=["POST"])
def delete_all():
    database = db()
    database.delete_all_docs()
    flash("Deleted all entries.", "success")
    return redirect(url_for("index"))


@app.route("/export", methods=["GET"])
def export_results():
    view = request.args.get("view", "all")
    query = request.args.get("q", "").strip()
    docs = _load_docs(view, query)

    export_path = export_result_to_markdown(docs)
    return send_file(export_path, as_attachment=True, download_name="results.md")


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
