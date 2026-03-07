import argparse
import json
import traceback
from pathlib import Path

from src.DatabaseConnector import db
from src.DocsParser import DocsParser
from src.logger import get_logger


logger = get_logger(__name__)


def print_helper_banner() -> None:
    help_message = """
    Usage: python3 main.py [options]

    Help:
    --help                  Display this help message

    Options:
    --run                   Run full scan of docs (also updates old entries)

    --get-by-id [id]        Gets an entry by id
    --get-by-name [name]    Gets an entry by file-name
    --get-by-tag [tag]      Gets entries by tag
    --get-incompliant       Gets all incompliant files

    --delete-by-id          Deletes an entry by id
    --delete-by-name        Deletes an entry by file-name
    --delete-all            Deletes ALL DB-Entries! Be careful!!

    --delete-changes        Deletes ALL the Changes from the DB

    --export-result         Exports Results to a .MD-File
    """
    print(help_message)


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


def _as_markdown_list(value) -> str:
    normalized = _normalize_value(value)
    if isinstance(normalized, list):
        if len(normalized) == 0:
            return "- N/A"
        return "\n".join(f"- {item}" for item in normalized)

    if normalized in (None, "", "N/A"):
        return "- N/A"

    return f"- {normalized}"


def export_result_to_markdown(results: dict) -> Path:
    output_dir = Path(__file__).resolve().parent / "output"
    output_dir.mkdir(parents=True, exist_ok=True)
    export_path = output_dir / "results.md"

    lines = ["# Exported docs results", ""]

    if len(results) == 0:
        lines.append("_No results found._")
    else:
        for doc_id in sorted(results.keys()):
            row = results[doc_id]
            lines.extend(
                [
                    f"## {row.get('title', 'N/A')} (ID: {row.get('id', 'N/A')})",
                    "",
                    f"- **Created at:** {row.get('created_at', 'N/A')}",
                    f"- **Changed at:** {row.get('changed_at', 'N/A')}",
                    f"- **Is compliant:** {row.get('is_compliant', 'N/A')}",
                    f"- **Last sync:** {row.get('last_sync', 'N/A')}",
                    "",
                    "### Tags",
                    _as_markdown_list(row.get("tags", "N/A")),
                    "",
                    "### Links",
                    _as_markdown_list(row.get("links", "N/A")),
                    "",
                    "### Video links",
                    _as_markdown_list(row.get("video_links", "N/A")),
                    "",
                ]
            )

    export_path.write_text("\n".join(lines), encoding="utf-8")
    return export_path


def _print_results(results: dict) -> None:
    print(json.dumps(results, indent=2, ensure_ascii=False))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--help", action="store_true")

    parser.add_argument("--run", action="store_true")

    parser.add_argument("--get-by-id", type=int)
    parser.add_argument("--get-by-name", type=str)
    parser.add_argument("--get-by-tag", type=str)
    parser.add_argument("--get-incompliant", action="store_true")

    parser.add_argument("--delete-by-id", type=int)
    parser.add_argument("--delete-by-name", type=str)
    parser.add_argument("--delete-all", action="store_true")

    parser.add_argument("--delete-changes", action="store_true")

    parser.add_argument("--export-result", action="store_true")

    return parser


def main():
    try:
        parser = build_parser()
        args = parser.parse_args()

        if args.help:
            print_helper_banner()
            return

        db_object = db()
        result = None

        if args.run:
            logger.info("Starting full document scan from CLI")
            docs_parser_obj = DocsParser()
            docs_parser_obj.parse_and_add_ALL_docs_to_db()
            logger.info("Full document scan completed")

        if args.get_by_id is not None:
            result = db_object.get_docs_by_id(args.get_by_id)

        if args.get_by_name is not None:
            result = db_object.get_docs_by_name(args.get_by_name)

        if args.get_by_tag is not None:
            result = db_object.get_docs_by_tag(args.get_by_tag)

        if args.get_incompliant:
            result = db_object.get_non_compliant_docs()

        if args.delete_by_id is not None:
            db_object.delete_docs_by_id(args.delete_by_id)
            logger.info("Deleted entry by id=%s", args.delete_by_id)
            print(f"Deleted entry with id={args.delete_by_id}")

        if args.delete_by_name is not None:
            db_object.delete_docs_by_name(args.delete_by_name)
            logger.info("Deleted entries by name=%s", args.delete_by_name)
            print(f"Deleted entries with name={args.delete_by_name}")

        if args.delete_all:
            db_object.delete_all_docs()
            logger.warning("Deleted all entries from database")
            print("Deleted all entries")

        if args.delete_changes:
            db_object.delete_all_changes()
            logger.warning("Deleted all changes from database")
            print("Deleted all changes")

        if args.export_result:
            if result is None:
                result = db_object.get_all_docs()

            export_path = export_result_to_markdown(result)
            logger.info("Exported results to %s", export_path)
            print(f"Exported results to {export_path}")

        if result is not None and not args.export_result:
            _print_results(result)

        if not any(vars(args).values()):
            print_helper_banner()

    except Exception:
        logger.error("Unhandled exception in CLI execution\n%s", traceback.format_exc())


if __name__ == "__main__":
    main()
