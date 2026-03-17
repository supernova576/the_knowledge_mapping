# The Knowledge Editing Tool

Web application for scanning Markdown documents, storing metadata in SQLite, and managing compliance, TODO tracking, version control sync, and HSLU semester views.

> **Disclaimer:** This README was written/updated with the help of AI.

## Current feature set

- **Dashboard overview** (`/`)
  - Run a full document scan.
  - See sync status, loaded docs, compliant docs, incompliant docs, and the count of documents in construction.
  - Filter documents by ID, name, tag, compliant, incompliant, and under construction.
  - Sort documents by title, created date, or changed date.
  - Set a manual compliance override for selected documents.
- **Version control view** (`/version_control`)
  - Inspect documentation repo change state.
  - Sync status snapshots and run file revert actions.
- **TODO view** (`/todo`)
  - Inspect TODO entries, sync TODO file to DB, add/delete TODOs, and update progress.
  - Create docs from templates and optionally write page history entries.
- **HSLU semester views**
  - `/hslu/semester_overview`: filter by semester/module/software status and maintain status values.
  - `/hslu/semester_checklist`: checklist-style view and status sync.

## Run locally

1. Create and activate a Python environment.
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
3. Start the app:
   ```bash
   python app.py
   ```
4. Open in browser:
   - `http://localhost:5000`

## Docker

### Build and run with Docker

```bash
docker build -t knowledge-mapping .
docker run --rm -p 5000:5000 \
  -v $(pwd)/output:/app/output \
  -v $(pwd)/logs:/app/logs \
  -v $(pwd)/conf.json:/app/conf.json \
  knowledge-mapping
```

### Build and run with Docker Compose

```bash
docker compose up -d --build
```

## Configuration

Adjust values in `conf.json`:

- `db.db_path`: database output path (default `output/docs.db`)
- `docs.full_path_to_docs`: absolute path to docs folder to scan (default `/the-knowledge/02_DOCS`)
- `todo.full_path_to_todo_file`: absolute path to TODO markdown file (default `/the-knowledge/README.md`)
- `git.full_path_to_git_dir`: absolute path to git directory (default `/the-knowledge/.git`)
- `log.log_file_path`: log file location (default `logs/app.log`)

Timestamps for syncs/logging are generated in the `Europe/Zurich` timezone.

## Notes

- The docs path must be accessible from where the app runs (host/container).
- The UI and parser behavior depend on the structure/content of your Markdown docs and configured paths.

## Features to implement
- document exporter
  - export by Tag / Name
  - convert exported doc to pdf
  - generate table of contents
  - display with PICTURES