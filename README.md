# the_knowledge_mapping

Flask-based GUI for scanning docs, storing metadata in SQLite, checking compliance, searching/filtering entries, deleting entries, and exporting results as Markdown.

## Features
- Run full document scan from the web UI.
- View an overview dashboard with compliance stats.
- Search/filter by ID, name, tag, or incompliant status.
- Delete entries by ID, by name, or all entries.
- Export current filtered result set to Markdown.

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
4. Open: `http://localhost:5000`

## Docker
### Build and run with Docker
```bash
docker build -t knowledge-mapping .
docker run --rm -p 5000:5000 -v $(pwd)/output:/app/output -v $(pwd)/logs:/app/logs -v $(pwd)/conf.json:/app/conf.json knowledge-mapping
```

### Build and run with Docker Compose
```bash
docker compose up --build
```

## Config
Adjust `conf.json`:
- `db.db_path`: database output path (default `output/docs.db`)
- `docs.full_path_to_docs`: absolute path to docs folder to scan (default `/the-knowledge/02_DOCS`)
- `todo.full_path_to_todo_file`: absolute path to TODO markdown file (default `/the-knowledge/README.md`)
- `git.full_path_to_git_dir`: absolute path to git directory (default `/the-knowledge/.git`)
- `log.log_file_path`: log file location (default `logs/app.log`)

Timestamps for syncs and logs are generated in the `Europe/Zurich` timezone.

> The docs path must be accessible from where the app runs (host/container).
