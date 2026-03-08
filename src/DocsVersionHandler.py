import json
import subprocess
from dataclasses import dataclass
from pathlib import Path

from .logger import get_logger


logger = get_logger(__name__)


@dataclass
class FileChangeSummary:
    file_path: str
    additions: int
    deletions: int


class DocsVersionHandler:
    def __init__(self) -> None:
        conf_path = Path(__file__).resolve().parent.parent / "conf.json"
        conf_data = json.loads(conf_path.read_text(encoding="utf-8"))

        configured_git_dir = conf_data.get("git", {}).get("full_path_to_git_dir", ".git")
        configured_docs_dir = conf_data.get("docs", {}).get("full_path_to_docs", "/docs")
        configured_todo_file = conf_data.get("todo", {}).get("full_path_to_todo_file", "/todo/README.md")
        self.git_executable = conf_data.get("git", {}).get("executable", "git")

        git_dir_path = Path(configured_git_dir)
        if not git_dir_path.is_absolute():
            git_dir_path = (Path(__file__).resolve().parent.parent / git_dir_path).resolve()

        self.git_dir = git_dir_path if git_dir_path.exists() else Path("/git")
        self.docs_dir = Path(configured_docs_dir)
        self.todo_file = Path(configured_todo_file)
        self.work_tree = self._resolve_work_tree()
        self.docs_path_candidates = self._build_docs_path_candidates(self.docs_dir)
        self.docs_pathspecs = self._build_docs_pathspecs()

        logger.info(
            "DocsVersionHandler initialized with git=%s git_dir=%s work_tree=%s docs_candidates=%s",
            self.git_executable,
            self.git_dir,
            self.work_tree,
            sorted(self.docs_path_candidates),
        )

    def _build_docs_pathspecs(self) -> list[str]:
        pathspecs: list[str] = []

        resolved_work_tree = self.work_tree.resolve()

        if self.docs_dir.is_absolute():
            try:
                relative_docs = self.docs_dir.resolve().relative_to(resolved_work_tree)
                pathspecs.append(relative_docs.as_posix())
            except ValueError:
                pass
        else:
            pathspecs.append(self.docs_dir.as_posix().strip("/"))

        for candidate in sorted(self.docs_path_candidates):
            if candidate not in pathspecs:
                pathspecs.append(candidate)

        return [value for value in pathspecs if value]

    def _resolve_work_tree(self) -> Path:
        if self.git_dir.name == ".git":
            return self.git_dir.parent

        synthetic_root = Path("/tmp/the_knowledge_mapping_git_worktree")
        synthetic_root.mkdir(parents=True, exist_ok=True)

        self._ensure_symlink(self.docs_dir, synthetic_root / "02_DOCS")
        if self.docs_dir.name and self.docs_dir.name != "02_DOCS":
            self._ensure_symlink(self.docs_dir, synthetic_root / self.docs_dir.name)
        if self.todo_file.name:
            self._ensure_symlink(self.todo_file, synthetic_root / self.todo_file.name)

        return synthetic_root

    def _ensure_symlink(self, source: Path, destination: Path) -> None:
        if not source.exists():
            logger.warning("Symlink source not found: %s", source)
            return

        try:
            if destination.is_symlink() or destination.exists():
                if destination.is_symlink() and destination.resolve() == source.resolve():
                    return
                if destination.is_dir() and not destination.is_symlink():
                    return
                destination.unlink()
            destination.symlink_to(source)
        except OSError as exc:
            logger.warning("Failed to create symlink %s -> %s: %s", destination, source, exc)

    def _build_docs_path_candidates(self, docs_dir: Path) -> set[str]:
        candidates: set[str] = set()

        docs_raw = docs_dir.as_posix().strip()
        if docs_raw:
            candidates.add(docs_raw.lstrip("/"))

        if docs_dir.name:
            candidates.add(docs_dir.name)

        if docs_dir.is_absolute():
            try:
                relative_docs = docs_dir.resolve().relative_to(self.work_tree.resolve())
                candidates.add(relative_docs.as_posix())
            except ValueError:
                pass

        candidates.add("02_DOCS")
        return {value.strip("/") for value in candidates if value and value.strip("/")}

    def _is_docs_file(self, file_path: str) -> bool:
        normalized_path = self._normalize_path(file_path)
        for candidate in self.docs_path_candidates:
            if normalized_path == candidate or normalized_path.startswith(f"{candidate}/"):
                return True
        return False

    def _normalize_path(self, file_path: str) -> str:
        return file_path.strip().strip('"').strip("/")

    def _run_git_command(self, arguments: list[str]) -> str:
        command = [
            self.git_executable,
            f"--git-dir={self.git_dir}",
            f"--work-tree={self.work_tree}",
            *arguments,
        ]

        try:
            completed = subprocess.run(command, capture_output=True, text=True, check=False)
        except FileNotFoundError as exc:
            logger.error("Git executable not found: %s", self.git_executable)
            raise RuntimeError(f"Git executable not found: {self.git_executable}") from exc

        if completed.returncode != 0:
            stderr = completed.stderr.strip() or "unknown git error"
            logger.error("Git command failed: %s\n%s", " ".join(command), stderr)
            raise RuntimeError(stderr)

        return completed.stdout.strip()

    def get_status_snapshot(self) -> dict:
        changes = self.get_line_change_summary()
        return {
            "has_changes": bool(changes),
            "changes": changes,
        }

    def get_line_change_summary(self) -> list[dict]:
        numstat_output = self._run_git_command([
            "-c",
            "core.quotepath=off",
            "diff",
            "--numstat",
            "HEAD",
            "--",
            *self.docs_pathspecs,
        ])

        summaries: dict[str, FileChangeSummary] = {}

        if numstat_output:
            for row in numstat_output.splitlines():
                parts = row.split("\t")
                if len(parts) < 3:
                    continue

                additions_raw, deletions_raw, file_path = parts[0], parts[1], self._normalize_path(parts[2])
                if not self._is_docs_file(file_path):
                    continue

                additions = int(additions_raw) if additions_raw.isdigit() else 0
                deletions = int(deletions_raw) if deletions_raw.isdigit() else 0
                summaries[file_path] = FileChangeSummary(file_path=file_path, additions=additions, deletions=deletions)

        porcelain_output = self._run_git_command([
            "-c",
            "core.quotepath=off",
            "status",
            "--porcelain",
            "--untracked-files=all",
            "--",
            *self.docs_pathspecs,
        ])

        for row in porcelain_output.splitlines() if porcelain_output else []:
            file_path = self._extract_porcelain_path(row)
            if not file_path:
                continue
            if not self._is_docs_file(file_path):
                continue
            if file_path not in summaries:
                summaries[file_path] = FileChangeSummary(file_path=file_path, additions=0, deletions=0)


        result: list[dict] = []
        for change in sorted(summaries.values(), key=lambda item: item.file_path.lower()):
            if change.additions == 0 and change.deletions == 0:
                continue

            result.append(
                {
                    "file_path": change.file_path,
                    "additions": change.additions,
                    "deletions": change.deletions,
                }
            )

        return result


    def _extract_porcelain_path(self, row: str) -> str:
        if len(row) < 4:
            return ""

        path_part = row[3:].strip()
        if " -> " in path_part:
            path_part = path_part.split(" -> ", maxsplit=1)[1].strip()

        return self._normalize_path(path_part)
