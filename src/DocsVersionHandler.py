import json
import subprocess
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlparse

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

        configured_git_dir = conf_data.get("git", {}).get("full_path_to_git_dir", "/the-knowledge/.git")
        configured_docs_dir = conf_data.get("docs", {}).get("full_path_to_docs", "/the-knowledge/02_DOCS")
        configured_todo_file = conf_data.get("todo", {}).get("full_path_to_todo_file", "/the-knowledge/README.md")
        git_conf = conf_data.get("git", {})
        self.git_executable = git_conf.get("executable", "git")
        self.git_username = str(git_conf.get("username", "")).strip()
        self.git_email = str(git_conf.get("email", "")).strip()

        self.git_dir = self._resolve_configured_path(configured_git_dir)
        self.docs_dir = self._resolve_configured_path(configured_docs_dir)
        self.todo_file = self._resolve_configured_path(configured_todo_file)

        self.work_tree = self._resolve_work_tree()
        self.docs_path_candidates = self._build_docs_path_candidates()
        self.docs_pathspecs = self._build_docs_pathspecs()

        logger.info(
            "DocsVersionHandler initialized with git=%s git_dir=%s work_tree=%s docs_candidates=%s",
            self.git_executable,
            self.git_dir,
            self.work_tree,
            sorted(self.docs_path_candidates),
        )

    def _resolve_configured_path(self, configured_path: str) -> Path:
        path = Path(configured_path)
        if path.is_absolute():
            return path
        return (Path(__file__).resolve().parent.parent / path).resolve()

    def _resolve_work_tree(self) -> Path:
        if self.git_dir.name == ".git":
            return self.git_dir.parent

        return self.git_dir

    def _build_docs_pathspecs(self) -> list[str]:
        pathspecs: list[str] = []

        for candidate in sorted(self.docs_path_candidates):
            if candidate not in pathspecs:
                pathspecs.append(candidate)

        return [value for value in pathspecs if value]

    def _display_name(self, file_path: str) -> str:
        normalized_path = self._normalize_path(file_path)

        for candidate in sorted(self.docs_path_candidates, key=len, reverse=True):
            if normalized_path == candidate:
                return Path(candidate).name
            if normalized_path.startswith(f"{candidate}/"):
                normalized_path = normalized_path[len(candidate) + 1 :]
                break

        return Path(normalized_path).stem

    def _build_docs_path_candidates(self) -> set[str]:
        candidates: set[str] = set()

        docs_raw = self.docs_dir.as_posix().strip()
        if docs_raw:
            candidates.add(docs_raw.lstrip("/"))

        if self.docs_dir.name:
            candidates.add(self.docs_dir.name)

        if self.docs_dir.is_absolute():
            try:
                relative_docs = self.docs_dir.resolve().relative_to(self.work_tree.resolve())
                candidates.add(relative_docs.as_posix())
            except ValueError:
                pass

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
        return_code, stdout, stderr = self._run_git_command_with_code(arguments)
        if return_code != 0:
            logger.error("Git command failed: %s\n%s", " ".join([self.git_executable, *arguments]), stderr or "unknown git error")
            raise RuntimeError(stderr or "unknown git error")
        return stdout


    def _build_git_identity_args(self) -> list[str]:
        args: list[str] = []
        if self.git_username:
            args.extend(["-c", f"user.name={self.git_username}"])
        if self.git_email:
            args.extend(["-c", f"user.email={self.git_email}"])
        return args

    def _run_git_command_with_code(self, arguments: list[str]) -> tuple[int, str, str]:
        command = [
            self.git_executable,
            *self._build_git_identity_args(),
            f"--git-dir={self.git_dir}",
            f"--work-tree={self.work_tree}",
            *arguments,
        ]

        try:
            completed = subprocess.run(command, capture_output=True, text=True, check=False)
        except FileNotFoundError as exc:
            logger.error("Git executable not found: %s", self.git_executable)
            raise RuntimeError(f"Git executable not found: {self.git_executable}") from exc

        return_code = completed.returncode
        stdout = completed.stdout.strip()
        stderr = completed.stderr.strip()

        if return_code != 0 and self._is_https_auth_error(stderr):
            fallback_result = self._run_git_command_with_ssh_origin_fallback(arguments)
            if fallback_result is not None:
                return fallback_result

        return return_code, stdout, stderr

    def _is_https_auth_error(self, stderr: str) -> bool:
        error_text = (stderr or "").lower()
        return "could not read username for 'https://" in error_text

    def _run_git_command_with_ssh_origin_fallback(self, arguments: list[str]) -> tuple[int, str, str] | None:
        if not arguments:
            return None

        git_action = arguments[0]
        if git_action not in {"pull", "push", "fetch"}:
            return None

        ssh_origin = self._get_ssh_origin_url()
        if not ssh_origin:
            return None

        fallback_arguments = [git_action, ssh_origin, *arguments[1:]]
        fallback_command = [
            self.git_executable,
            *self._build_git_identity_args(),
            f"--git-dir={self.git_dir}",
            f"--work-tree={self.work_tree}",
            *fallback_arguments,
        ]

        logger.warning(
            "HTTPS authentication failed; retrying git command over SSH: %s",
            " ".join([self.git_executable, *fallback_arguments]),
        )

        fallback_completed = subprocess.run(fallback_command, capture_output=True, text=True, check=False)
        return (
            fallback_completed.returncode,
            fallback_completed.stdout.strip(),
            fallback_completed.stderr.strip(),
        )

    def _get_ssh_origin_url(self) -> str:
        origin_code, origin_stdout, _ = self._run_git_command_raw(["remote", "get-url", "origin"])
        if origin_code != 0:
            return ""

        return self._convert_remote_to_ssh(origin_stdout.strip())

    def _run_git_command_raw(self, arguments: list[str]) -> tuple[int, str, str]:
        command = [
            self.git_executable,
            *self._build_git_identity_args(),
            f"--git-dir={self.git_dir}",
            f"--work-tree={self.work_tree}",
            *arguments,
        ]

        try:
            completed = subprocess.run(command, capture_output=True, text=True, check=False)
        except FileNotFoundError as exc:
            logger.error("Git executable not found: %s", self.git_executable)
            raise RuntimeError(f"Git executable not found: {self.git_executable}") from exc

        return completed.returncode, completed.stdout.strip(), completed.stderr.strip()

    def _convert_remote_to_ssh(self, remote_url: str) -> str:
        normalized_url = (remote_url or "").strip()
        if not normalized_url:
            return ""

        if normalized_url.startswith("git@"):
            return normalized_url

        if normalized_url.startswith("ssh://"):
            return normalized_url

        if normalized_url.startswith("http://") or normalized_url.startswith("https://"):
            parsed_url = urlparse(normalized_url)
            if parsed_url.hostname and parsed_url.path:
                repo_path = parsed_url.path.lstrip("/")
                return f"git@{parsed_url.hostname}:{repo_path}"

        if "@" in normalized_url and ":" in normalized_url and not normalized_url.startswith("/"):
            return normalized_url

        return ""


    def revert_file(self, file_path: str) -> None:
        normalized_path = self._normalize_path(file_path)
        if not normalized_path:
            raise ValueError("file path is required")
        if not self._is_docs_file(normalized_path):
            raise ValueError("only files inside the docs directory can be reverted")

        restore_result = self._run_git_command_with_code([
            "restore",
            "--staged",
            "--worktree",
            "--",
            normalized_path,
        ])

        if restore_result[0] == 0:
            return

        tracked_result = self._run_git_command_with_code([
            "ls-files",
            "--error-unmatch",
            "--",
            normalized_path,
        ])

        if tracked_result[0] != 0:
            clean_result = self._run_git_command_with_code([
                "clean",
                "-f",
                "--",
                normalized_path,
            ])
            if clean_result[0] != 0:
                raise RuntimeError(clean_result[2] or restore_result[2] or "failed to clean untracked file")
            return

        raise RuntimeError(restore_result[2] or "failed to restore file")

    def get_status_snapshot(self) -> dict:
        changes = self.get_line_change_summary()
        remote_status = self.get_remote_update_status()
        return {
            "has_changes": bool(changes),
            "changes": changes,
            "remote_status": remote_status,
        }

    def get_remote_update_status(self) -> dict:
        upstream_code, upstream_stdout, _ = self._run_git_command_with_code(
            ["rev-parse", "--abbrev-ref", "--symbolic-full-name", "@{u}"]
        )
        if upstream_code != 0:
            return {
                "has_remote_changes": False,
                "message": "No upstream tracking branch configured.",
                "upstream": "",
            }

        upstream_branch = upstream_stdout.strip()
        fetch_code, _, fetch_stderr = self._run_git_command_with_code(["fetch", "--quiet"])
        if fetch_code != 0:
            return {
                "has_remote_changes": False,
                "message": f"Could not check remote updates: {fetch_stderr or 'git fetch failed'}",
                "upstream": upstream_branch,
            }

        count_code, count_stdout, count_stderr = self._run_git_command_with_code(
            ["rev-list", "--left-right", "--count", "@{u}...HEAD"]
        )
        if count_code != 0:
            return {
                "has_remote_changes": False,
                "message": f"Could not compare with remote: {count_stderr or 'git rev-list failed'}",
                "upstream": upstream_branch,
            }

        counts = count_stdout.split()
        if len(counts) != 2:
            return {
                "has_remote_changes": False,
                "message": "Could not parse remote comparison status.",
                "upstream": upstream_branch,
            }

        behind_count = int(counts[0]) if counts[0].isdigit() else 0
        ahead_count = int(counts[1]) if counts[1].isdigit() else 0

        if behind_count > 0:
            return {
                "has_remote_changes": True,
                "message": f"Remote has {behind_count} newer commit(s). Please run git pull.",
                "upstream": upstream_branch,
                "behind": behind_count,
                "ahead": ahead_count,
            }

        if ahead_count > 0:
            return {
                "has_remote_changes": False,
                "message": f"Local branch is ahead of remote by {ahead_count} commit(s).",
                "upstream": upstream_branch,
                "behind": behind_count,
                "ahead": ahead_count,
            }

        return {
            "has_remote_changes": False,
            "message": "Local and remote are in sync.",
            "upstream": upstream_branch,
            "behind": behind_count,
            "ahead": ahead_count,
        }

    def pull_latest(self) -> str:
        output = self._run_git_command(["pull"])
        return output or "Already up to date."

    def commit_and_push(self, message: str) -> str:
        commit_message = (message or "").strip()
        if not commit_message:
            raise ValueError("commit message is required")

        self._run_git_command(["add", "-A"])

        commit_code, commit_stdout, commit_stderr = self._run_git_command_with_code(["commit", "-m", commit_message])
        if commit_code != 0:
            combined_output = "\n".join(value for value in [commit_stdout, commit_stderr] if value).strip()
            no_changes_messages = ["nothing to commit", "no changes added to commit"]
            if any(message in combined_output.lower() for message in no_changes_messages):
                raise RuntimeError("No changes to commit.")
            raise RuntimeError(commit_stderr or commit_stdout or "failed to commit changes")

        push_output = self._run_git_command(["push"])
        commit_summary = commit_stdout or "Commit created successfully."
        if push_output:
            return f"{commit_summary}\n{push_output}"
        return commit_summary

    def get_line_change_summary(self) -> list[dict]:
        numstat_output = self._run_git_command([
            "-c",
            "core.quotepath=off",
            "diff",
            "--ignore-cr-at-eol",
            "--numstat",
            "HEAD",
            "--",
        ])

        summaries: dict[str, FileChangeSummary] = {}

        if numstat_output:
            for row in numstat_output.splitlines():
                parts = row.split("\t")
                if len(parts) < 3:
                    continue

                additions_raw, deletions_raw, file_path = parts[0], parts[1], self._normalize_path(parts[2])
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
        ])

        for row in porcelain_output.splitlines() if porcelain_output else []:
            file_path = self._extract_porcelain_path(row)
            if not file_path:
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
                    "display_name": self._display_name(change.file_path),
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
