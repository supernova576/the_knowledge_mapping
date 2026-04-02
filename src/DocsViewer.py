import json
import re
import traceback
from urllib.parse import quote
from pathlib import Path

import bleach
import mistune

from .logger import get_logger


logger = get_logger(__name__)


class DocsViewer:
    ALLOWED_TAGS = [
        "a",
        "blockquote",
        "br",
        "code",
        "em",
        "h1",
        "h2",
        "h3",
        "h4",
        "h5",
        "h6",
        "hr",
        "li",
        "ol",
        "p",
        "pre",
        "strong",
        "ul",
        "table",
        "thead",
        "tbody",
        "tr",
        "th",
        "td",
        "img",
    ]
    ALLOWED_ATTRS = {
        "a": ["href", "title", "rel", "target"],
        "img": ["src", "alt", "title", "loading", "decoding"],
    }
    ALLOWED_PROTOCOLS = ["http", "https", "mailto"]

    def __init__(self, conf: dict | None = None) -> None:
        try:
            self.conf = conf or self._load_conf()
            self.docs_root = Path(self.conf.get("docs", {}).get("full_path_to_docs", "")).resolve()
            self.pictures_root = Path(self.conf.get("pictures", {}).get("full_path_to_pictures", "")).resolve()
            self._markdown = mistune.create_markdown(escape=False, plugins=["table"])
            if not self.docs_root.exists() or not self.docs_root.is_dir():
                raise FileNotFoundError(f"Configured docs path does not exist: {self.docs_root}")
            if not self.pictures_root.exists() or not self.pictures_root.is_dir():
                raise FileNotFoundError(f"Configured pictures path does not exist: {self.pictures_root}")
            logger.info("DocsViewer initialized docs_root=%s", self.docs_root)
        except Exception:
            logger.error("DocsViewer initialization failed\n%s", traceback.format_exc())
            raise

    def _load_conf(self) -> dict:
        conf_path = Path(__file__).resolve().parent.parent / "conf.json"
        return json.loads(conf_path.read_text(encoding="utf-8"))

    def _normalize_md_filename(self, file_name: str) -> str:
        cleaned = str(file_name or "").strip().replace("\\", "/")
        if not cleaned:
            raise ValueError("File name is required.")
        if cleaned.startswith("/"):
            raise ValueError("Absolute paths are not allowed.")
        cleaned = cleaned.lstrip("./")
        if not cleaned.lower().endswith(".md"):
            cleaned = f"{cleaned}.md"
        return cleaned

    def _resolve_doc_path(self, file_name: str) -> Path:
        normalized = self._normalize_md_filename(file_name)
        target = (self.docs_root / normalized).resolve()

        if self.docs_root not in target.parents:
            raise ValueError("Selected file is outside the configured docs directory.")
        if not target.exists() or not target.is_file():
            raise FileNotFoundError(f"Markdown file not found: {normalized}")

        return target

    def _resolve_doc_relative_path(self, relative_path: str) -> Path:
        normalized = self._normalize_md_filename(relative_path)
        target = (self.docs_root / normalized).resolve()
        if self.docs_root != target and self.docs_root not in target.parents:
            raise ValueError("Selected file is outside the configured docs directory.")
        if not target.exists() or not target.is_file():
            raise FileNotFoundError(f"Markdown file not found: {normalized}")
        return target

    def _slugify(self, value: str) -> str:
        slug = re.sub(r"[^a-zA-Z0-9._ -]+", "", str(value or "").strip()).strip()
        slug = re.sub(r"\s+", "-", slug)
        return slug.lower()

    def _resolve_wikilink_target(self, target_note: str, current_doc_path: Path) -> Path:
        cleaned = str(target_note or "").strip().replace("\\", "/")
        if not cleaned:
            raise ValueError("Invalid wikilink target.")

        cleaned_note = cleaned.split("#", 1)[0].strip()
        if not cleaned_note:
            raise ValueError("Invalid wikilink target.")

        candidates: list[Path] = []
        link_path = Path(cleaned_note)
        suffix = link_path.suffix.lower()
        if suffix == ".md":
            candidates.append((self.docs_root / link_path).resolve())
            candidates.append((current_doc_path.parent / link_path).resolve())
        else:
            candidates.append((self.docs_root / f"{cleaned_note}.md").resolve())
            candidates.append((current_doc_path.parent / f"{cleaned_note}.md").resolve())

        for candidate in candidates:
            if self.docs_root != candidate and self.docs_root not in candidate.parents:
                continue
            if candidate.exists() and candidate.is_file():
                return candidate

        raise FileNotFoundError(f"Could not resolve wikilink target: {target_note}")

    def _replace_wikilinks(self, markdown_text: str, current_doc_path: Path) -> str:
        def _replacer(match: re.Match[str]) -> str:
            raw = match.group(1)
            target_part, alias_part = (raw.split("|", 1) + [""])[:2]
            target_note = str(target_part).strip()
            alias = str(alias_part).strip() or target_note
            if not target_note:
                return alias
            try:
                resolved_target = self._resolve_wikilink_target(target_note, current_doc_path)
                relative_target = resolved_target.relative_to(self.docs_root).as_posix()
                href = f"/docs/view/by-path/{quote(relative_target, safe='/')}"
            except Exception:
                href = f"/docs/view/by-name/{self._slugify(target_note)}"
            return f"[{alias}]({href})"

        return re.sub(r"\[\[([^\]]+)\]\]", _replacer, str(markdown_text or ""))

    def _replace_wiki_images(self, markdown_text: str) -> str:
        def _replacer(match: re.Match[str]) -> str:
            raw = str(match.group(1) or "")
            target_part, alias_part = (raw.split("|", 1) + [""])[:2]
            target_asset = str(target_part).strip()
            alt_text = str(alias_part).strip()
            if not target_asset:
                return ""

            file_name = Path(target_asset).name
            if not file_name:
                return ""

            image_path = (self.pictures_root / file_name).resolve()
            if self.pictures_root != image_path and self.pictures_root not in image_path.parents:
                return ""
            if not image_path.exists() or not image_path.is_file():
                logger.warning("Referenced wiki image not found: %s", target_asset)
                return f"![{alt_text or file_name}]()"

            safe_name = quote(file_name, safe="")
            return f"![{alt_text or file_name}](/docs/pictures/{safe_name})"

        return re.sub(r"!\[\[([^\]]+)\]\]", _replacer, str(markdown_text or ""))

    def _sanitize_html(self, html_content: str) -> str:
        return bleach.clean(
            str(html_content or ""),
            tags=self.ALLOWED_TAGS,
            attributes=self.ALLOWED_ATTRS,
            protocols=self.ALLOWED_PROTOCOLS,
            strip=True,
        )

    def render_doc_to_html(self, file_name: str) -> tuple[str, str]:
        try:
            path = self._resolve_doc_path(file_name)
            markdown_text = path.read_text(encoding="utf-8")
            preprocessed = self._replace_wiki_images(markdown_text)
            preprocessed = self._replace_wikilinks(preprocessed, path)
            rendered_html = self._markdown(preprocessed)
            sanitized_html = self._sanitize_html(rendered_html)
            return path.stem, sanitized_html
        except Exception:
            logger.error("Failed to render markdown preview for %s\n%s", file_name, traceback.format_exc())
            raise

    def find_filename_by_slug(self, slug: str) -> str:
        try:
            normalized_slug = self._slugify(slug)
            if not normalized_slug:
                raise ValueError("Invalid note slug.")

            for candidate in self.docs_root.rglob("*.md"):
                candidate_slug = self._slugify(candidate.stem)
                if candidate_slug == normalized_slug:
                    return candidate.name

            raise FileNotFoundError(f"No markdown note found for slug: {slug}")
        except Exception:
            logger.error("Failed to resolve markdown slug=%s\n%s", slug, traceback.format_exc())
            raise

    def render_doc_to_html_by_relative_path(self, relative_path: str) -> tuple[str, str]:
        try:
            path = self._resolve_doc_relative_path(relative_path)
            markdown_text = path.read_text(encoding="utf-8")
            preprocessed = self._replace_wiki_images(markdown_text)
            preprocessed = self._replace_wikilinks(preprocessed, path)
            rendered_html = self._markdown(preprocessed)
            sanitized_html = self._sanitize_html(rendered_html)
            return path.stem, sanitized_html
        except Exception:
            logger.error(
                "Failed to render markdown preview for relative path=%s\n%s",
                relative_path,
                traceback.format_exc(),
            )
            raise
