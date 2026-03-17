import json
import re
import traceback
from pathlib import Path
from sys import exit as adieu

from .logger import get_logger


logger = get_logger(__name__)


class DocsExporter:
    IGNORE_SECTION_HEADING = "## Zusätzliche Ressourcen"

    def __init__(self) -> None:
        try:
            conf_path = Path(__file__).resolve().parent.parent / "conf.json"
            with open(conf_path, "r", encoding="utf-8") as conf_file:
                conf = json.loads(conf_file.read())

            self.docs_root = Path(conf.get("docs", {}).get("full_path_to_docs", "")).resolve()
            self.images_root = (self.docs_root.parent / "04_IMAGES").resolve()
            self.export_dir = (Path(__file__).resolve().parent.parent / "output" / "exports").resolve()
            self.export_dir.mkdir(parents=True, exist_ok=True)
            logger.info("Docs exporter initialized docs_root=%s images_root=%s", self.docs_root, self.images_root)
        except Exception:
            logger.error("DocsExporter initialization failed\n%s", traceback.format_exc())
            adieu(1)

    def _safe_pdf_name(self, title: str) -> str:
        normalized = re.sub(r"[^a-zA-Z0-9_-]+", "_", str(title or "export").strip()).strip("_")
        return f"{normalized or 'export'}.pdf"

    def _parse_db_array(self, value: str) -> list[str]:
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

    def _resolve_doc_path(self, doc_title: str) -> Path | None:
        sanitized = str(doc_title or "").strip().replace("/", "").replace("\\", "")
        if not sanitized:
            return None

        direct = self.docs_root / f"{sanitized}.md"
        if direct.exists() and direct.is_file():
            return direct

        exact_name = f"{sanitized}.md"
        for file_path in self.docs_root.rglob("*.md"):
            if file_path.name == exact_name or file_path.stem == sanitized:
                return file_path
        return None

    def _strip_ignored_section(self, markdown: str) -> str:
        pattern = re.compile(rf"(?ims)^\s*{re.escape(self.IGNORE_SECTION_HEADING)}\s*$.*", re.MULTILINE)
        return re.sub(pattern, "", markdown).strip()

    def _extract_toc_entries(self, markdown: str) -> list[tuple[int, str]]:
        entries: list[tuple[int, str]] = []
        for line in markdown.splitlines():
            match = re.match(r"^(#{2,4})\s+(.+?)\s*$", line.strip())
            if not match:
                continue
            level = len(match.group(1)) - 1
            text = match.group(2).strip()
            entries.append((level, text))
        return entries

    def _extract_obsidian_images(self, markdown: str) -> list[str]:
        images = re.findall(r"!\[\[([^\]|]+)(?:\|[^\]]+)?\]\]", markdown)
        deduped: list[str] = []
        for image_name in images:
            cleaned = Path(str(image_name).strip()).name
            if cleaned and cleaned not in deduped:
                deduped.append(cleaned)
        return deduped

    def _to_display_line(self, markdown_line: str) -> str:
        text = str(markdown_line or "")
        text = re.sub(r"\*\*(.*?)\*\*", r"\1", text)
        text = re.sub(r"\*(.*?)\*", r"\1", text)
        text = re.sub(r"\[\[([^\]|]+)\|([^\]]+)\]\]", r"\2", text)
        text = re.sub(r"\[\[([^\]]+)\]\]", r"\1", text)
        text = re.sub(r"\[([^\]]+)\]\(([^)]+)\)", r"\1 (\2)", text)
        return text.strip()

    def export_docs_to_pdf(self, export_title: str, docs: list[dict]) -> Path:
        try:
            from fpdf import FPDF
        except ImportError as exc:
            raise RuntimeError("Missing dependency fpdf2. Install requirements before using export.") from exc

        ordered_docs = sorted(docs, key=lambda doc: str(doc.get("title", "")).casefold())
        file_name = self._safe_pdf_name(export_title)
        output_path = self.export_dir / file_name

        pdf = FPDF()
        pdf.set_auto_page_break(auto=True, margin=15)

        pdf.add_page()
        pdf.set_fill_color(25, 135, 84)
        pdf.set_text_color(255, 255, 255)
        pdf.set_font("Helvetica", "B", 28)
        pdf.rect(10, 20, 190, 40, "F")
        pdf.set_xy(14, 32)
        pdf.multi_cell(182, 10, export_title.strip() or "Documentation Export", align="C")

        toc_sections: list[tuple[str, list[tuple[int, str]]]] = []
        body_docs: list[dict] = []
        all_links: dict[str, list[str]] = {}
        all_video_links: dict[str, list[str]] = {}

        for doc in ordered_docs:
            title = str(doc.get("title", "")).strip()
            doc_path = self._resolve_doc_path(title)
            if not doc_path:
                logger.warning("Doc file not found for title=%s", title)
                continue

            content = doc_path.read_text(encoding="utf-8")
            cleaned_content = self._strip_ignored_section(content)
            toc_sections.append((title, self._extract_toc_entries(cleaned_content)))
            body_docs.append({"title": title, "content": cleaned_content})
            all_links[title] = self._parse_db_array(doc.get("links", "N/A"))
            all_video_links[title] = self._parse_db_array(doc.get("video_links", "N/A"))

        pdf.add_page()
        pdf.set_text_color(0, 0, 0)
        pdf.set_font("Helvetica", "B", 18)
        pdf.cell(0, 10, "Table of Contents", ln=True)
        pdf.ln(2)
        pdf.set_font("Helvetica", size=11)

        for doc_title, sections in toc_sections:
            pdf.set_font("Helvetica", "B", 12)
            pdf.multi_cell(0, 7, f"- {doc_title}")
            pdf.set_font("Helvetica", size=11)
            for level, section_title in sections:
                indent = "  " * max(level - 1, 1)
                pdf.multi_cell(0, 6, f"{indent}- {section_title}")

        for doc_data in body_docs:
            pdf.add_page()
            pdf.set_font("Helvetica", "B", 18)
            pdf.cell(0, 10, doc_data["title"], ln=True)
            pdf.ln(2)

            for line in doc_data["content"].splitlines():
                stripped = line.strip()
                if not stripped:
                    pdf.ln(2)
                    continue

                heading_match = re.match(r"^(#{1,6})\s+(.+)$", stripped)
                if heading_match:
                    level = len(heading_match.group(1))
                    text = self._to_display_line(heading_match.group(2))
                    size = max(11, 20 - level * 2)
                    pdf.set_font("Helvetica", "B", size)
                    pdf.multi_cell(0, 8, text)
                    pdf.set_font("Helvetica", size=11)
                    continue

                image_match = re.match(r"^!\[\[([^\]|]+)(?:\|[^\]]+)?\]\]$", stripped)
                if image_match:
                    image_name = Path(image_match.group(1)).name
                    image_path = self.images_root / image_name
                    if image_path.exists() and image_path.is_file():
                        try:
                            if pdf.get_y() > 240:
                                pdf.add_page()
                            pdf.image(str(image_path), w=120)
                            pdf.ln(2)
                        except Exception:
                            logger.warning("Failed to render image %s", image_path)
                    continue

                text = self._to_display_line(stripped)
                if text:
                    pdf.set_font("Helvetica", size=11)
                    pdf.multi_cell(0, 6, text)

            used_images = self._extract_obsidian_images(doc_data["content"])
            if used_images:
                pdf.ln(2)
                pdf.set_font("Helvetica", "B", 12)
                pdf.multi_cell(0, 7, "Used pictures")
                pdf.set_font("Helvetica", size=10)
                for img in used_images:
                    pdf.multi_cell(0, 6, f"- {img}")

        pdf.add_page()
        pdf.set_font("Helvetica", "B", 18)
        pdf.cell(0, 10, "Ressources", ln=True)

        pdf.ln(2)
        pdf.set_font("Helvetica", "B", 14)
        pdf.cell(0, 8, "Page Links", ln=True)
        pdf.set_font("Helvetica", size=11)
        page_links_found = False
        for title in sorted(all_links.keys(), key=str.casefold):
            links = all_links.get(title, [])
            for link in links:
                page_links_found = True
                pdf.multi_cell(0, 6, f"- {title} => {link}")
        if not page_links_found:
            pdf.multi_cell(0, 6, "NONE")

        pdf.ln(2)
        pdf.set_font("Helvetica", "B", 14)
        pdf.cell(0, 8, "Video Links", ln=True)
        pdf.set_font("Helvetica", size=11)
        video_links_found = False
        for title in sorted(all_video_links.keys(), key=str.casefold):
            links = all_video_links.get(title, [])
            for link in links:
                video_links_found = True
                pdf.multi_cell(0, 6, f"- {title} => {link}")
        if not video_links_found:
            pdf.multi_cell(0, 6, "NONE")

        pdf.output(str(output_path))
        return output_path
