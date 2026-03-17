import json
import re
import traceback
from datetime import datetime
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

    def _multi_cell_line(self, pdf, height: int | float, text: str, align: str = "L") -> None:
        pdf.multi_cell(0, height, text, align=align, new_x="LMARGIN", new_y="NEXT")

    def _to_plain_text(self, markdown_line: str) -> str:
        text = str(markdown_line or "")
        text = re.sub(r"\[\[([^\]|]+)\|([^\]]+)\]\]", r"\2", text)
        text = re.sub(r"\[\[([^\]]+)\]\]", r"\1", text)
        text = re.sub(r"\[([^\]]+)\]\(([^)]+)\)", r"\1 (\2)", text)
        return text.strip()

    def _inline_to_html(self, text: str) -> str:
        escaped = (
            str(text or "")
            .replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
        )
        escaped = re.sub(r"\[\[([^\]|]+)\|([^\]]+)\]\]", r"\2", escaped)
        escaped = re.sub(r"\[\[([^\]]+)\]\]", r"\1", escaped)
        escaped = re.sub(r"\*\*(.+?)\*\*", r"<b>\1</b>", escaped)
        escaped = re.sub(
            r"==(.+?)==",
            r'<b><font color="#FFFFFF" bgcolor="#D4B039">\1</font></b>',
            escaped,
        )
        escaped = re.sub(r"`([^`]+)`", r"<code>\1</code>", escaped)
        escaped = re.sub(r"\$([^$\n]+)\$", r"<i>\1</i>", escaped)
        return escaped

    def _ensure_space(self, pdf, min_space: float) -> None:
        if pdf.get_y() + min_space > (pdf.h - pdf.b_margin):
            pdf.add_page()

    def _render_text_line(self, pdf, text: str, indent: float = 0) -> None:
        html = self._inline_to_html(self._to_plain_text(text))
        self._ensure_space(pdf, 8)
        if indent > 0:
            pdf.set_x(pdf.l_margin + indent)
        pdf.write_html(html)
        pdf.ln(4)

    def _collect_fenced_block(self, lines: list[str], start: int, delimiter: str) -> tuple[list[str], int]:
        collected: list[str] = []
        index = start
        while index < len(lines):
            current = lines[index]
            if current.strip() == delimiter:
                return collected, index + 1
            collected.append(current.rstrip("\n"))
            index += 1
        return collected, index

    def _is_table_separator(self, line: str) -> bool:
        stripped = line.strip()
        if "|" not in stripped:
            return False
        cells = [cell.strip() for cell in stripped.strip("|").split("|")]
        return all(re.fullmatch(r":?-{3,}:?", cell or "") for cell in cells if cell != "")

    def _parse_table_row(self, line: str) -> list[str]:
        row = [cell.strip() for cell in line.strip().strip("|").split("|")]
        return [self._to_plain_text(cell) for cell in row]

    def _render_table(self, pdf, table_lines: list[str]) -> None:
        if len(table_lines) < 2:
            return

        header = self._parse_table_row(table_lines[0])
        rows = [self._parse_table_row(line) for line in table_lines[2:] if line.strip()]
        col_count = max(1, len(header))
        col_width = (pdf.w - pdf.l_margin - pdf.r_margin) / col_count
        row_height = 8
        required_height = row_height * (1 + len(rows)) + 4
        self._ensure_space(pdf, required_height)

        pdf.set_font("Helvetica", "B", 11)
        for col in range(col_count):
            pdf.cell(col_width, row_height, header[col] if col < len(header) else "", border=1)
        pdf.ln(row_height)

        pdf.set_font("Helvetica", size=10)
        for row in rows:
            for col in range(col_count):
                pdf.cell(col_width, row_height, row[col] if col < len(row) else "", border=1)
            pdf.ln(row_height)
        pdf.ln(2)

    def _render_code_block(self, pdf, lines: list[str]) -> None:
        if not lines:
            return
        block = "\n".join(lines)
        line_count = max(1, len(lines))
        self._ensure_space(pdf, line_count * 6 + 6)
        pdf.set_font("Courier", size=10)
        pdf.set_fill_color(245, 245, 245)
        pdf.multi_cell(0, 5, block, border=1, fill=True, new_x="LMARGIN", new_y="NEXT")
        pdf.ln(1)
        pdf.set_font("Helvetica", size=11)

    def _render_latex_block(self, pdf, lines: list[str]) -> None:
        formula = "\n".join(lines).strip()
        if not formula:
            return
        self._ensure_space(pdf, 14)
        pdf.set_font("Helvetica", "I", 11)
        pdf.set_fill_color(250, 250, 235)
        pdf.multi_cell(0, 6, formula, border=1, fill=True, new_x="LMARGIN", new_y="NEXT")
        pdf.ln(1)
        pdf.set_font("Helvetica", size=11)

    def export_docs_to_pdf(self, export_title: str, docs: list[dict], user_description: str = "") -> Path:
        try:
            from fpdf import FPDF
        except ImportError as exc:
            raise RuntimeError("Missing dependency fpdf2. Install requirements before using export.") from exc

        ordered_docs = sorted(docs, key=lambda doc: str(doc.get("title", "")).casefold())
        file_name = self._safe_pdf_name(export_title)
        output_path = self.export_dir / file_name

        pdf = FPDF()
        pdf.alias_nb_pages()
        pdf.set_auto_page_break(auto=True, margin=15)

        pdf.add_page()
        pdf.set_fill_color(25, 135, 84)
        pdf.set_text_color(255, 255, 255)
        pdf.set_font("Helvetica", "B", 28)
        pdf.rect(10, 20, 190, 40, "F")
        pdf.set_xy(14, 32)
        pdf.multi_cell(182, 10, export_title.strip() or "Documentation Export", align="C", new_x="LMARGIN", new_y="NEXT")

        pdf.set_text_color(0, 0, 0)
        pdf.set_font("Helvetica", size=12)
        pdf.ln(5)
        pdf.multi_cell(0, 7, f"Description: {user_description or 'N/A'}", new_x="LMARGIN", new_y="NEXT")
        pdf.multi_cell(0, 7, f"Date and time of export: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", new_x="LMARGIN", new_y="NEXT")
        pdf.multi_cell(0, 7, "Amount of pages: {nb}", new_x="LMARGIN", new_y="NEXT")

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
        pdf.cell(0, 10, "Table of Contents", new_x="LMARGIN", new_y="NEXT")
        pdf.ln(2)
        pdf.set_font("Helvetica", size=11)

        for doc_title, sections in toc_sections:
            pdf.set_font("Helvetica", "B", 12)
            self._multi_cell_line(pdf, 7, f"- {doc_title}")
            pdf.set_font("Helvetica", size=11)
            for level, section_title in sections:
                indent = "  " * max(level - 1, 1)
                self._multi_cell_line(pdf, 6, f"{indent}- {section_title}")

        for doc_data in body_docs:
            pdf.add_page()
            pdf.set_font("Helvetica", "B", 18)
            pdf.cell(0, 10, doc_data["title"], new_x="LMARGIN", new_y="NEXT")
            pdf.ln(2)

            lines = doc_data["content"].splitlines()
            idx = 0
            while idx < len(lines):
                line = lines[idx]
                stripped = line.strip()

                if not stripped:
                    pdf.ln(2)
                    idx += 1
                    continue

                if stripped == "```":
                    block, idx = self._collect_fenced_block(lines, idx + 1, "```")
                    self._render_code_block(pdf, block)
                    continue

                if stripped == "$$":
                    block, idx = self._collect_fenced_block(lines, idx + 1, "$$")
                    self._render_latex_block(pdf, block)
                    continue

                heading_match = re.match(r"^(#{1,6})\s+(.+)$", stripped)
                if heading_match:
                    level = len(heading_match.group(1))
                    text = self._to_plain_text(heading_match.group(2))
                    size = max(11, 20 - level * 2)
                    if level <= 2:
                        self._ensure_space(pdf, 30)
                    pdf.set_font("Helvetica", "B", size)
                    self._multi_cell_line(pdf, 8, text)
                    pdf.set_font("Helvetica", size=11)
                    idx += 1
                    continue

                image_match = re.match(r"^!\[\[([^\]|]+)(?:\|[^\]]+)?\]\]$", stripped)
                if image_match:
                    image_name = Path(image_match.group(1)).name
                    image_path = self.images_root / image_name
                    if image_path.exists() and image_path.is_file():
                        try:
                            self._ensure_space(pdf, 95)
                            pdf.image(str(image_path), w=120)
                            pdf.ln(2)
                        except Exception:
                            logger.warning("Failed to render image %s", image_path)
                    idx += 1
                    continue

                if idx + 1 < len(lines) and "|" in stripped and self._is_table_separator(lines[idx + 1]):
                    table_lines = [line]
                    idx += 1
                    while idx < len(lines) and lines[idx].strip() and "|" in lines[idx]:
                        table_lines.append(lines[idx])
                        idx += 1
                    self._render_table(pdf, table_lines)
                    continue

                checklist_match = re.match(r"^[-*]\s+\[( |x|X)\]\s+(.+)$", stripped)
                if checklist_match:
                    marker = "☐" if checklist_match.group(1).strip() == "" else "☑"
                    self._render_text_line(pdf, f"{marker} {checklist_match.group(2)}")
                    idx += 1
                    continue

                bullet_match = re.match(r"^[-*]\s+(.+)$", stripped)
                if bullet_match:
                    self._render_text_line(pdf, f"• {bullet_match.group(1)}")
                    idx += 1
                    continue

                self._render_text_line(pdf, stripped)
                idx += 1

            used_images = self._extract_obsidian_images(doc_data["content"])
            if used_images:
                pdf.ln(2)
                pdf.set_font("Helvetica", "B", 12)
                self._multi_cell_line(pdf, 7, "Used pictures")
                pdf.set_font("Helvetica", size=10)
                for img in used_images:
                    self._multi_cell_line(pdf, 6, f"- {img}")

        pdf.add_page()
        pdf.set_font("Helvetica", "B", 18)
        pdf.cell(0, 10, "Ressources", new_x="LMARGIN", new_y="NEXT")

        pdf.ln(2)
        pdf.set_font("Helvetica", "B", 14)
        pdf.cell(0, 8, "Page Links", new_x="LMARGIN", new_y="NEXT")
        pdf.set_font("Helvetica", size=11)
        page_links_found = False
        for title in sorted(all_links.keys(), key=str.casefold):
            links = all_links.get(title, [])
            for link in links:
                page_links_found = True
                self._multi_cell_line(pdf, 6, f"- {title} => {link}")
        if not page_links_found:
            self._multi_cell_line(pdf, 6, "NONE")

        pdf.ln(2)
        pdf.set_font("Helvetica", "B", 14)
        pdf.cell(0, 8, "Video Links", new_x="LMARGIN", new_y="NEXT")
        pdf.set_font("Helvetica", size=11)
        video_links_found = False
        for title in sorted(all_video_links.keys(), key=str.casefold):
            links = all_video_links.get(title, [])
            for link in links:
                video_links_found = True
                self._multi_cell_line(pdf, 6, f"- {title} => {link}")
        if not video_links_found:
            self._multi_cell_line(pdf, 6, "NONE")

        pdf.output(str(output_path))
        return output_path
