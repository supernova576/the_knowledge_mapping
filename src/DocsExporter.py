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
    PDF_TEXT_REPLACEMENTS = str.maketrans(
        {
            "•": "-",
            "‣": "-",
            "◦": "-",
            "☐": "[ ]",
            "☑": "[x]",
            "✓": "x",
            "✔": "x",
            "–": "-",
            "—": "-",
            "…": "...",
            "\u00a0": " ",
        }
    )

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
        pdf.multi_cell(0, height, self._sanitize_pdf_text(text), align=align, new_x="LMARGIN", new_y="NEXT")

    def _sanitize_pdf_text(self, text: str) -> str:
        sanitized = str(text or "").translate(self.PDF_TEXT_REPLACEMENTS)
        return sanitized.encode("latin-1", errors="replace").decode("latin-1")

    def _to_plain_text(self, markdown_line: str) -> str:
        text = str(markdown_line or "")
        text = re.sub(r"\[\[([^\]|]+)\|([^\]]+)\]\]", r"\2", text)
        text = re.sub(r"\[\[([^\]]+)\]\]", r"\1", text)
        text = re.sub(r"\[([^\]]+)\]\(([^)]+)\)", r"\1 (\2)", text)
        return self._sanitize_pdf_text(text.strip())

    def _strip_inline_markdown(self, text: str) -> str:
        plain = self._to_plain_text(text)
        plain = re.sub(r"\*\*(.+?)\*\*", r"\1", plain)
        plain = re.sub(r"==(.+?)==", r"\1", plain)
        plain = re.sub(r"`([^`]+)`", r"\1", plain)
        plain = re.sub(r"\$([^$\n]+)\$", r"\1", plain)
        return self._sanitize_pdf_text(plain)

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
        escaped = re.sub(r"==(.+?)==", r"<b>\1</b>", escaped)
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
            if current.strip().startswith(delimiter):
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
        stripped = line.strip()
        if stripped.startswith("|"):
            stripped = stripped[1:]
        if stripped.endswith("|"):
            stripped = stripped[:-1]

        cells: list[str] = []
        current: list[str] = []
        bracket_depth = 0
        paren_depth = 0
        escape_next = False

        for char in stripped:
            if escape_next:
                current.append(char)
                escape_next = False
                continue
            if char == "\\":
                escape_next = True
                current.append(char)
                continue
            if char == "[":
                bracket_depth += 1
            elif char == "]" and bracket_depth > 0:
                bracket_depth -= 1
            elif char == "(":
                paren_depth += 1
            elif char == ")" and paren_depth > 0:
                paren_depth -= 1

            if char == "|" and bracket_depth == 0 and paren_depth == 0:
                cells.append("".join(current).strip())
                current = []
                continue
            current.append(char)

        cells.append("".join(current).strip())
        return [self._strip_inline_markdown(cell) for cell in cells]

    def _render_table(self, pdf, table_lines: list[str]) -> None:
        if len(table_lines) < 2:
            return

        header = self._parse_table_row(table_lines[0])
        rows = [self._parse_table_row(line) for line in table_lines[2:] if line.strip()]
        col_count = max(1, len(header))
        col_width = (pdf.w - pdf.l_margin - pdf.r_margin) / col_count
        col_widths = [col_width] * col_count

        self._render_table_row(pdf, header, col_widths, font_style="B", font_size=11)
        for row in rows:
            self._render_table_row(pdf, row, col_widths, font_style="", font_size=10)
        pdf.ln(2)

    def _estimate_wrapped_line_count(self, pdf, text: str, width: float) -> int:
        usable_width = max(width - 2, 1)
        paragraphs = str(text or "").splitlines() or [""]
        line_count = 0

        for paragraph in paragraphs:
            if not paragraph:
                line_count += 1
                continue

            current_width = 0.0
            current_has_content = False
            for chunk in re.findall(r"\S+\s*", paragraph):
                chunk_width = pdf.get_string_width(chunk)
                if current_has_content and current_width + chunk_width > usable_width:
                    line_count += 1
                    current_width = chunk_width
                else:
                    current_width += chunk_width
                current_has_content = True

            if current_has_content:
                line_count += 1

        return max(line_count, 1)

    def _render_table_row(
        self,
        pdf,
        cells: list[str],
        col_widths: list[float],
        font_style: str,
        font_size: int,
    ) -> None:
        pdf.set_font("Helvetica", font_style, font_size)
        line_height = 6
        padded_cells = [
            self._sanitize_pdf_text(cells[idx] if idx < len(cells) else "")
            for idx in range(len(col_widths))
        ]
        line_counts = [
            self._estimate_wrapped_line_count(pdf, cell_text, col_widths[idx])
            for idx, cell_text in enumerate(padded_cells)
        ]
        row_height = max(line_counts) * line_height + 2
        self._ensure_space(pdf, row_height + 1)

        start_x = pdf.get_x()
        start_y = pdf.get_y()
        current_x = start_x

        for width, cell_text in zip(col_widths, padded_cells):
            pdf.rect(current_x, start_y, width, row_height)
            pdf.set_xy(current_x + 1, start_y + 1)
            pdf.multi_cell(
                width - 2,
                line_height,
                cell_text,
                border=0,
                new_x="LEFT",
                new_y="TOP",
            )
            current_x += width

        pdf.set_xy(start_x, start_y + row_height)

    def _render_code_block(self, pdf, lines: list[str]) -> None:
        if not lines:
            return
        block = self._sanitize_pdf_text("\n".join(lines))
        line_count = max(1, len(lines))
        self._ensure_space(pdf, line_count * 6 + 6)
        pdf.set_font("Courier", size=10)
        pdf.set_fill_color(245, 245, 245)
        pdf.multi_cell(0, 5, block, border=1, fill=True, new_x="LMARGIN", new_y="NEXT")
        pdf.ln(1)
        pdf.set_font("Helvetica", size=11)

    def _render_latex_block(self, pdf, lines: list[str]) -> None:
        formula = self._sanitize_pdf_text("\n".join(lines).strip())
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

        exported_at = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        safe_export_title = self._sanitize_pdf_text(export_title.strip() or "Documentation Export")
        safe_user_description = self._sanitize_pdf_text(user_description or "N/A")

        pdf.add_page()
        pdf.set_fill_color(33, 37, 41)
        pdf.rect(0, 0, pdf.w, pdf.h, "F")

        pdf.set_fill_color(43, 48, 53)
        pdf.set_draw_color(73, 80, 87)
        pdf.rect(12, 18, 186, 84, "FD")
        pdf.set_fill_color(25, 135, 84)
        pdf.rect(20, 84, 170, 2.5, "F")

        pdf.set_text_color(173, 181, 189)
        pdf.set_font("Helvetica", size=11)
        pdf.set_xy(24, 30)
        pdf.cell(0, 6, "DOCUMENTATION EXPORT", new_x="LMARGIN", new_y="NEXT")

        pdf.set_text_color(248, 249, 250)
        pdf.set_font("Helvetica", "B", 28)
        pdf.set_xy(24, 44)
        pdf.multi_cell(152, 12, safe_export_title, new_x="LMARGIN", new_y="NEXT")

        pdf.set_text_color(173, 181, 189)
        pdf.set_font("Helvetica", size=11)
        pdf.set_xy(24, 70)
        pdf.multi_cell(152, 7, self._sanitize_pdf_text(f"Generated on {exported_at}"), new_x="LMARGIN", new_y="NEXT")

        details_x = 24
        details_y = pdf.h - 78
        details_w = 164
        details_inner_x = details_x + 8

        pdf.set_fill_color(43, 48, 53)
        pdf.set_draw_color(73, 80, 87)
        pdf.rect(details_x, details_y, details_w, 54, "FD")
        pdf.set_fill_color(25, 135, 84)
        pdf.rect(details_x, details_y, 5, 54, "F")

        pdf.set_text_color(248, 249, 250)
        pdf.set_xy(details_inner_x, details_y + 8)
        pdf.set_font("Helvetica", "B", 12)
        pdf.multi_cell(0, 8, "EXPORT DETAILS", new_x="LMARGIN", new_y="NEXT")

        pdf.set_text_color(173, 181, 189)
        pdf.set_font("Helvetica", size=11)
        pdf.set_x(details_inner_x)
        pdf.multi_cell(details_w - 14, 7, f"Description: {safe_user_description}", new_x="LMARGIN", new_y="NEXT")
        pdf.set_x(details_inner_x)
        pdf.multi_cell(
            details_w - 14,
            7,
            self._sanitize_pdf_text(f"Date and time of export: {exported_at}"),
            new_x="LMARGIN",
            new_y="NEXT",
        )
        pdf.set_x(details_inner_x)
        pdf.multi_cell(details_w - 14, 7, "Amount of pages: {nb}", new_x="LMARGIN", new_y="NEXT")

        toc_sections: list[tuple[str, list[tuple[int, str]]]] = []
        body_docs: list[dict] = []
        all_links: dict[str, list[str]] = {}
        all_video_links: dict[str, list[str]] = {}
        all_used_images: dict[str, list[str]] = {}

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
            all_used_images[title] = self._extract_obsidian_images(cleaned_content)

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
            pdf.cell(0, 10, self._sanitize_pdf_text(doc_data["title"]), new_x="LMARGIN", new_y="NEXT")
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

                if stripped.startswith("```"):
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
                    marker = "[ ]" if checklist_match.group(1).strip() == "" else "[x]"
                    self._render_text_line(pdf, f"{marker} {checklist_match.group(2)}")
                    idx += 1
                    continue

                bullet_match = re.match(r"^[-*]\s+(.+)$", stripped)
                if bullet_match:
                    self._render_text_line(pdf, f"- {bullet_match.group(1)}")
                    idx += 1
                    continue

                self._render_text_line(pdf, stripped)
                idx += 1

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

        pdf.ln(2)
        pdf.set_font("Helvetica", "B", 14)
        pdf.cell(0, 8, "Pictures Used", new_x="LMARGIN", new_y="NEXT")
        pdf.set_font("Helvetica", size=11)
        images_found = False
        for title in sorted(all_used_images.keys(), key=str.casefold):
            images = all_used_images.get(title, [])
            for image in images:
                images_found = True
                self._multi_cell_line(pdf, 6, f"- {title} => {image}")
        if not images_found:
            self._multi_cell_line(pdf, 6, "NONE")

        pdf.output(str(output_path))
        return output_path
