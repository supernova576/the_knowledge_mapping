import tempfile
import unittest
from pathlib import Path

from src.DocsExporter import DocsExporter


class DocsExporterLearningHtmlTests(unittest.TestCase):
    def _exporter(self, temp_dir: Path) -> DocsExporter:
        exporter = DocsExporter.__new__(DocsExporter)
        exporter.export_dir = temp_dir
        return exporter

    def test_build_learning_export_payload_supports_multiple_rows(self):
        exporter = self._exporter(Path(tempfile.mkdtemp()))
        payload = exporter._build_learning_export_payload(
            [
                {
                    "id": 2,
                    "file_name": "Beta",
                    "source_note_name": "Beta.md",
                    "creation_date": "01.01.2026",
                    "last_modified_date": "02.01.2026",
                    "questions": [{"id": "q1", "text": "B?", "type": "SINGLE_CHOICE", "options": ["1", "2"]}],
                    "answers": [{"question_id": "q1", "correct_answers": ["1"]}],
                },
                {
                    "id": 1,
                    "file_name": "Alpha",
                    "source_note_name": "Alpha.md",
                    "creation_date": "01.01.2026",
                    "last_modified_date": "02.01.2026",
                    "questions": [{"id": "q1", "text": "A?", "type": "FREETEXT", "options": []}],
                    "answers": [{"question_id": "q1", "correct_answers": ["Answer"]}],
                },
            ]
        )

        self.assertEqual(2, len(payload))
        self.assertEqual("Alpha", payload[0]["file_name"])
        self.assertEqual(1, payload[1]["question_count"])
        self.assertEqual(["1"], payload[1]["answers_map"]["q1"])

    def test_export_learnings_to_html_creates_single_offline_file(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            exporter = self._exporter(Path(temp_dir))
            output = exporter.export_learnings_to_html(
                export_title="Learning Deck",
                learning_payloads=[
                    {
                        "id": 1,
                        "file_name": "Alpha",
                        "source_note_name": "Alpha.md",
                        "creation_date": "01.01.2026",
                        "last_modified_date": "02.01.2026",
                        "questions": [{"id": "q1", "text": "Question", "type": "SINGLE_CHOICE", "options": ["Yes", "No"]}],
                        "answers": [{"question_id": "q1", "correct_answers": ["Yes"]}],
                    }
                ],
                metadata={"description": "Deck summary", "tags": "#algorithms"},
            )
            html_content = output.read_text(encoding="utf-8")

            self.assertTrue(output.exists())
            self.assertIn("tkm_export_", html_content)
            self.assertNotIn("http://", html_content)
            self.assertNotIn("https://", html_content)
            self.assertIn("Show Answer Key", html_content)
            self.assertIn("Hide Answer Key", html_content)
            self.assertIn("Expected answer", html_content)
            self.assertIn("Export Title", html_content)
            self.assertIn("Description", html_content)
            self.assertIn("Tags", html_content)
            self.assertIn("color-scheme: dark;", html_content)
            self.assertIn("--bg: #212529;", html_content)


if __name__ == "__main__":
    unittest.main()
