import json
import re
import traceback
from pathlib import Path
from urllib import error as urllib_error
from urllib import request as urllib_request

from .logger import get_logger
from .timezone_utils import now_in_zurich


logger = get_logger(__name__)


class DocsAIFeedback:
    def __init__(self, conf: dict | None = None) -> None:
        self.conf = conf or self._load_conf()
        ai_conf = self.conf.get("ai_feedback", {})
        docs_conf = self.conf.get("docs", {})

        self.docs_path = Path(docs_conf.get("full_path_to_docs", "")).resolve()
        self.output_path = Path(ai_conf.get("output_path") or ai_conf.get("the_knowledge_path", "")).resolve()
        self.prompt_template_path = Path(
            ai_conf.get("prompt_template_path", "/the-knowledge/03_TEMPLATES/2 - AI Prompt.md")
        ).resolve()
        self.feedback_template_path = Path(
            ai_conf.get("feedback_template_path", "/the-knowledge/03_TEMPLATES/2 - AI Feedback.md")
        ).resolve()
        self.base_url = str(ai_conf.get("base_url", "")).strip()
        self.api_key = str(ai_conf.get("api_key", "")).strip()
        self.model = str(ai_conf.get("model", "")).strip()
        self.provider_order = ai_conf.get("provider", [])
        self.request_timeout_seconds = int(ai_conf.get("timeout_seconds", 120))
        self.http_referer = str(ai_conf.get("http_referer", "http://localhost")).strip()
        self.app_title = str(ai_conf.get("app_title", "The Knowledge Mapping")).strip()

    def _load_conf(self) -> dict:
        conf_path = Path(__file__).resolve().parent.parent / "conf.json"
        with open(conf_path, "r", encoding="utf-8") as conf_file:
            return json.loads(conf_file.read())

    def _normalize_doc_filename(self, file_name: str) -> str:
        cleaned = str(file_name or "").strip().replace("\\", "/")
        if not cleaned or "/" in cleaned:
            raise ValueError("Invalid document name.")
        if not cleaned.lower().endswith(".md"):
            cleaned = f"{cleaned}.md"
        return cleaned

    def _resolve_doc_path(self, file_name: str) -> Path:
        normalized = self._normalize_doc_filename(file_name)
        target_path = (self.docs_path / normalized).resolve()

        if self.docs_path not in target_path.parents:
            raise ValueError("The selected document is outside the configured docs directory.")
        if not target_path.exists() or not target_path.is_file():
            raise FileNotFoundError(f"Document not found: {normalized}")
        return target_path

    def _read_template(self, template_path: Path, label: str) -> str:
        if not template_path.exists() or not template_path.is_file():
            raise FileNotFoundError(f"{label} template not found: {template_path}")
        return template_path.read_text(encoding="utf-8")

    def _build_messages(self, note_name: str, doc_content: str, evaluation_context: str) -> list[dict]:
        return [
            {
                "role": "system",
                "content": (
                    "You review markdown documentation. "
                    "Return exactly one JSON object with keys 'score' and 'feedback'. "
                    "The 'score' must be numeric. "
                    "The 'feedback' must be markdown text."
                ),
            },
            {
                "role": "user",
                "content": (
                    f"Evaluate the markdown note named '{note_name}'.\n\n"
                    "Use the following evaluation context:\n"
                    f"{evaluation_context}\n\n"
                    "Now review this markdown note:\n"
                    f"{doc_content}\n\n"
                    "Return JSON only, with this schema:\n"
                    '{"score": <number>, "feedback": "<markdown feedback>"}'
                ),
            },
        ]

    def _extract_response_content(self, response_json: dict) -> str:
        choices = response_json.get("choices", [])
        if not choices:
            raise ValueError("AI response did not contain any choices.")

        message = choices[0].get("message", {})
        content = message.get("content", "")

        if isinstance(content, str):
            return content.strip()

        if isinstance(content, list):
            text_parts: list[str] = []
            for item in content:
                if isinstance(item, dict) and item.get("type") == "text":
                    text_parts.append(str(item.get("text", "")))
            return "\n".join(text_parts).strip()

        raise ValueError("Unsupported AI response content format.")

    def _parse_json_payload(self, raw_content: str) -> dict:
        cleaned = str(raw_content or "").strip()
        if not cleaned:
            raise ValueError("AI response was empty.")

        try:
            parsed = json.loads(cleaned)
        except json.JSONDecodeError:
            json_match = re.search(r"\{.*\}", cleaned, flags=re.DOTALL)
            if not json_match:
                raise ValueError("AI response was not valid JSON.") from None
            parsed = json.loads(json_match.group(0))

        if not isinstance(parsed, dict):
            raise ValueError("AI response JSON must be an object.")

        return parsed

    def _normalize_score(self, value) -> float:
        try:
            score = float(value)
        except (TypeError, ValueError):
            raise ValueError("AI response did not provide a numeric score.") from None

        if score != score:  # NaN check
            raise ValueError("AI response score must not be NaN.")
        return score

    def _request_ai_feedback(self, messages: list[dict]) -> dict:
        if not self.base_url:
            raise ValueError("AI feedback base_url is missing in conf.json.")
        if not self.api_key or self.api_key == "-":
            raise ValueError("AI feedback api_key is missing in conf.json.")
        if not self.model:
            raise ValueError("AI feedback model is missing in conf.json.")

        payload: dict = {
            "model": self.model,
            "messages": messages,
            "temperature": 0.2,
            "provider": {
                "order": self.provider_order if isinstance(self.provider_order, list) else [],
                "require_parameters": True,
            },
            "response_format": {
                "type": "json_schema",
                "json_schema": {
                    "name": "ai_feedback",
                    "strict": True,
                    "schema": {
                        "type": "object",
                        "additionalProperties": False,
                        "properties": {
                            "score": {"type": "number"},
                            "feedback": {"type": "string"},
                        },
                        "required": ["score", "feedback"],
                    },
                },
            },
        }

        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
            "HTTP-Referer": self.http_referer,
            "X-Title": self.app_title,
        }

        request_payload = json.dumps(payload).encode("utf-8")
        request_object = urllib_request.Request(
            self.base_url,
            data=request_payload,
            headers=headers,
            method="POST",
        )

        try:
            with urllib_request.urlopen(request_object, timeout=self.request_timeout_seconds) as response:
                response_json = json.loads(response.read().decode("utf-8"))
        except urllib_error.HTTPError as exc:
            error_body = exc.read().decode("utf-8", errors="replace")
            logger.error("OpenRouter request failed\n%s", traceback.format_exc())
            raise RuntimeError(f"OpenRouter request failed: HTTP {exc.code} - {error_body}") from exc
        except urllib_error.URLError as exc:
            logger.error("OpenRouter request failed\n%s", traceback.format_exc())
            raise RuntimeError(f"OpenRouter request failed: {exc.reason}") from exc
        except json.JSONDecodeError as exc:
            logger.error("OpenRouter response was not valid JSON\n%s", traceback.format_exc())
            raise RuntimeError(f"OpenRouter returned invalid JSON: {exc}") from exc

        raw_content = self._extract_response_content(response_json)
        parsed_payload = self._parse_json_payload(raw_content)

        feedback_text = str(parsed_payload.get("feedback", "")).strip()
        if not feedback_text:
            raise ValueError("AI response did not provide feedback text.")

        return {
            "score": self._normalize_score(parsed_payload.get("score")),
            "feedback": feedback_text,
        }

    def generate_feedback(self, file_name: str) -> dict:
        try:
            doc_path = self._resolve_doc_path(file_name)
            note_name = doc_path.stem.strip()
            doc_content = doc_path.read_text(encoding="utf-8")
            evaluation_context = self._read_template(self.prompt_template_path, "AI prompt")
            feedback_template = self._read_template(self.feedback_template_path, "AI feedback")

            ai_feedback = self._request_ai_feedback(self._build_messages(note_name, doc_content, evaluation_context))
            return {
                "note_name": note_name,
                "score": ai_feedback["score"],
                "feedback": ai_feedback["feedback"],
                "creation_date": now_in_zurich().strftime("%d.%m.%Y"),
                "feedback_template": feedback_template,
            }
        except Exception:
            logger.error("Failed to generate AI feedback\n%s", traceback.format_exc())
            raise
