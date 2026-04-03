import json
import mimetypes
import re
import socket
import traceback
from base64 import b64encode
from datetime import datetime
from pathlib import Path
from time import monotonic
from urllib import error as urllib_error
from urllib import request as urllib_request

from .logger import get_logger
from .timezone_utils import now_in_zurich


logger = get_logger(__name__)


class OpenRouterImageNotSupportedError(RuntimeError):
    pass


class DocsAIFeedback:
    def __init__(self, conf: dict | None = None) -> None:
        self.conf = conf or self._load_conf()
        ai_conf = self.conf.get("ai_feedback", {})
        docs_conf = self.conf.get("docs", {})
        pictures_conf = self.conf.get("pictures", {})

        self.docs_path = Path(docs_conf.get("full_path_to_docs", "")).resolve()
        self.pictures_path = Path(pictures_conf.get("full_path_to_pictures", "")).resolve()
        self.output_path = Path(ai_conf.get("output_path") or ai_conf.get("the_knowledge_path", "")).resolve()
        self.prompt_template_path = Path(
            ai_conf.get("prompt_template_path", "/the-knowledge/03_TEMPLATES/2 - AI Prompt.md")
        ).resolve()
        self.feedback_template_path = Path(
            ai_conf.get("feedback_template_path", "/the-knowledge/03_TEMPLATES/2 - AI Feedback.md")
        ).resolve()
        self.error_output_path = Path(ai_conf.get("error_output_path", "output/ai_feedback_error")).resolve()
        self.base_url = str(ai_conf.get("base_url", "")).strip()
        self.api_key = str(ai_conf.get("api_key", "")).strip()
        self.model = str(ai_conf.get("model", "")).strip()
        self.provider_order = ai_conf.get("provider", [])
        self.request_timeout_seconds = int(ai_conf.get("timeout_seconds", 120))
        self.http_referer = str(ai_conf.get("http_referer", "http://localhost")).strip()
        self.app_title = str(ai_conf.get("app_title", "The Knowledge Mapping")).strip()
        self.max_images_per_request = int(ai_conf.get("max_images_per_request", 8))
        self.max_image_size_bytes = int(ai_conf.get("max_image_size_bytes", 5 * 1024 * 1024))

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

    def _build_previous_feedback_section(self, previous_feedback: dict | None) -> str:
        if not isinstance(previous_feedback, dict):
            return ""

        previous_score = previous_feedback.get("score")
        previous_feedback_text = str(previous_feedback.get("feedback", "")).strip()
        if previous_score is None or not previous_feedback_text:
            return ""

        previous_version = int(previous_feedback.get("version", 0) or 0)
        previous_creation_date = str(previous_feedback.get("creation_date", "N/A")).strip() or "N/A"
        review_instruction = ""
        try:
            if float(previous_score) < 100:
                review_instruction = (
                    "Because the previous score was below 100, explicitly evaluate whether the previously "
                    "identified issues were implemented or are still missing.\n"
                )
        except (TypeError, ValueError):
            pass

        return (
            "Use this previous AI feedback for context before evaluating the current documentation version:\n"
            f"Previous version: {previous_version}\n"
            f"Previous creation date: {previous_creation_date}\n"
            f"Previous score: {previous_score}\n"
            f"Previous feedback:\n{previous_feedback_text}\n\n"
            f"{review_instruction}"
        )

    def _build_messages(
        self,
        note_name: str,
        doc_content: str,
        evaluation_context: str,
        previous_feedback: dict | None = None,
        include_images: bool = True,
    ) -> list[dict]:
        previous_feedback_context = self._build_previous_feedback_section(previous_feedback)
        image_parts = self._build_image_message_parts(doc_content) if include_images else []
        user_text = (
            f"Evaluate the markdown note named '{note_name}'.\n\n"
            "Use the following evaluation context:\n"
            f"{evaluation_context}\n\n"
            f"{previous_feedback_context}"
            "Now review this markdown note:\n"
            f"{doc_content}\n\n"
            "Return JSON only, with this schema:\n"
            '{"score": <number>, "feedback": "<markdown feedback>"}'
        )
        user_content: str | list[dict] = user_text
        if image_parts:
            user_content = [{"type": "text", "text": user_text}, *image_parts]

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
                "content": user_content,
            },
        ]

    def _extract_referenced_image_names(self, doc_content: str) -> list[str]:
        markdown_text = str(doc_content or "")
        matches: list[str] = []
        matches.extend(re.findall(r"!\[\[([^\]|]+)(?:\|[^\]]+)?\]\]", markdown_text))
        matches.extend(re.findall(r"!\[[^\]]*\]\(([^)\s]+)(?:\s+\"[^\"]*\")?\)", markdown_text))

        unique_names: list[str] = []
        seen: set[str] = set()
        for raw_name in matches:
            candidate = Path(str(raw_name or "").strip()).name
            if not candidate:
                continue
            lowered = candidate.lower()
            if lowered in seen:
                continue
            seen.add(lowered)
            unique_names.append(candidate)

        return unique_names

    def _build_image_data_url(self, file_name: str) -> str | None:
        if not self.pictures_path.exists() or not self.pictures_path.is_dir():
            logger.warning("Configured pictures directory is unavailable: %s", self.pictures_path)
            return None

        candidate_name = Path(str(file_name or "").strip()).name
        if not candidate_name:
            return None

        image_path = (self.pictures_path / candidate_name).resolve()
        if self.pictures_path != image_path and self.pictures_path not in image_path.parents:
            logger.warning("Rejected image outside configured pictures directory: %s", file_name)
            return None
        if not image_path.exists() or not image_path.is_file():
            logger.warning("Referenced image not found for AI feedback: %s", candidate_name)
            return None

        mime_type, _ = mimetypes.guess_type(str(image_path))
        if mime_type not in {"image/png", "image/jpeg", "image/webp", "image/gif"}:
            logger.warning("Skipped unsupported image format for AI feedback: %s", image_path)
            return None

        if image_path.stat().st_size > self.max_image_size_bytes:
            logger.warning("Skipped image exceeding size limit (%s bytes): %s", self.max_image_size_bytes, image_path)
            return None

        image_bytes = image_path.read_bytes()
        encoded = b64encode(image_bytes).decode("ascii")
        return f"data:{mime_type};base64,{encoded}"

    def _build_image_message_parts(self, doc_content: str) -> list[dict]:
        parts: list[dict] = []
        image_names = self._extract_referenced_image_names(doc_content)
        for image_name in image_names[: max(0, self.max_images_per_request)]:
            data_url = self._build_image_data_url(image_name)
            if not data_url:
                continue
            parts.append({"type": "image_url", "image_url": {"url": data_url}})
        return parts

    def _build_request_payload(self, messages: list[dict], use_strict_schema: bool) -> dict:
        payload: dict = {
            "model": self.model,
            "messages": messages,
            "temperature": 0.2,
            "stream": False,
            "provider": {
                "order": self.provider_order if isinstance(self.provider_order, list) else [],
                "require_parameters": True,
            },
        }

        if use_strict_schema:
            payload["response_format"] = {
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
            }

        return payload

    def _extract_response_content(self, response_json: dict) -> str:
        choices = response_json.get("choices", [])
        if not choices:
            raise ValueError("AI response did not contain any choices.")

        choice = choices[0]
        message = choice.get("message", {})
        content = message.get("content", "")

        if isinstance(content, str):
            return content.strip()

        if isinstance(content, list):
            text_parts: list[str] = []
            for item in content:
                if isinstance(item, dict) and item.get("type") == "text":
                    text_parts.append(str(item.get("text", "")))
            return "\n".join(text_parts).strip()

        tool_calls = message.get("tool_calls", [])
        if tool_calls:
            function_call = tool_calls[0].get("function", {})
            arguments = function_call.get("arguments", "")
            if isinstance(arguments, str) and arguments.strip():
                return arguments.strip()

        text = choice.get("text", "")
        if isinstance(text, str) and text.strip():
            return text.strip()

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

    def _dump_error_payload(
        self,
        *,
        note_name: str,
        request_payload: dict,
        use_strict_schema: bool,
        error_message: str,
        response_json: dict | None = None,
        raw_response_text: str | None = None,
        http_status: int | None = None,
    ) -> None:
        try:
            self.error_output_path.mkdir(parents=True, exist_ok=True)
            safe_note_name = re.sub(r"[^A-Za-z0-9._-]+", "_", str(note_name or "unknown").strip()).strip("._") or "unknown"
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            target_path = self.error_output_path / f"{timestamp}_{safe_note_name}_ai_feedback_error.json"

            payload = {
                "created_at": datetime.now().isoformat(),
                "note_name": note_name,
                "model": self.model,
                "provider_order": self.provider_order if isinstance(self.provider_order, list) else [],
                "base_url": self.base_url,
                "use_strict_schema": use_strict_schema,
                "http_status": http_status,
                "error_message": error_message,
                "request_payload": request_payload,
                "response_json": response_json,
                "raw_response_text": raw_response_text,
            }
            target_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
            logger.error("Wrote AI feedback error payload to %s", target_path)
        except Exception:
            logger.error("Failed to write AI feedback error payload\n%s", traceback.format_exc())

    def _read_response_with_deadline(self, response) -> str:
        started_at = monotonic()
        chunks: list[bytes] = []

        while True:
            elapsed_seconds = monotonic() - started_at
            if elapsed_seconds >= self.request_timeout_seconds:
                raise TimeoutError(
                    f"OpenRouter response exceeded total timeout of {self.request_timeout_seconds} seconds."
                )

            chunk = response.read(64 * 1024)
            if not chunk:
                break
            chunks.append(chunk)

        return b"".join(chunks).decode("utf-8")

    def _is_image_input_unsupported_error(self, http_status: int, parsed_error_body: dict | None, raw_error_body: str) -> bool:
        if http_status != 404:
            return False

        error_message = ""
        if isinstance(parsed_error_body, dict):
            error_data = parsed_error_body.get("error", {})
            if isinstance(error_data, dict):
                error_message = str(error_data.get("message", "")).strip()

        if not error_message:
            error_message = str(raw_error_body or "").strip()

        return "support image input" in error_message.casefold()

    def _request_ai_feedback_once(self, note_name: str, messages: list[dict], use_strict_schema: bool) -> dict:
        if not self.base_url:
            raise ValueError("AI feedback base_url is missing in conf.json.")
        if not self.api_key or self.api_key == "-":
            raise ValueError("AI feedback api_key is missing in conf.json.")
        if not self.model:
            raise ValueError("AI feedback model is missing in conf.json.")
        payload = self._build_request_payload(messages, use_strict_schema=use_strict_schema)

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
                raw_response_text = self._read_response_with_deadline(response)
                response_json = json.loads(raw_response_text)
        except urllib_error.HTTPError as exc:
            error_body = exc.read().decode("utf-8", errors="replace")
            parsed_error_body = None
            try:
                parsed_error_body = json.loads(error_body)
            except json.JSONDecodeError:
                parsed_error_body = None
            self._dump_error_payload(
                note_name=note_name,
                request_payload=payload,
                use_strict_schema=use_strict_schema,
                error_message=f"OpenRouter request failed: HTTP {exc.code}",
                response_json=parsed_error_body if isinstance(parsed_error_body, dict) else None,
                raw_response_text=error_body,
                http_status=exc.code,
            )
            logger.error("OpenRouter request failed\n%s", traceback.format_exc())
            if self._is_image_input_unsupported_error(exc.code, parsed_error_body, error_body):
                raise OpenRouterImageNotSupportedError(
                    f"OpenRouter request failed: HTTP {exc.code} - {error_body}"
                ) from exc
            raise RuntimeError(f"OpenRouter request failed: HTTP {exc.code} - {error_body}") from exc
        except (TimeoutError, socket.timeout) as exc:
            self._dump_error_payload(
                note_name=note_name,
                request_payload=payload,
                use_strict_schema=use_strict_schema,
                error_message=str(exc),
            )
            logger.error("OpenRouter request timed out\n%s", traceback.format_exc())
            raise RuntimeError(str(exc)) from exc
        except urllib_error.URLError as exc:
            self._dump_error_payload(
                note_name=note_name,
                request_payload=payload,
                use_strict_schema=use_strict_schema,
                error_message=f"OpenRouter request failed: {exc.reason}",
            )
            logger.error("OpenRouter request failed\n%s", traceback.format_exc())
            raise RuntimeError(f"OpenRouter request failed: {exc.reason}") from exc
        except json.JSONDecodeError as exc:
            self._dump_error_payload(
                note_name=note_name,
                request_payload=payload,
                use_strict_schema=use_strict_schema,
                error_message=f"OpenRouter returned invalid JSON: {exc}",
                raw_response_text=raw_response_text,
            )
            logger.error("OpenRouter response was not valid JSON\n%s", traceback.format_exc())
            raise RuntimeError(f"OpenRouter returned invalid JSON: {exc}") from exc

        try:
            raw_content = self._extract_response_content(response_json)
            parsed_payload = self._parse_json_payload(raw_content)

            feedback_text = str(parsed_payload.get("feedback", "")).strip()
            if not feedback_text:
                raise ValueError("AI response did not provide feedback text.")

            return {
                "score": self._normalize_score(parsed_payload.get("score")),
                "feedback": feedback_text,
            }
        except (ValueError, KeyError, TypeError) as exc:
            self._dump_error_payload(
                note_name=note_name,
                request_payload=payload,
                use_strict_schema=use_strict_schema,
                error_message=str(exc),
                response_json=response_json,
                raw_response_text=raw_response_text,
            )
            raise

    def _request_openrouter_json(self, endpoint: str) -> dict:
        if not self.api_key or self.api_key == "-":
            raise ValueError("AI feedback api_key is missing in conf.json.")

        base_origin = str(self.base_url or "").strip().rstrip("/")
        if not base_origin:
            base_origin = "https://openrouter.ai/api/v1/chat/completions"
        if "/api/v1/" in base_origin:
            base_origin = base_origin.split("/api/v1/", 1)[0]

        target_url = f"{base_origin}{endpoint}"
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
            "HTTP-Referer": self.http_referer,
            "X-Title": self.app_title,
        }
        request_object = urllib_request.Request(target_url, headers=headers, method="GET")
        with urllib_request.urlopen(request_object, timeout=self.request_timeout_seconds) as response:
            return json.loads(self._read_response_with_deadline(response))

    def _extract_credits_left(self, payload: dict) -> float | None:
        if not isinstance(payload, dict):
            return None

        data = payload.get("data")
        if not isinstance(data, dict):
            data = payload

        direct_candidates = ("credits_left", "remaining", "remaining_credits", "credits")
        for key in direct_candidates:
            if key in data:
                return self._normalize_score(data.get(key))

        total = data.get("total_credits", data.get("limit"))
        used = data.get("total_usage", data.get("usage"))
        if total is not None and used is not None:
            return self._normalize_score(total) - self._normalize_score(used)

        return None


    def _extract_input_modalities(self, payload: dict) -> set[str]:
        if not isinstance(payload, dict):
            return set()

        data = payload.get("data")
        model_data = data if isinstance(data, dict) else payload
        architecture = model_data.get("architecture", {}) if isinstance(model_data, dict) else {}

        candidates: list = []
        if isinstance(architecture, dict):
            candidates.append(architecture.get("input_modalities"))
            candidates.append(architecture.get("modalities"))

        if isinstance(model_data, dict):
            candidates.append(model_data.get("input_modalities"))
            candidates.append(model_data.get("modalities"))

        supported: set[str] = set()
        for candidate in candidates:
            if not isinstance(candidate, list):
                continue
            for entry in candidate:
                normalized = str(entry or "").strip().lower()
                if normalized:
                    supported.add(normalized)

        return supported

    def _extract_openrouter_model_entries(self, payload: dict) -> list[dict]:
        if not isinstance(payload, dict):
            return []

        data = payload.get("data")
        if isinstance(data, list):
            return [entry for entry in data if isinstance(entry, dict)]
        if isinstance(data, dict):
            return [data]
        if isinstance(payload, dict):
            return [payload]
        return []

    def _find_openrouter_model_payload(self, payload: dict, model_name: str) -> dict | None:
        normalized_model_name = str(model_name or "").strip().lower()
        if not normalized_model_name:
            return None

        for model_entry in self._extract_openrouter_model_entries(payload):
            candidate_identifiers = {
                str(model_entry.get("id", "")).strip().lower(),
                str(model_entry.get("canonical_slug", "")).strip().lower(),
                str(model_entry.get("name", "")).strip().lower(),
            }
            if normalized_model_name in candidate_identifiers:
                return model_entry

        return None

    def fetch_openrouter_input_modalities(self) -> set[str]:
        model_name = str(self.model or "").strip()
        if not model_name:
            return set()

        payload = self._request_openrouter_json("/api/v1/models")
        matched_model_payload = self._find_openrouter_model_payload(payload, model_name)
        if matched_model_payload is None:
            logger.info("OpenRouter model metadata not found for configured model: %s", model_name)
            return set()
        return self._extract_input_modalities(matched_model_payload)

    def fetch_openrouter_credits_left(self) -> float:
        endpoints_to_try = ["/api/v1/credits", "/api/v1/auth/key"]
        last_error: Exception | None = None

        for endpoint in endpoints_to_try:
            try:
                payload = self._request_openrouter_json(endpoint)
                credits_left = self._extract_credits_left(payload)
                if credits_left is not None:
                    return max(0.0, credits_left)
            except Exception as exc:
                last_error = exc
                continue

        if last_error:
            raise RuntimeError(f"Failed to fetch OpenRouter credits: {last_error}") from last_error
        raise RuntimeError("Failed to fetch OpenRouter credits: response did not include remaining credits.")

    def _request_ai_feedback(self, note_name: str, messages: list[dict]) -> dict:
        try:
            return self._request_ai_feedback_once(note_name, messages, use_strict_schema=True)
        except OpenRouterImageNotSupportedError:
            raise
        except (ValueError, RuntimeError) as exc:
            logger.warning(
                "Strict AI feedback request failed for model %s. Retrying without json_schema. Reason: %s",
                self.model,
                exc,
            )
            return self._request_ai_feedback_once(note_name, messages, use_strict_schema=False)

    def _request_ai_json_object_once(self, note_name: str, messages: list[dict], use_strict_schema: bool) -> dict:
        payload = self._build_request_payload(messages, use_strict_schema=use_strict_schema)
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
            "HTTP-Referer": self.http_referer,
            "X-Title": self.app_title,
        }
        request_object = urllib_request.Request(
            self.base_url,
            data=json.dumps(payload).encode("utf-8"),
            headers=headers,
            method="POST",
        )
        with urllib_request.urlopen(request_object, timeout=self.request_timeout_seconds) as response:
            response_json = json.loads(self._read_response_with_deadline(response))
        return self._parse_json_payload(self._extract_response_content(response_json))

    def generate_learning_questions(self, note_name: str, note_content: str, prompt_content: str) -> dict:
        if not self.base_url:
            raise ValueError("AI feedback base_url is missing in conf.json.")
        if not self.api_key or self.api_key == "-":
            raise ValueError("AI feedback api_key is missing in conf.json.")
        if not self.model:
            raise ValueError("AI feedback model is missing in conf.json.")

        messages = [
            {
                "role": "system",
                "content": (
                    "Generate exam questions from markdown notes. "
                    "Return exactly one JSON object with keys 'questions' and 'answers'."
                ),
            },
            {
                "role": "user",
                "content": (
                    f"Prompt instructions:\n{prompt_content}\n\n"
                    f"Source note name: {note_name}\n"
                    f"Source note markdown:\n{note_content}\n\n"
                    "Return JSON only."
                ),
            },
        ]
        try:
            return self._request_ai_json_object_once(note_name, messages, use_strict_schema=True)
        except Exception:
            logger.warning("Strict learning question generation failed. Retrying without json_schema.")
            return self._request_ai_json_object_once(note_name, messages, use_strict_schema=False)

    def generate_feedback(
        self,
        file_name: str,
        previous_feedback: dict | None = None,
        include_images: bool = True,
    ) -> dict:
        try:
            doc_path = self._resolve_doc_path(file_name)
            note_name = doc_path.stem.strip()
            doc_content = doc_path.read_text(encoding="utf-8")
            evaluation_context = self._read_template(self.prompt_template_path, "AI prompt")
            feedback_template = self._read_template(self.feedback_template_path, "AI feedback")

            ai_feedback = self._request_ai_feedback(
                note_name,
                self._build_messages(
                    note_name,
                    doc_content,
                    evaluation_context,
                    previous_feedback=previous_feedback,
                    include_images=include_images,
                ),
            )
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
