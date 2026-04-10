"""
DingTalk platform adapter.

Supports:
- Stream-mode WebSocket long connection via dingtalk-stream SDK
- Direct-message text receive/send with session context
- Inbound image/file/audio/video caching
- Outbound image via media upload + native image message (mediaId)
- Outbound document/file via Wiki workspace upload
- OpenAPI integration (access token, union ID resolution)
"""

# ---------------------------------------------------------------------------
# Standard library
# ---------------------------------------------------------------------------

import asyncio
import json
import logging
import mimetypes
import os
import random
import re
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional
from urllib.parse import urlsplit

# ---------------------------------------------------------------------------
# Third-party (optional)
# ---------------------------------------------------------------------------

try:
    import dingtalk_stream
    from dingtalk_stream import AsyncChatbotHandler, ChatbotMessage
    DINGTALK_STREAM_AVAILABLE = True
except ImportError:
    DINGTALK_STREAM_AVAILABLE = False
    dingtalk_stream = None  # type: ignore[assignment]
    AsyncChatbotHandler = object  # type: ignore[assignment]

try:
    import httpx
    HTTPX_AVAILABLE = True
except ImportError:
    HTTPX_AVAILABLE = False
    httpx = None  # type: ignore[assignment]

# ---------------------------------------------------------------------------
# Internal
# ---------------------------------------------------------------------------

from gateway.config import Platform, PlatformConfig
from gateway.platforms.base import (
    BasePlatformAdapter,
    MessageEvent,
    MessageType,
    SendResult,
    cache_audio_from_bytes,
    cache_document_from_bytes,
    cache_image_from_bytes,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants & tuning
# ---------------------------------------------------------------------------

MAX_MESSAGE_LENGTH = 20000
DEDUP_WINDOW_SECONDS = 300
DEDUP_MAX_SIZE = 1000
RECONNECT_BACKOFF = [2, 5, 10, 30, 60]
_QUERY_SECRET_RE = re.compile(r"(?P<key>session|access_token)=([^&]+)", re.IGNORECASE)
_LONG_SECRET_RE = re.compile(r"([A-Za-z0-9+/=_-]{16,})")


def check_dingtalk_requirements() -> bool:
    """Check if DingTalk dependencies are available."""
    try:
        import dingtalk_stream  # noqa: F401
        return True
    except ImportError:
        return False


class DingTalkAdapter(BasePlatformAdapter):
    """DingTalk bot adapter via Stream mode."""

    MAX_MESSAGE_LENGTH = MAX_MESSAGE_LENGTH

    # =========================================================================
    # Lifecycle — init / connect / disconnect
    # =========================================================================

    def __init__(self, config: PlatformConfig):
        super().__init__(config, Platform.DINGTALK)

        extra = config.extra or {}
        self._client_id: str = extra.get("client_id") or os.getenv("DINGTALK_CLIENT_ID", "")
        self._client_secret: str = extra.get("client_secret") or os.getenv("DINGTALK_CLIENT_SECRET", "")
        self._agent_id: str = str(extra.get("agent_id") or os.getenv("DINGTALK_AGENT_ID", "") or "")
        self._robot_code: str = str(
            extra.get("robot_code")
            or os.getenv("DINGTALK_ROBOT_CODE", "")
            or self._client_id
            or ""
        )

        self._stream_client: Any = None
        self._stream_task: Optional[asyncio.Task] = None
        self._http_client: Optional["httpx.AsyncClient"] = None

        # Message deduplication: msg_id -> timestamp
        self._seen_messages: Dict[str, float] = {}
        # Map chat_id -> session_webhook for reply routing
        self._session_webhooks: Dict[str, str] = {}
        # Chat/session context for DM media upload-link replies
        self._session_context: Dict[str, Dict[str, Any]] = {}
        self._user_union_ids: Dict[str, str] = {}
        self._wiki_workspaces: Dict[str, tuple] = {}
        self._access_token: str = ""
        self._access_token_expires_at: float = 0.0

    async def connect(self) -> bool:
        """Connect to DingTalk via Stream Mode."""
        if not DINGTALK_STREAM_AVAILABLE:
            logger.warning("[%s] dingtalk-stream not installed. Run: pip install dingtalk-stream", self.name)
            return False
        if not HTTPX_AVAILABLE:
            logger.warning("[%s] httpx not installed. Run: pip install httpx", self.name)
            return False
        if not self._client_id or not self._client_secret:
            logger.warning("[%s] DINGTALK_CLIENT_ID and DINGTALK_CLIENT_SECRET required", self.name)
            return False

        try:
            self._http_client = httpx.AsyncClient(timeout=30.0)
            credential = dingtalk_stream.Credential(self._client_id, self._client_secret)
            self._stream_client = dingtalk_stream.DingTalkStreamClient(credential)

            # Capture the current event loop for cross-thread dispatch
            loop = asyncio.get_running_loop()
            handler = _IncomingHandler(self, loop)
            self._stream_client.register_callback_handler(
                dingtalk_stream.ChatbotMessage.TOPIC, handler
            )

            self._stream_task = asyncio.create_task(self._run_stream())
            self._mark_connected()
            logger.info("[%s] Connected via Stream Mode", self.name)
            return True
        except Exception as e:
            logger.error("[%s] Failed to connect: %s", self.name, e)
            return False

    async def _run_stream(self) -> None:
        """Run the blocking stream client with auto-reconnection."""
        backoff_idx = 0
        while self._running:
            try:
                logger.debug("[%s] Starting stream client...", self.name)
                await self._stream_client.start()
            except asyncio.CancelledError:
                return
            except Exception as e:
                if not self._running:
                    return
                logger.warning("[%s] Stream client error: %s", self.name, self._redact(e))

            if not self._running:
                return

            delay = self._compute_reconnect_delay(backoff_idx)
            logger.info("[%s] Reconnecting in %ds...", self.name, delay)
            await asyncio.sleep(delay)
            backoff_idx += 1

    async def disconnect(self) -> None:
        """Disconnect from DingTalk."""
        self._running = False
        self._mark_disconnected()

        if self._stream_task:
            self._stream_task.cancel()
            try:
                await self._stream_task
            except asyncio.CancelledError:
                pass
            self._stream_task = None

        if self._http_client:
            await self._http_client.aclose()
            self._http_client = None

        self._stream_client = None
        self._session_webhooks.clear()
        self._seen_messages.clear()
        self._wiki_workspaces.clear()
        logger.info("[%s] Disconnected", self.name)

    # =========================================================================
    # Inbound — message handling & parsing
    # =========================================================================

    async def _on_message(self, message: "ChatbotMessage") -> None:
        """Process an incoming DingTalk chatbot message."""
        msg_id = getattr(message, "message_id", None) or uuid.uuid4().hex
        if self._should_ignore_message(message):
            logger.debug("[%s] Ignoring self/echo DingTalk message %s", self.name, self._redact(msg_id))
            return
        if self._is_duplicate(msg_id):
            logger.debug("[%s] Duplicate message %s, skipping", self.name, self._redact(msg_id))
            return

        text = self._extract_text(message)
        raw_type = str(getattr(message, "message_type", "") or "").strip().lower()

        # Chat context
        conversation_id = getattr(message, "conversation_id", "") or ""
        conversation_type = getattr(message, "conversation_type", "1")
        is_group = str(conversation_type) == "2"
        sender_id = getattr(message, "sender_id", "") or ""
        sender_nick = getattr(message, "sender_nick", "") or sender_id
        sender_staff_id = getattr(message, "sender_staff_id", "") or ""
        if not sender_staff_id:
            raw = getattr(message, "_raw_data", None) or {}
            if isinstance(raw, str):
                try:
                    raw = json.loads(raw)
                except Exception:
                    raw = {}
            if isinstance(raw, dict):
                sender_staff_id = str(raw.get("senderStaffId") or raw.get("sender_staff_id") or "")

        # Strictly separate group vs DM chat_id to prevent cross-talk
        if is_group:
            if not conversation_id:
                logger.warning("[%s] Group message missing conversation_id, skipping. sender=%s", self.name, self._redact(sender_id))
                return
            chat_id, chat_type = conversation_id, "group"
        else:
            chat_id = sender_id
            if not chat_id:
                logger.warning("[%s] DM message missing sender_id, skipping", self.name)
                return
            chat_type = "dm"

        # Store session webhook for reply routing
        session_webhook = getattr(message, "session_webhook", None) or ""
        if session_webhook and chat_id:
            self._session_webhooks[chat_id] = session_webhook
            self._session_context.setdefault(chat_id, {})["session_webhook"] = session_webhook
        if chat_id:
            context = self._session_context.setdefault(chat_id, {})
            context.update(
                {
                    "chat_type": chat_type,
                    "conversation_id": conversation_id,
                    "user_id": sender_id,
                    "user_name": sender_nick,
                    "sender_staff_id": sender_staff_id,
                }
            )

        source = self.build_source(
            chat_id=chat_id, chat_name=getattr(message, "conversation_title", None),
            chat_type=chat_type, user_id=sender_id, user_name=sender_nick,
            user_id_alt=sender_staff_id if sender_staff_id else None,
        )

        # Parse timestamp
        create_at = getattr(message, "create_at", None)
        try:
            timestamp = datetime.fromtimestamp(int(create_at) / 1000, tz=timezone.utc) if create_at else datetime.now(tz=timezone.utc)
        except (ValueError, OSError, TypeError):
            timestamp = datetime.now(tz=timezone.utc)

        message_type = MessageType.TEXT
        media_urls: list[str] = []
        media_types: list[str] = []
        if (
            raw_type == "picture"
            or (raw_type == "richtext" and self._has_rich_text_media(message, "picture"))
            or self._reply_message_type(message) == "picture"
        ):
            cached_path, media_type = await self._download_inbound_picture(message)
            media_urls, media_types, message_type = [cached_path], [media_type], MessageType.PHOTO
        elif (
            raw_type == "file"
            or (raw_type == "richtext" and self._has_rich_text_media(message, "file"))
            or self._reply_message_type(message) == "file"
        ):
            cached_path, media_type = await self._download_inbound_file(message)
            media_urls, media_types = [cached_path], [media_type]
            message_type = MessageType.PHOTO if media_type.startswith("image/") else MessageType.DOCUMENT
            if message_type == MessageType.DOCUMENT and media_type.startswith("text/"):
                try:
                    injected_text = Path(cached_path).read_text(encoding="utf-8")
                    text = f"{text}\n{injected_text}".strip()
                except Exception:
                    pass
        elif raw_type == "audio" or self._reply_message_type(message) == "audio":
            cached_path, media_type = await self._download_inbound_audio(message)
            media_urls, media_types, message_type = [cached_path], [media_type], MessageType.AUDIO
        elif raw_type == "video" or self._reply_message_type(message) == "video":
            cached_path, media_type = await self._download_inbound_video(message)
            media_urls, media_types, message_type = [cached_path], [media_type], MessageType.VIDEO

        # Strip quoted-reply markers for non-text messages (e.g. "[引用] [图片]")
        if message_type != MessageType.TEXT and text:
            text = re.sub(r"^\s*\[引用\]\s*\[(?:图片|文件|音频|视频)\]\s*", "", text).strip()

        if not text and not media_urls:
            logger.debug("[%s] Empty message, skipping", self.name)
            return
        event = MessageEvent(
            text=text,
            message_type=message_type,
            source=source,
            message_id=msg_id,
            raw_message=message,
            media_urls=media_urls,
            media_types=media_types,
            timestamp=timestamp,
        )
        logger.debug("[%s] Message from %s in %s: %s",
                      self.name, self._redact(sender_nick), self._redact(chat_id[:20] if chat_id else "?"), text[:50])
        await self.handle_message(event)

    # =========================================================================
    # Helpers — redaction, reconnect delay
    # =========================================================================

    @staticmethod
    def _mask_secret(value: str) -> str:
        if len(value) <= 8:
            return "***"
        return f"{value[:4]}***{value[-4:]}"

    def _redact(self, value: Any) -> str:
        text = str(value)
        text = _QUERY_SECRET_RE.sub(lambda match: f"{match.group('key')}={self._mask_secret(match.group(2))}", text)
        return _LONG_SECRET_RE.sub(lambda match: self._mask_secret(match.group(1)), text)

    @classmethod
    def _should_ignore_message(cls, message: "ChatbotMessage") -> bool:
        """Return True for self-messages and echo/sync messages."""
        sender_id = cls._get_attr_variants(message, "sender_id", "senderId")
        chatbot_user_id = cls._get_attr_variants(message, "chatbot_user_id", "chatbotUserId")
        if sender_id and chatbot_user_id and str(sender_id) == str(chatbot_user_id):
            return True
        if bool(getattr(message, "is_echo", False) or getattr(message, "isEcho", False)):
            return True
        source = str(cls._get_attr_variants(message, "message_source", "messageSource") or "").strip().lower()
        return source in {"bot", "echo", "sync"}

    @staticmethod
    def _get_attr_variants(obj: Any, *names: str) -> Any:
        """Try multiple attribute names, return the first truthy value."""
        for name in names:
            val = getattr(obj, name, None)
            if val is not None:
                return val
        return None

    @staticmethod
    def _raise_http_error(response: Any, operation: str) -> None:
        """Raise RuntimeError with HTTP status and truncated body."""
        body = ""
        try:
            body = response.text[:500]
        except Exception:
            pass
        raise RuntimeError(f"DingTalk {operation} failed: HTTP {response.status_code} — {body}")

    @staticmethod
    def _compute_reconnect_delay(backoff_idx: int) -> float:
        base = RECONNECT_BACKOFF[min(backoff_idx, len(RECONNECT_BACKOFF) - 1)]
        return base + random.uniform(0.0, 1.0)

    @staticmethod
    def _extract_text(message: "ChatbotMessage") -> str:
        """Extract plain text from a DingTalk chatbot message."""
        text = getattr(message, "text", None) or ""
        if isinstance(text, dict):
            content = text.get("content", "").strip()
        elif hasattr(text, "content"):
            content = str(getattr(text, "content", "") or "").strip()
        else:
            content = str(text).strip()

        # Fall back to rich text if present
        if not content:
            rich_text = DingTalkAdapter._rich_text_items(message)
            if rich_text:
                parts = [
                    item["text"]
                    for item in rich_text
                    if isinstance(item, dict) and item.get("text")
                ]
                content = " ".join(parts).strip()

        # Fall back to audio recognition text
        if not content:
            recognition = str(DingTalkAdapter._message_content(message).get("recognition") or "").strip()
            if recognition:
                content = recognition

        return content

    @staticmethod
    def _coerce_mapping(value: Any) -> Dict[str, Any]:
        if isinstance(value, dict):
            return value
        if isinstance(value, str):
            try:
                parsed = json.loads(value)
            except Exception:
                return {}
            return parsed if isinstance(parsed, dict) else {}
        if value is None:
            return {}
        if hasattr(value, "__dict__"):
            return {
                key: field_value
                for key, field_value in vars(value).items()
                if not key.startswith("_")
            }
        return {}

    @classmethod
    def _text_payload(cls, message: "ChatbotMessage") -> Dict[str, Any]:
        return cls._coerce_mapping(getattr(message, "text", None))

    @staticmethod
    def _message_content(message: "ChatbotMessage") -> Dict[str, Any]:
        content = getattr(message, "content", None)
        if isinstance(content, dict):
            return content
        extensions = getattr(message, "extensions", None)
        if isinstance(extensions, dict):
            ext_content = extensions.get("content")
            if isinstance(ext_content, dict):
                return ext_content
        return {}

    @staticmethod
    def _rich_text_items(message: "ChatbotMessage") -> list[dict[str, Any]]:
        rich_text_content = getattr(message, "rich_text_content", None)
        rich_text_list = getattr(rich_text_content, "rich_text_list", None)
        if isinstance(rich_text_list, list):
            return [item for item in rich_text_list if isinstance(item, dict)]
        rich_text = getattr(message, "rich_text", None)
        if isinstance(rich_text, list):
            return [item for item in rich_text if isinstance(item, dict)]
        content = DingTalkAdapter._message_content(message)
        content_rich_text = content.get("richText") or content.get("richTextList") or []
        if isinstance(content_rich_text, list):
            return [item for item in content_rich_text if isinstance(item, dict)]
        return []

    @staticmethod
    def _rich_text_item_kind(item: Dict[str, Any]) -> str:
        item_type = str(item.get("type") or "").strip().lower()
        if item.get("pictureUrl"):
            return "picture"
        if not item.get("downloadCode"):
            return ""
        if item_type in {"", "picture", "image"}:
            return "picture"
        if item_type == "file":
            return "file"
        if item_type in {"audio", "voice"}:
            return "audio"
        if item_type == "video":
            return "video"
        return ""

    @classmethod
    def _find_rich_text_item(cls, message: "ChatbotMessage", kind: str) -> Dict[str, Any]:
        for item in cls._rich_text_items(message):
            if cls._rich_text_item_kind(item) == kind:
                return item
        return {}

    @classmethod
    def _has_rich_text_media(cls, message: "ChatbotMessage", kind: str) -> bool:
        return bool(cls._find_rich_text_item(message, kind))

    @classmethod
    def _reply_payload(cls, message: "ChatbotMessage") -> Dict[str, Any]:
        text_payload = cls._text_payload(message)
        reply = cls._coerce_mapping(text_payload.get("repliedMsg"))
        if reply:
            return reply
        return cls._coerce_mapping(cls._message_content(message).get("repliedMsg"))

    @classmethod
    def _reply_message_type(cls, message: "ChatbotMessage") -> str:
        reply = cls._reply_payload(message)
        return str(reply.get("msgType") or reply.get("messageType") or "").strip().lower()

    @classmethod
    def _reply_content(cls, message: "ChatbotMessage") -> Dict[str, Any]:
        return cls._coerce_mapping(cls._reply_payload(message).get("content"))

    @classmethod
    def _reply_rich_text_item(cls, message: "ChatbotMessage", kind: str) -> Dict[str, Any]:
        reply_content = cls._reply_content(message)
        rich_text = reply_content.get("richText") or reply_content.get("richTextList") or []
        if not isinstance(rich_text, list):
            return {}
        for item in rich_text:
            if isinstance(item, dict) and cls._rich_text_item_kind(item) == kind:
                return item
        return {}

    async def _download_by_code(self, download_code: str, robot_code: str, fallback_mime: str) -> tuple[Any, str]:
        """Download file via downloadCode + robotCode, return (response, media_type)."""
        download_url = await self._query_message_file_download_url(
            download_code=str(download_code), robot_code=str(robot_code),
        )
        response = await self._download_remote_response(download_url)
        return response, self._response_media_type(response, fallback=fallback_mime)

    async def _download_inbound_picture(self, message: "ChatbotMessage") -> tuple[str, str]:
        content = self._message_content(message)
        rich_text_item = self._find_rich_text_item(message, "picture")
        reply_type = self._reply_message_type(message)
        reply_content = self._reply_content(message)
        reply_rich_text_item = self._reply_rich_text_item(message, "picture")
        picture_url = (
            str(content.get("pictureUrl") or "")
            or str(rich_text_item.get("pictureUrl") or "")
            or (str(reply_content.get("pictureUrl") or "") if reply_type == "picture" else "")
            or str(reply_rich_text_item.get("pictureUrl") or "")
        )
        download_code = (
            self._get_attr_variants(message, "picture_download_code", "pictureDownloadCode")
            or getattr(getattr(message, "image_content", None), "download_code", None)
            or content.get("pictureDownloadCode")
            or self._get_attr_variants(message, "download_code", "downloadCode")
            or content.get("downloadCode")
            or rich_text_item.get("downloadCode")
            or (reply_content.get("downloadCode") if reply_type == "picture" else "")
            or reply_rich_text_item.get("downloadCode")
            or ""
        )
        if picture_url:
            response = await self._download_remote_response(str(picture_url))
            fallback = mimetypes.guess_type(urlsplit(str(picture_url)).path)[0] or "image/jpeg"
            media_type = self._response_media_type(response, fallback=fallback)
            ext = mimetypes.guess_extension(media_type) or ".jpg"
            return cache_image_from_bytes(response.content, ext=ext), media_type

        robot_code = self._get_attr_variants(message, "robot_code", "robotCode") or self._robot_code or ""
        if not download_code or not robot_code:
            raise RuntimeError("Missing DingTalk picture downloadCode or robotCode")

        response, media_type = await self._download_by_code(download_code, robot_code, "image/jpeg")
        ext = mimetypes.guess_extension(media_type) or ".jpg"
        return cache_image_from_bytes(response.content, ext=ext), media_type

    async def _download_inbound_file(self, message: "ChatbotMessage") -> tuple[str, str]:
        content = self._message_content(message)
        rich_text_item = self._find_rich_text_item(message, "file")
        reply_type = self._reply_message_type(message)
        reply_content = self._reply_content(message)
        reply_rich_text_item = self._reply_rich_text_item(message, "file")
        file_name = str(
            self._get_attr_variants(message, "file_name", "fileName")
            or content.get("fileName")
            or rich_text_item.get("fileName")
            or (reply_content.get("fileName") if reply_type == "file" else "")
            or reply_rich_text_item.get("fileName")
            or "attachment"
        )
        download_code = (
            self._get_attr_variants(message, "download_code", "downloadCode")
            or content.get("downloadCode")
            or rich_text_item.get("downloadCode")
            or (reply_content.get("downloadCode") if reply_type == "file" else "")
            or reply_rich_text_item.get("downloadCode")
            or ""
        )
        robot_code = self._get_attr_variants(message, "robot_code", "robotCode") or self._robot_code or ""
        if not download_code or not robot_code:
            raise RuntimeError("Missing DingTalk file downloadCode or robotCode")

        fallback = mimetypes.guess_type(file_name)[0] or "application/octet-stream"
        response, media_type = await self._download_by_code(download_code, robot_code, fallback)
        return cache_document_from_bytes(response.content, file_name), media_type

    async def _download_inbound_media(
        self, message: "ChatbotMessage", media_kind: str, fallback_mime: str, default_ext: str,
    ) -> tuple[str, str]:
        """Generic download for audio/video inbound media."""
        content = self._message_content(message)
        reply_type = self._reply_message_type(message)
        reply_content = self._reply_content(message)
        reply_rich_text_item = self._reply_rich_text_item(message, media_kind)
        download_code = (
            self._get_attr_variants(message, "download_code", "downloadCode")
            or content.get("downloadCode")
            or (reply_content.get("downloadCode") if reply_type == media_kind else "")
            or reply_rich_text_item.get("downloadCode")
            or ""
        )
        robot_code = self._get_attr_variants(message, "robot_code", "robotCode") or self._robot_code or ""
        if not download_code or not robot_code:
            raise RuntimeError(f"Missing DingTalk {media_kind} downloadCode or robotCode")

        response, media_type = await self._download_by_code(download_code, robot_code, fallback_mime)
        suffix = mimetypes.guess_extension(media_type) or default_ext
        if media_kind == "audio":
            return cache_audio_from_bytes(response.content, ext=suffix), media_type
        return cache_document_from_bytes(response.content, f"{media_kind}{suffix}"), media_type

    async def _download_inbound_audio(self, message: "ChatbotMessage") -> tuple[str, str]:
        """Download inbound audio message."""
        return await self._download_inbound_media(message, "audio", "audio/ogg", ".ogg")

    async def _download_inbound_video(self, message: "ChatbotMessage") -> tuple[str, str]:
        """Download inbound video message."""
        return await self._download_inbound_media(message, "video", "video/mp4", ".mp4")

    @staticmethod
    def _response_media_type(response: Any, *, fallback: str) -> str:
        headers = getattr(response, "headers", {}) or {}
        media_type = headers.get("content-type") or headers.get("Content-Type") or fallback
        normalized = str(media_type).split(";")[0].strip() or fallback
        return fallback if (normalized == "application/octet-stream" and fallback) else normalized

    async def _download_remote_response(self, url: str) -> Any:
        if not self._http_client:
            raise RuntimeError("HTTP client not initialized")
        response = await self._http_client.get(url, timeout=30.0)
        if response.status_code >= 400:
            raise RuntimeError(f"DingTalk media download failed: HTTP {response.status_code}")
        return response

    async def _query_message_file_download_url(self, *, download_code: str, robot_code: str) -> str:
        if not self._http_client:
            raise RuntimeError("HTTP client not initialized")
        access_token = await self._get_access_token()
        response = await self._http_client.post(
            "https://api.dingtalk.com/v1.0/robot/messageFiles/download",
            headers={"x-acs-dingtalk-access-token": access_token},
            json={"downloadCode": download_code, "robotCode": robot_code},
            timeout=15.0,
        )
        if response.status_code >= 400:
            raise RuntimeError(f"DingTalk message file download URL request failed: HTTP {response.status_code}")
        payload = response.json()
        download_url = str(payload.get("downloadUrl") or "")
        if not download_url:
            raise RuntimeError("DingTalk message file download response missing downloadUrl")
        return download_url

    # =========================================================================
    # Deduplication
    # =========================================================================

    def _is_duplicate(self, msg_id: str) -> bool:
        """Check and record a message ID. Returns True if already seen."""
        now = time.time()
        if len(self._seen_messages) > DEDUP_MAX_SIZE:
            cutoff = now - DEDUP_WINDOW_SECONDS
            expired = [k for k, v in self._seen_messages.items() if v <= cutoff]
            for k in expired:
                del self._seen_messages[k]

        if msg_id in self._seen_messages:
            return True
        self._seen_messages[msg_id] = now
        return False

    # =========================================================================
    # Outbound — send / send_image / send_document / send_voice / send_video
    # =========================================================================

    async def send(
        self, chat_id: str, content: str,
        reply_to: Optional[str] = None, metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        """Send a markdown reply via DingTalk session webhook."""
        metadata = metadata or {}
        session_webhook = metadata.get("session_webhook") or self._session_webhooks.get(chat_id)
        if not session_webhook:
            return SendResult(success=False,
                              error="No session_webhook available. Reply must follow an incoming message.")
        if not self._http_client:
            return SendResult(success=False, error="HTTP client not initialized")
        payload = {"msgtype": "text", "text": {"content": content[:self.MAX_MESSAGE_LENGTH]}}

        try:
            resp = await self._http_client.post(session_webhook, json=payload, timeout=15.0)
            if resp.status_code < 400:
                return SendResult(success=True, message_id=uuid.uuid4().hex[:12])
            body = resp.text
            logger.warning("[%s] Send failed HTTP %d: %s", self.name, resp.status_code, body[:200])
            return SendResult(success=False, error=f"HTTP {resp.status_code}: {body[:200]}")
        except httpx.TimeoutException:
            return SendResult(success=False, error="Timeout sending message to DingTalk")
        except Exception as e:
            logger.error("[%s] Send error: %s", self.name, e)
            return SendResult(success=False, error=str(e))

    async def send_typing(self, chat_id: str, metadata=None) -> None:
        """DingTalk does not support typing indicators."""
        pass

    async def get_chat_info(self, chat_id: str) -> Dict[str, Any]:
        """Return basic info about a DingTalk conversation."""
        context = self._session_context.get(chat_id, {})
        return {
            "chat_id": chat_id,
            "name": context.get("chat_name") or chat_id,
            "type": context.get("chat_type") or ("group" if "group" in chat_id.lower() else "dm"),
        }

    async def _send_error_notice(
        self, chat_id: str, context: Dict[str, Any], error_text: str,
        reply_to: Optional[str] = None, prefix: str = "Operation failed.",
    ) -> None:
        """Best-effort user-visible error reply via the session webhook."""
        session_webhook = context.get("session_webhook") or self._session_webhooks.get(chat_id)
        if not session_webhook:
            return
        detail = re.sub(r"\s+", " ", str(error_text or "Unknown DingTalk error").strip())
        if len(detail) > 900:
            detail = detail[:897] + "..."
        try:
            await self.send(chat_id, f"{prefix}\nError: {detail}", reply_to=reply_to, metadata={"session_webhook": session_webhook})
        except Exception as exc:
            logger.warning("[%s] Failed to send error notice: %s", self.name, exc)

    async def send_document(
        self, chat_id: str, file_path: str, caption: Optional[str] = None,
        file_name: Optional[str] = None, reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None, **kwargs,
    ) -> SendResult:
        """Upload a document for the requesting user, then reply with a text link."""
        context = dict(self._session_context.get(chat_id, {}))
        context.update(metadata or {})
        # Guard: file exists
        if not os.path.exists(file_path):
            error = f"File not found: {file_path}"
            await self._send_error_notice(chat_id, context, error, reply_to=reply_to, prefix="File send failed.")
            return SendResult(success=False, error=error)
        # Guard: sender_staff_id required (prefer metadata for group-chat stability)
        operator_user_id = str(
            (metadata or {}).get("sender_staff_id")
            or context.get("sender_staff_id")
            or ""
        ).strip()
        if not operator_user_id:
            error = "Missing sender_staff_id in session context. Ensure the bot has permission to read sender_staff_id."
            logger.warning("[%s] send_document: %s for chat_id=%s", self.name, error, self._redact(chat_id[:20] if chat_id else "?"))
            await self._send_error_notice(chat_id, context, error, reply_to=reply_to, prefix="File send failed.")
            return SendResult(success=False, error=error)

        try:
            union_id = await self._ensure_user_union_id(operator_user_id)
            display_name = file_name or Path(file_path).name
            upload = await self._upload_file_to_space(union_id=union_id, file_path=file_path, file_name=display_name)
            download_url = upload.get("download_url") or ""
            if not download_url:
                error = "Missing DingTalk file download URL after upload"
                await self._send_error_notice(chat_id, context, error, reply_to=reply_to, prefix="File send failed.")
                return SendResult(success=False, error=error)

            lines = []
            if caption:
                lines.append(caption)
            lines.append(f"{display_name}: {download_url}")
            return await self.send(
                chat_id,
                "\n".join(lines),
                reply_to=reply_to,
                metadata={"session_webhook": context.get("session_webhook")},
            )
        except Exception as exc:
            error = str(exc)
            logger.error("[%s] send_document error: %s", self.name, error)
            await self._send_error_notice(chat_id, context, error, reply_to=reply_to, prefix="File send failed.")
            return SendResult(success=False, error=error)

    # =========================================================================
    # Media — upload / download helpers
    # =========================================================================

    async def _upload_media(self, file_path: str, media_type: str = "image") -> str:
        """Upload a file to DingTalk via the old oapi media/upload endpoint."""
        if not self._http_client:
            raise RuntimeError("HTTP client not initialized")
        if not os.path.exists(file_path):
            raise RuntimeError(f"File not found: {file_path}")
        access_token = await self._get_access_token()
        file_name = Path(file_path).name
        content_type = mimetypes.guess_type(file_name)[0] or "application/octet-stream"
        with open(file_path, "rb") as f:
            files = {"media": (file_name, f, content_type)}
            response = await self._http_client.post(
                "https://oapi.dingtalk.com/media/upload",
                params={"access_token": access_token, "type": media_type},
                files=files,
                timeout=60.0,
            )
        if response.status_code >= 400:
            raise RuntimeError(f"DingTalk media upload failed: HTTP {response.status_code}")
        payload = response.json()
        media_id = str(payload.get("media_id") or "")
        if not media_id:
            raise RuntimeError(f"DingTalk media upload response missing media_id: {payload}")
        logger.debug("[%s] Uploaded media %s -> %s", self.name, file_name, media_id)
        return media_id

    async def _send_image_message(
        self, chat_id: str, media_id: str, metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        """Send an image message using the robot messaging API."""
        if not self._http_client:
            return SendResult(success=False, error="HTTP client not initialized")
        context = dict(self._session_context.get(chat_id, {}))
        context.update(metadata or {})
        chat_type = context.get("chat_type", "dm")
        access_token = await self._get_access_token()
        msg_param = json.dumps({"photoURL": media_id})
        body: Dict[str, Any] = {
            "robotCode": self._robot_code,
            "msgKey": "sampleImageMsg",
            "msgParam": msg_param,
        }

        if chat_type == "group":
            conversation_id = context.get("conversation_id") or chat_id
            body["openConversationId"] = conversation_id
            endpoint = "https://api.dingtalk.com/v1.0/robot/groupMessages/send"
        else:
            # DM: use sender_staff_id as the user identifier
            user_id = context.get("sender_staff_id") or context.get("user_id") or ""
            if not user_id:
                return SendResult(success=False, error="Missing user ID for DM image message")
            body["userIds"] = [user_id]
            endpoint = "https://api.dingtalk.com/v1.0/robot/oToMessages/batchSend"

        try:
            resp = await self._http_client.post(
                endpoint,
                headers={
                    "x-acs-dingtalk-access-token": access_token,
                    "Content-Type": "application/json",
                },
                json=body,
                timeout=15.0,
            )
            if resp.status_code < 400:
                resp_data = resp.json()
                logger.debug("[%s] Image message sent: %s", self.name, resp_data)
                return SendResult(success=True, message_id=str(resp_data.get("processQueryKey") or uuid.uuid4().hex[:12]))
            body_text = resp.text
            logger.warning("[%s] Image send failed HTTP %d: %s", self.name, resp.status_code, body_text[:200])
            return SendResult(success=False, error=f"HTTP {resp.status_code}: {body_text[:200]}")
        except Exception as e:
            logger.error("[%s] Image send error: %s", self.name, e)
            return SendResult(success=False, error=str(e))

    async def send_image_file(
        self, chat_id: str, image_path: str, caption: Optional[str] = None,
        reply_to: Optional[str] = None, metadata: Optional[Dict[str, Any]] = None, **kwargs,
    ) -> SendResult:
        context = dict(self._session_context.get(chat_id, {}))
        context.update(metadata or {})
        if not os.path.exists(image_path):
            error = f"File not found: {image_path}"
            await self._send_error_notice(chat_id, context, error, reply_to=reply_to, prefix="Image send failed.")
            return SendResult(success=False, error=error)

        try:
            media_id = await self._upload_media(image_path, media_type="image")
            if caption:
                await self.send(chat_id, caption, reply_to=reply_to, metadata=metadata)
            result = await self._send_image_message(chat_id, media_id, metadata=metadata)
            if not result.success:
                await self._send_error_notice(chat_id, context, result.error or "Unknown image send error", reply_to=reply_to, prefix="Image send failed.")
            return result
        except Exception as exc:
            error = str(exc)
            logger.error("[%s] send_image_file error: %s", self.name, error)
            await self._send_error_notice(chat_id, context, error, reply_to=reply_to, prefix="Image send failed.")
            return SendResult(success=False, error=error)

    async def send_image(
        self, chat_id: str, image_url: str, caption: Optional[str] = None,
        reply_to: Optional[str] = None, metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        context = dict(self._session_context.get(chat_id, {}))
        context.update(metadata or {})
        try:
            response = await self._download_remote_response(image_url)
            media_type = self._response_media_type(response, fallback="image/jpeg")
            ext = mimetypes.guess_extension(media_type) or ".jpg"
            image_path = cache_image_from_bytes(response.content, ext=ext)
            return await self.send_image_file(
                chat_id, image_path, caption=caption, reply_to=reply_to, metadata=metadata,
            )
        except Exception as exc:
            error = str(exc)
            logger.error("[%s] send_image error: %s", self.name, error)
            await self._send_error_notice(chat_id, context, error, reply_to=reply_to, prefix="Image send failed.")
            return SendResult(success=False, error=error)

    async def send_voice(
        self, chat_id: str, audio_path: str, reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None, **kwargs,
    ) -> SendResult:
        return await self.send_document(
            chat_id, audio_path, caption=kwargs.get("caption"),
            file_name=kwargs.get("file_name"), reply_to=reply_to, metadata=metadata,
        )

    async def send_video(
        self, chat_id: str, video_path: str, caption: Optional[str] = None,
        reply_to: Optional[str] = None, metadata: Optional[Dict[str, Any]] = None, **kwargs,
    ) -> SendResult:
        return await self.send_document(
            chat_id, video_path, caption=caption,
            file_name=kwargs.get("file_name"), reply_to=reply_to, metadata=metadata,
        )

    async def send_animation(
        self, chat_id: str, animation_url: str, caption: Optional[str] = None,
        reply_to: Optional[str] = None, metadata: Optional[Dict[str, Any]] = None, **kwargs,
    ) -> SendResult:
        return await self.send_image(
            chat_id, animation_url, caption=caption, reply_to=reply_to, metadata=metadata,
        )

    # =========================================================================
    # OpenAPI — access token, union ID, wiki workspace
    # =========================================================================

    async def _get_access_token(self) -> str:
        now = time.time()
        if self._access_token and now < self._access_token_expires_at:
            return self._access_token
        if not self._http_client:
            raise RuntimeError("HTTP client not initialized")
        response = await self._http_client.post(
            "https://api.dingtalk.com/v1.0/oauth2/accessToken",
            json={"appKey": self._client_id, "appSecret": self._client_secret},
            timeout=15.0,
        )
        if response.status_code >= 400:
            raise RuntimeError(f"DingTalk accessToken request failed: HTTP {response.status_code}")
        payload = response.json()
        access_token = str(payload.get("accessToken") or "")
        expire_in = int(payload.get("expireIn") or 0)
        if not access_token:
            raise RuntimeError("DingTalk accessToken response missing accessToken")
        self._access_token = access_token
        self._access_token_expires_at = now + max(expire_in - 60, 0)
        return access_token

    async def _ensure_user_union_id(self, user_id: str) -> str:
        if not user_id:
            raise RuntimeError("Cannot resolve union_id: user_id (sender_staff_id) is empty")
        cached = self._user_union_ids.get(user_id)
        if cached:
            return cached
        context = self._session_context.values()
        for item in context:
            if (item.get("user_id") == user_id or item.get("sender_staff_id") == user_id) and item.get("union_id"):
                union_id = str(item["union_id"])
                self._user_union_ids[user_id] = union_id
                return union_id
        union_id = await self._fetch_user_union_id(user_id)
        self._user_union_ids[user_id] = union_id
        for item in self._session_context.values():
            if item.get("user_id") == user_id or item.get("sender_staff_id") == user_id:
                item["union_id"] = union_id
        return union_id

    async def _upload_file_to_space(self, union_id: str, file_path: str, file_name: str) -> Dict[str, Any]:
        """Upload file to user's personal wiki space, return dentry_uuid + download_url."""
        workspace_id, root_node_id, space_id = await self._resolve_wiki_root_node(union_id)
        file_size = Path(file_path).stat().st_size
        upload_info = await self._query_upload_info(
            union_id=union_id, root_node_id=root_node_id, file_path=file_path, file_name=file_name,
        )
        await self._put_uploaded_file(
            file_path, resource_url=upload_info["resource_url"], headers=upload_info.get("headers") or {},
        )
        file_record = await self._submit_uploaded_file(
            union_id=union_id, root_node_id=root_node_id, file_name=file_name,
            media_id=upload_info["media_id"], file_size=file_size,
        )
        doc_url = await self._query_dentry_url(
            union_id=union_id, space_id=workspace_id, dentry_uuid=file_record["dentry_uuid"],
        )
        return {"dentry_uuid": file_record["dentry_uuid"], "download_url": doc_url}

    async def _fetch_user_union_id(self, user_id: str) -> str:
        if not self._http_client:
            raise RuntimeError("HTTP client not initialized")
        access_token = await self._get_access_token()
        response = await self._http_client.post(
            "https://oapi.dingtalk.com/topapi/v2/user/get",
            params={"access_token": access_token},
            json={"userid": user_id, "language": "zh_CN"},
            timeout=15.0,
        )
        if response.status_code >= 400:
            raise RuntimeError(f"DingTalk user detail request failed: HTTP {response.status_code}")
        payload = response.json()
        if int(payload.get("errcode") or 0) != 0:
            raise RuntimeError(payload.get("errmsg") or f"Failed to fetch DingTalk user detail for {user_id}")
        union_id = str(((payload.get("result") or {}).get("unionid")) or "")
        if not union_id:
            raise RuntimeError(f"Missing unionId for DingTalk user {user_id}")
        return union_id

    async def _resolve_wiki_root_node(self, union_id: str) -> tuple:
        """Resolve the user's personal document workspace_id and root_node_id."""
        cached = self._wiki_workspaces.get(union_id)
        if cached:
            return cached
        if not self._http_client:
            raise RuntimeError("HTTP client not initialized")
        access_token = await self._get_access_token()
        response = await self._http_client.get(
            "https://api.dingtalk.com/v2.0/wiki/mineWorkspaces",
            params={"operatorId": union_id},
            headers={"x-acs-dingtalk-access-token": access_token},
            timeout=15.0,
        )
        if response.status_code >= 400:
            self._raise_http_error(response, "wiki workspace query")

        payload = response.json()
        logger.debug("[DingTalk] Wiki workspace response: %s", json.dumps(payload, ensure_ascii=False)[:500])
        ws = payload.get("workspace") or {}
        workspace_id = str(ws.get("workspaceId") or ws.get("workspace_id") or "")
        root_node_id = str(ws.get("rootNodeId") or ws.get("root_node_id") or "")
        space_id = str(ws.get("spaceId") or ws.get("space_id") or "")
        if not workspace_id or not root_node_id:
            raise RuntimeError(f"DingTalk wiki workspace response missing workspaceId or rootNodeId: {list(ws.keys())}")

        result = (workspace_id, root_node_id, space_id)
        self._wiki_workspaces[union_id] = result
        return result

    async def _query_upload_info(self, *, union_id: str, root_node_id: str, file_path: str, file_name: str) -> Dict[str, Any]:
        if not self._http_client:
            raise RuntimeError("HTTP client not initialized")
        file_size = Path(file_path).stat().st_size
        access_token = await self._get_access_token()
        response = await self._http_client.post(
            f"https://api.dingtalk.com/v2.0/storage/spaces/files/{root_node_id}/uploadInfos/query",
            params={"unionId": union_id},
            headers={"x-acs-dingtalk-access-token": access_token},
            json={
                "protocol": "HEADER_SIGNATURE",
                "option": {
                    "storageDriver": "DINGTALK",
                    "preCheckParam": {
                        "size": file_size,
                        "name": file_name,
                    },
                },
            },
            timeout=30.0,
        )
        if response.status_code >= 400:
            self._raise_http_error(response, "upload info request")
        payload = response.json()
        signature_info = payload.get("headerSignatureInfo") or {}
        resource_urls = signature_info.get("resourceUrls") or []
        resource_url = resource_urls[0] if resource_urls else ""
        upload_key = str(payload.get("uploadKey") or "")
        if not resource_url or not upload_key:
            raise RuntimeError("DingTalk upload info response missing upload resource URL or uploadKey")
        return {
            "resource_url": resource_url,
            "headers": signature_info.get("headers") or {},
            "media_id": upload_key,
        }

    async def _put_uploaded_file(self, file_path: str, *, resource_url: str, headers: Dict[str, str]) -> None:
        if not self._http_client:
            raise RuntimeError("HTTP client not initialized")
        response = await self._http_client.put(
            resource_url,
            content=Path(file_path).read_bytes(),
            headers=headers,
            timeout=60.0,
        )
        if response.status_code >= 400:
            raise RuntimeError(f"DingTalk file upload PUT failed: HTTP {response.status_code}")

    async def _submit_uploaded_file(self, *, union_id: str, root_node_id: str, file_name: str, media_id: str, file_size: int) -> Dict[str, Any]:
        if not self._http_client:
            raise RuntimeError("HTTP client not initialized")
        access_token = await self._get_access_token()
        response = await self._http_client.post(
            f"https://api.dingtalk.com/v2.0/storage/spaces/files/{root_node_id}/commit",
            params={"unionId": union_id},
            headers={"x-acs-dingtalk-access-token": access_token},
            json={
                "uploadKey": media_id,
                "name": file_name,
                "option": {
                    "size": file_size,
                    "conflictStrategy": "OVERWRITE",
                    "convertToOnlineDoc": False,
                },
            },
            timeout=30.0,
        )
        if response.status_code >= 400:
            self._raise_http_error(response, "upload commit")
        payload = response.json()
        dentry = payload.get("dentry") or {}
        dentry_uuid = str(dentry.get("uuid") or "")
        if not dentry_uuid:
            raise RuntimeError("DingTalk upload commit response missing dentry.uuid")
        return {"dentry_uuid": dentry_uuid}

    async def _query_dentry_url(self, *, union_id: str, space_id: str, dentry_uuid: str) -> str:
        """Construct a shareable DingTalk document URL for the uploaded file."""
        return f"https://alidocs.dingtalk.com/i/nodes/{dentry_uuid}"


# ---------------------------------------------------------------------------
# Internal stream handler
# ---------------------------------------------------------------------------

class _IncomingHandler(AsyncChatbotHandler if DINGTALK_STREAM_AVAILABLE else object):
    """dingtalk-stream ChatbotHandler that forwards messages to the adapter."""

    def __init__(self, adapter: DingTalkAdapter, loop: asyncio.AbstractEventLoop):
        if DINGTALK_STREAM_AVAILABLE:
            super().__init__()
        self._adapter = adapter
        self._loop = loop

    def process(self, callback_message):
        """Called by dingtalk-stream in its thread when a message arrives.

        Schedules the async handler on the main event loop.
        """
        loop = self._loop
        if loop is None or loop.is_closed():
            logger.error("[DingTalk] Event loop unavailable, cannot dispatch message")
            return

        message = ChatbotMessage.from_dict(getattr(callback_message, 'data', {}) or {})
        future = asyncio.run_coroutine_threadsafe(self._adapter._on_message(message), loop)
        try:
            future.result(timeout=60)
        except Exception:
            logger.exception("[DingTalk] Error processing incoming message")
