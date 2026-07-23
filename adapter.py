"""
VK Messenger (ВКонтакте) platform adapter using Bots Long Poll API.

Connects to VK Bots Long Poll server for inbound events and uses the
VK API (api.vk.com) for outbound messages.

The Bots Long Poll API (groups.getBotsLongPollServer) returns events
in Callback API format: {"type": "message_new", "object": {...}}.

Configuration in config.yaml:
    gateway:
      platforms:
        vk:
          enabled: true
          extra:
            group_id: "239730227"          # or VK_GROUP_ID env var
            token: "vk1.a...."             # or VK_GROUP_TOKEN env var
            home_channel: "2000000001"     # chat ID for welcome/cron
            dm_policy: "open"              # open | allowlist | disabled
            allow_from: ["12345"]
            group_policy: "open"           # open | allowlist | disabled
            group_allow_from: ["-123456789"]

Reference: https://dev.vk.com/en/api/bots/longpoll
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

try:
    import aiohttp

    AIOHTTP_AVAILABLE = True
except ImportError:
    AIOHTTP_AVAILABLE = False
    aiohttp = None  # type: ignore[assignment]

from gateway.config import Platform, PlatformConfig
from gateway.platforms.base import (
    BasePlatformAdapter,
    MessageEvent,
    MessageType,
    SendResult,
)
from gateway.session import SessionSource

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

VK_API_VERSION = "5.199"
VK_API_BASE = "https://api.vk.com/method/"
LONG_POLL_TIMEOUT = 25  # seconds — VK server will hold the connection
LONG_POLL_RECONNECT_DELAY = 3  # seconds before reconnecting on error
MAX_MESSAGE_LENGTH = 4096  # VK message limit

# Retry constants for flood control
MAX_RETRIES = 3
INITIAL_RETRY_DELAY = 0.5  # seconds

# VK message flags
VK_FLAG_IS_OUTBOX = 2
VK_FLAG_IS_CHAT = 8


def _build_vk_api_url(method: str) -> str:
    """Build a VK API URL for the given method."""
    return f"{VK_API_BASE}{method}"


# ---------------------------------------------------------------------------
# Adapter
# ---------------------------------------------------------------------------

class VKAdapter(BasePlatformAdapter):
    """VK Messenger adapter using Bots Long Poll API."""

    supports_code_blocks: bool = False
    typed_command_prefix: str = "/"

    def __init__(self, config: PlatformConfig):
        super().__init__(config, Platform("vk"))

        extra = config.extra or {}

        # Auth
        self.token = os.getenv("VK_GROUP_TOKEN") or extra.get("token", "")
        self.group_id = os.getenv("VK_GROUP_ID") or extra.get("group_id", "")

        # Home channel for welcome/cron delivery
        home_channel = extra.get("home_channel", "")
        self.home_channel = str(home_channel) if home_channel else ""

        # Thinking peer — route reasoning/thinking to separate chat
        thinking_peer = extra.get("thinking_peer_id", "")
        self.thinking_peer_id = int(thinking_peer) if thinking_peer else None
        logger.info("thinking_peer_id=%s", self.thinking_peer_id)

        # Access policy
        self.dm_policy = extra.get("dm_policy", extra.get("dmPolicy", "open"))
        self.group_policy = extra.get("group_policy", extra.get("groupPolicy", "open"))
        self.allow_from: List[str] = extra.get("allow_from", [])
        self.group_allow_from: List[str] = extra.get("group_allow_from", [])

        # Env-based allowlist
        env_allowed = os.getenv("VK_ALLOWED_USERS", "").strip()
        if env_allowed:
            self.allow_from = [uid.strip() for uid in env_allowed.split(",") if uid.strip()]
        self.allow_all = os.getenv("VK_ALLOW_ALL_USERS", "").strip().lower() == "true"

        # Long Poll state
        self._server: Optional[str] = None
        self._key: Optional[str] = None
        self._ts: Optional[str] = None
        self._session: Optional[aiohttp.ClientSession] = None
        self._poll_task: Optional[asyncio.Task] = None

        # Rate limiting
        self._last_api_call: float = 0
        self._api_call_delay: float = 0.34  # ~3 calls per second

        # User name cache: user_id -> name
        self._user_cache: Dict[str, str] = {}

        # Callback query tracking
        self._pending_callbacks: Dict[str, dict] = {}

    # ── Access policy ────────────────────────────────────────────────────

    @property
    def enforces_own_access_policy(self) -> bool:
        return self.dm_policy == "allowlist" or self.group_policy == "allowlist"

    def _is_user_allowed(self, user_id: str, chat_type: str) -> bool:
        """Check if a user is allowed to interact with the bot."""
        if self.allow_all:
            return True

        if chat_type == "dm":
            if self.dm_policy == "disabled":
                return False
            if self.dm_policy == "allowlist":
                return user_id in self.allow_from
            return True  # "open"

        if chat_type == "group":
            if self.group_policy == "disabled":
                return False
            if self.group_policy == "allowlist":
                return user_id in self.group_allow_from
            return True  # "open"

        return True

    # ── VK API helpers ──────────────────────────────────────────────────

    async def _api_request_with_retry(
        self, method: str, params: Dict[str, Any], is_send: bool = False
    ) -> Dict[str, Any]:
        """Make a VK API request with flood control retry."""
        for attempt in range(MAX_RETRIES):
            data = await self._api_request(method, params)

            if "error" not in data:
                return data

            error = data["error"]
            error_code = error.get("error_code")
            error_msg = error.get("error_msg", "")

            # Check for flood control (error 9)
            is_flood = (
                error_code == 9 or
                "Flood control" in error_msg or
                "too much messages" in error_msg.lower()
            )

            if is_flood and is_send and attempt < MAX_RETRIES - 1:
                delay = INITIAL_RETRY_DELAY * (2 ** attempt)
                logger.warning(
                    "Flood control detected, retry %d/%d after %.1fs",
                    attempt + 1, MAX_RETRIES, delay,
                )
                await asyncio.sleep(delay)
                continue

            return data

        return data

    async def _api_request(
        self, method: str, params: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Make a VK API request with rate limiting."""
        if not AIOHTTP_AVAILABLE:
            logger.error("aiohttp is not installed — cannot make VK API requests")
            return {"error": {"error_code": -1, "error_msg": "aiohttp not installed"}}

        # Rate limiting
        now = time.monotonic()
        since_last = now - self._last_api_call
        if since_last < self._api_call_delay:
            await asyncio.sleep(self._api_call_delay - since_last)
        self._last_api_call = time.monotonic()

        url = _build_vk_api_url(method)
        payload = {
            "access_token": self.token,
            "v": VK_API_VERSION,
            **(params or {}),
        }

        try:
            async with self._session.post(url, data=payload, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                data = await resp.json()
        except asyncio.TimeoutError:
            logger.warning("VK API request timed out: %s", method)
            return {"error": {"error_code": -2, "error_msg": "timeout"}}
        except aiohttp.ClientError as e:
            logger.warning("VK API request failed: %s — %s", method, e)
            return {"error": {"error_code": -3, "error_msg": str(e)}}
        except Exception as e:
            logger.warning("VK API request error: %s — %s", method, e)
            return {"error": {"error_code": -4, "error_msg": str(e)}}

        if "error" in data:
            err = data["error"]
            logger.warning(
                "VK API error [%s]: code=%s msg=%s",
                method, err.get("error_code"), err.get("error_msg"),
            )

        return data

    async def _get_long_poll_server(self) -> bool:
        """Get Long Poll server details from VK API.

        Uses messages.getLongPollServer which returns events in Callback API format.
        """
        params = {"version": "2.1", "need_messages": 1}
        data = await self._api_request("messages.getLongPollServer", params)

        if "error" in data:
            logger.error("Failed to get Long Poll server: %s", data["error"])
            return False

        response = data.get("response", {})
        self._server = response.get("server")
        self._key = response.get("key")
        self._ts = response.get("ts")

        if not all([self._server, self._key, self._ts]):
            logger.error("Incomplete Long Poll server response: %s", response)
            return False

        logger.info(
            "Got VK Long Poll server: %s (ts=%s)",
            self._server, self._ts,
        )
        return True

    async def _get_user_name(self, user_id: str) -> str:
        """Get a user's first + last name from VK API, with caching."""
        if user_id in self._user_cache:
            return self._user_cache[user_id]

        params = {"user_ids": user_id, "fields": "first_name,last_name"}
        data = await self._api_request("users.get", params)

        name = f"id{user_id}"
        if "error" not in data:
            users = data.get("response", [])
            if users:
                u = users[0]
                first = u.get("first_name", "")
                last = u.get("last_name", "")
                name = f"{first} {last}".strip() or name

        self._user_cache[user_id] = name
        return name

    async def _get_chat_title(self, peer_id: int) -> str:
        """Get chat title for a multi-user conversation."""
        params = {"peer_ids": str(peer_id)}
        data = await self._api_request("messages.getConversationsById", params)

        if "error" not in data:
            items = data.get("response", {}).get("items", [])
            if items:
                chat_settings = items[0].get("chat_settings", {})
                title = chat_settings.get("title", "")
                if title:
                    return title

        return f"chat_{peer_id}"

    # ── Attachment handling ─────────────────────────────────────────────

    def _get_attachment_url(self, attachment: dict) -> Optional[Tuple[str, str]]:
        """Extract download URL and filename from a VK attachment.

        Returns (url, filename) or None.
        Supports: photo, doc, audio, audio_message, sticker.
        """
        att_type = attachment.get("type")
        att_data = attachment.get(att_type, {})

        if att_type == "photo":
            sizes = att_data.get("sizes", [])
            if not sizes:
                return None
            # Sort by size priority
            size_priority = {"s": 1, "m": 2, "x": 3, "y": 4, "z": 5, "w": 6}
            sizes_sorted = sorted(
                sizes,
                key=lambda s: size_priority.get(s.get("type", "x"), 3),
                reverse=True,
            )
            url = sizes_sorted[0].get("url")
            if not url:
                return None
            photo_id = att_data.get("id", "unknown")
            return url, f"photo_{photo_id}.jpg"

        elif att_type == "doc":
            url = att_data.get("url")
            if not url:
                return None
            filename = att_data.get("title", f"doc_{att_data.get('id', 'unknown')}")
            return url, filename

        elif att_type == "audio_message":
            url = att_data.get("link_mp3")
            ext = ".mp3"
            if not url:
                url = att_data.get("link_ogg")
                ext = ".ogg"
            if not url:
                return None
            duration = att_data.get("duration", 0)
            return url, f"voice_{duration}s{ext}"

        elif att_type == "audio":
            url = att_data.get("url")
            if not url:
                return None
            artist = att_data.get("artist", "unknown")
            title = att_data.get("title", "unknown")
            return url, f"{artist} - {title}.mp3"

        elif att_type == "sticker":
            images = att_data.get("images", [])
            if not images:
                return None
            url = images[-1].get("url")
            if not url:
                return None
            sticker_id = att_data.get("sticker_id", "unknown")
            return url, f"sticker_{sticker_id}.png"

        return None

    async def _download_attachment(
        self, url: str, save_path: Path
    ) -> bool:
        """Download a file from URL and save to path."""
        try:
            async with self._session.get(url, timeout=aiohttp.ClientTimeout(total=60)) as resp:
                if resp.status != 200:
                    logger.error("Failed to download %s: status %d", url, resp.status)
                    return False
                content = await resp.read()

            save_path.parent.mkdir(parents=True, exist_ok=True)
            save_path.write_bytes(content)
            logger.debug("Downloaded attachment to %s (%d bytes)", save_path, len(content))
            return True
        except Exception as e:
            logger.error("Error downloading attachment: %s", e)
            return False

    async def _process_attachments(
        self, attachments: List[dict], peer_id: int
    ) -> List[str]:
        """Process message attachments and return MEDIA: tags for Hermes.

        Downloads attachments to temp directory and returns list of
        'MEDIA:/path/to/file' strings to append to message text.
        """
        if not attachments:
            return []

        # Create temp directory for attachments
        tmp_dir = Path("/tmp/hermes_vk_attachments")
        tmp_dir.mkdir(exist_ok=True)

        media_tags = []
        timestamp = time.strftime("%Y%m%d_%H%M%S")

        for att in attachments:
            result = self._get_attachment_url(att)
            if not result:
                att_type = att.get("type", "unknown")
                logger.debug("No download URL for attachment type: %s", att_type)
                continue

            url, filename = result
            # Sanitize filename
            safe_name = re.sub(r'[^\w.\-]', '_', filename)
            save_name = f"{timestamp}_{safe_name}"
            save_path = tmp_dir / save_name

            success = await self._download_attachment(url, save_path)
            if success:
                att_type = att.get("type", "file")
                logger.info(
                    "Downloaded attachment [%s]: %s (%d bytes)",
                    att_type, save_path, save_path.stat().st_size,
                )
                media_tags.append(f"MEDIA:{save_path}")

        return media_tags

    # ── Long message splitting ──────────────────────────────────────────

    def _split_message(self, text: str, max_length: int = MAX_MESSAGE_LENGTH) -> List[str]:
        """Split a long message into chunks that fit VK's message limit.

        Tries to split on newlines, then spaces, to avoid breaking words.
        """
        if len(text) <= max_length:
            return [text]

        # Reserve space for chunk indicator [N/M]
        safe_length = max_length - 20
        parts = []
        lines = text.split("\n")
        current = ""

        for line in lines:
            if len(line) > safe_length:
                # Very long line — split by characters
                if current:
                    parts.append(current)
                    current = ""
                for i in range(0, len(line), safe_length):
                    parts.append(line[i:i + safe_length])
            elif len(current) + len(line) + 1 <= safe_length:
                current = (current + "\n" + line) if current else line
            else:
                if current:
                    parts.append(current)
                current = line

        if current:
            parts.append(current)

        # Add chunk indicators if split
        if len(parts) > 1:
            parts = [f"[{i+1}/{len(parts)}]\n{p}" for i, p in enumerate(parts)]

        return parts

    # ── Long Poll loop ─────────────────────────────────────────────────

    async def _poll_loop(self) -> None:
        """Main Long Poll loop — receives events from VK.

        Uses messages.getLongPollServer which returns events in
        Callback API format: {"type": "message_new", "object": {...}}
        """
        while self._running:
            if not self._server or not self._key or not self._ts:
                logger.info("Re-initializing Long Poll server...")
                if not await self._get_long_poll_server():
                    await asyncio.sleep(LONG_POLL_RECONNECT_DELAY)
                    continue

            server_url = "https://" + self._server if not self._server.startswith("http") else self._server
            url = f"{server_url}?act=a_check&key={self._key}&ts={self._ts}&wait={LONG_POLL_TIMEOUT}"

            try:
                async with self._session.get(
                    url, timeout=aiohttp.ClientTimeout(total=LONG_POLL_TIMEOUT + 10)
                ) as resp:
                    data = await resp.json()
            except asyncio.TimeoutError:
                # Normal timeout — just retry
                continue
            except aiohttp.ClientError as e:
                logger.warning("Long Poll connection error: %s", e)
                await asyncio.sleep(LONG_POLL_RECONNECT_DELAY)
                continue
            except Exception as e:
                logger.warning("Long Poll unexpected error: %s", e)
                await asyncio.sleep(LONG_POLL_RECONNECT_DELAY)
                continue

            # Check for VK-level error (e.g. ts expired)
            if "error" in data:
                err = data["error"]
                logger.warning("Long Poll server error: %s", err)
                self._server = None
                self._key = None
                self._ts = None
                await asyncio.sleep(LONG_POLL_RECONNECT_DELAY)
                continue

            # "failed" codes from Long Poll server
            if "failed" in data:
                failed_code = data["failed"]
                if failed_code in (2, 3):
                    # Need to reinitialize server
                    logger.info("Long Poll server needs reinit (failed=%d)", failed_code)
                    self._server = None
                    self._key = None
                    self._ts = None
                await asyncio.sleep(LONG_POLL_RECONNECT_DELAY)
                continue

            # Update ts
            new_ts = data.get("ts")
            if new_ts:
                self._ts = new_ts

            # Process updates
            updates = data.get("updates", [])
            if updates:
                logger.info("Received %d VK Long Poll updates", len(updates))
            for update in updates:
                await self._process_update(update)

    async def _process_update(self, update) -> None:
        """Process a single Long Poll update.

        VK's messages.getLongPollServer returns events in Callback API format:
          {"type": "message_new", "object": {"message": {...}}, "group_id": ...}

        But it can also return classic array format for some event types:
          [event_code, ...args]
        """
        if isinstance(update, dict):
            event_type = update.get("type")
            logger.info("Received VK event: %s", event_type)
            event_object = update.get("object", {})

            if event_type == "message_new":
                await self._process_new_message(event_object)
            elif event_type == "message_edit":
                pass  # Could handle if needed
            elif event_type == "message_typing_state":
                pass  # Typing indicator — ignore
            elif event_type == "message_reaction_event":
                pass  # Reactions — ignore
            elif event_type == "text_message_new":
                pass  # Duplicate of message_new in some modes
            else:
                logger.debug("Ignored VK event type: %s", event_type)
        elif isinstance(update, (list, tuple)):
            # Classic array format fallback
            if len(update) < 2:
                return
            event_code = update[0]

            if event_code == 4:  # new message
                await self._process_new_message_lp(update)
            elif event_code == 85:  # message event (keyboard_callback, etc)
                await self._process_message_event(update)
            elif event_code == 126:  # keyboard_callback
                await self._process_keyboard_callback(update)
            else:
                logger.debug("Ignored VK event code: %s", event_code)
        else:
            logger.debug("Ignored non-dict/non-list update: %s", update)

    async def _process_keyboard_callback(self, update: list) -> None:
        """Process keyboard callback from Long Poll array format.

        Format: [126, peer_id, user_id, random_id, message_id, payload_json]
        """
        if len(update) < 6:
            logger.warning("Keyboard callback too short: %d fields", len(update))
            return

        peer_id = update[1]
        user_id = update[2]
        message_id = update[4]
        payload_str = update[5]

        logger.info("Keyboard callback: peer=%d user=%d msg=%d payload=%s",
                     peer_id, user_id, message_id, payload_str[:100])

        # Parse payload
        try:
            payload = json.loads(payload_str) if isinstance(payload_str, str) else payload_str
        except (json.JSONDecodeError, TypeError):
            payload = {"text": payload_str}

        # Answer callback query (acknowledge to VK)
        await self._api_request("messages.answerCallbackQuery", {
            "peer_id": peer_id,
            "message_id": message_id,
            "code": 0,  # Success
        })

        # Get button text
        action = payload.get("action", payload)
        button_text = action.get("label", "") if isinstance(action, dict) else ""
        callback_data = action.get("payload", "") if isinstance(action, dict) else ""

        if not button_text and not callback_data:
            button_text = payload.get("text", "")

        # Build text from callback
        text = button_text or callback_data or str(payload)

        if not text:
            return

        # Determine chat type
        is_chat = peer_id > 2000000000
        chat_type = "group" if is_chat else "dm"
        chat_user_id = str(peer_id) if is_chat else str(user_id)

        if not self._is_user_allowed(chat_user_id, chat_type):
            return

        user_name = await self._get_user_name(str(user_id))
        chat_name = await self._get_chat_title(peer_id) if is_chat else user_name

        event = MessageEvent(
            text=text,
            message_type=MessageType.TEXT,
            message_id=str(message_id),
            raw_message={"peer_id": peer_id, "from_id": user_id, "id": message_id},
        )
        event.source = SessionSource(
            platform=Platform.VK,
            chat_id=str(peer_id),
            user_id=str(user_id),
            user_name=user_name,
            chat_name=chat_name,
            chat_type=chat_type,
            message_id=str(message_id),
        )
        await self.handle_message(event)

    async def _process_message_event(self, update: list) -> None:
        """Process message event (code 85) — keyboard callbacks, etc.

        Format: [85, user_id, flags, peer_id, date, json_data]
        """
        if len(update) < 6:
            return

        user_id = update[1]
        peer_id = update[3]
        json_str = update[5]

        try:
            data = json.loads(json_str) if isinstance(json_str, str) else json_str
        except (json.JSONDecodeError, TypeError):
            return

        action = data.get("action", {})
        action_type = action.get("type", "")
        message_id = data.get("message", {}).get("id", 0)

        if action_type == "keyboard_callback":
            payload = action.get("payload", "")
            logger.info("Message event callback: peer=%d user=%d payload=%s",
                        peer_id, user_id, str(payload)[:100])

            # Answer callback
            await self._api_request("messages.answerCallbackQuery", {
                "peer_id": peer_id,
                "message_id": message_id,
                "code": 0,
            })

            # Parse payload for button text
            try:
                if isinstance(payload, str):
                    payload_data = json.loads(payload)
                else:
                    payload_data = payload
            except (json.JSONDecodeError, TypeError):
                payload_data = payload

            # Extract text from payload
            button_text = ""
            if isinstance(payload_data, dict):
                action_data = payload_data.get("action", payload_data)
                if isinstance(action_data, dict):
                    button_text = action_data.get("label", "")
                if not button_text:
                    button_text = payload_data.get("text", "")

            if not button_text:
                return

            is_chat = peer_id > 2000000000
            chat_type = "group" if is_chat else "dm"
            chat_user_id = str(peer_id) if is_chat else str(user_id)

            if not self._is_user_allowed(chat_user_id, chat_type):
                return

            user_name = await self._get_user_name(str(user_id))
            chat_name = await self._get_chat_title(peer_id) if is_chat else user_name

            event = MessageEvent(
                text=button_text,
                message_type=MessageType.TEXT,
                message_id=str(message_id),
                raw_message={"peer_id": peer_id, "from_id": user_id, "id": message_id},
            )
            event.source = SessionSource(
                platform=Platform.VK,
                chat_id=str(peer_id),
                user_id=str(user_id),
                user_name=user_name,
                chat_name=chat_name,
                chat_type=chat_type,
                message_id=str(message_id),
            )
            await self.handle_message(event)

        elif action_type == "photo_change":
            pass  # Ignore photo change events

    async def _process_new_message(self, event_object: dict) -> None:
        """Process a new message event from Callback API format.

        The event object contains a "message" key with the message data:
          {"message": {"id": ..., "from_id": ..., "peer_id": ..., "text": ..., ...}}
        """
        msg = event_object.get("message", event_object)

        # Extract message data
        text = msg.get("text", "").strip()
        from_id = msg.get("from_id", 0)
        peer_id = msg.get("peer_id", 0)
        message_id = msg.get("id", 0)
        payload = msg.get("payload", "")
        attachments = msg.get("attachments", [])

        # Skip outbox messages (bot's own)
        if msg.get("out", 0) == 1:
            return

        # Skip messages from the group itself (bot's own posts: from_id is negative)
        if from_id < 0:
            return

        # Skip empty messages (but allow if there are attachments)
        if not text and not payload and not attachments:
            return

        # Determine chat type
        user_id = str(from_id)
        chat_id = str(peer_id)
        is_chat = peer_id > 2000000000

        if is_chat:
            chat_type = "group"
        else:
            chat_type = "dm"
            user_id = str(peer_id)

        # Access control
        if not self._is_user_allowed(user_id, chat_type):
            logger.info("User %s not allowed (chat_type=%s)", user_id, chat_type)
            return

        # Get user name for display
        user_name = await self._get_user_name(user_id)

        # Build chat name
        if is_chat:
            chat_name = await self._get_chat_title(peer_id)
        else:
            chat_name = user_name

        # In group chats, prefix with the user's name
        if is_chat:
            display_text = f"[{user_name}] {text}" if text else text
        else:
            display_text = text

        # Handle payload (callback buttons)
        if payload and not text:
            try:
                payload_data = json.loads(payload)
                button_text = payload_data.get("text", "")
                if button_text:
                    display_text = button_text
            except (json.JSONDecodeError, TypeError):
                pass

        # Process attachments
        media_tags = []
        if attachments:
            logger.info("Processing %d attachments for message %d", len(attachments), message_id)
            media_tags = await self._process_attachments(attachments, peer_id)
            if media_tags:
                logger.info("Found %d downloadable attachments", len(media_tags))

        # Combine text with attachment info
        if media_tags:
            attachment_info = "\n".join(media_tags)
            if display_text:
                display_text = f"{display_text}\n\n{attachment_info}"
            else:
                display_text = attachment_info

        # Determine message type
        msg_type = MessageType.TEXT
        if attachments:
            # Check for voice message
            for att in attachments:
                if att.get("type") == "audio_message":
                    msg_type = MessageType.VOICE
                    break

        # Create MessageEvent
        event = MessageEvent(
            text=display_text,
            message_type=msg_type,
            message_id=str(message_id),
            raw_message=msg,
        )

        # Build source info
        event.source = SessionSource(
            platform=Platform.VK,
            chat_id=chat_id,
            user_id=user_id,
            user_name=user_name,
            chat_name=chat_name,
            chat_type=chat_type,
            message_id=str(message_id),
        )

        # Forward to Hermes
        await self.handle_message(event)

    async def _process_new_message_lp(self, update: list) -> None:
        """Process a new message from Bot Long Poll v2.1 array format.

        Format: [4, id, flags, peer_id, date, label, text]
        """
        logger.info("LP: msg_id=%s flags=%s peer=%s text=%s", update[1], update[2], update[3], str(update[6])[:80])
        if len(update) < 7:
            logger.warning("LP: too short, need 7 got %d", len(update))
            return

        msg_id = update[1]
        flags = update[2]
        peer_id = update[3]
        timestamp = update[4]
        label = update[5]
        text = update[6]

        # Skip bot's own messages (outbox flag)
        if flags & VK_FLAG_IS_OUTBOX:
            logger.debug("LP: skipping bot's own message (outbox) msg_id=%d", msg_id)
            return

        if not text:
            return

        user_id = str(peer_id)
        chat_id = str(peer_id)
        is_chat = peer_id > 2000000000

        if is_chat:
            chat_type = "group"
        else:
            chat_type = "dm"

        if not self._is_user_allowed(user_id, chat_type):
            return

        chat_name = await self._get_chat_title(peer_id) if is_chat else f"user:{user_id}"
        user_name = chat_name

        msg = {"id": msg_id, "from_id": 0, "peer_id": peer_id, "text": text, "out": 0}

        event = MessageEvent(
            text=text,
            message_type=MessageType.TEXT,
            message_id=str(msg_id),
            raw_message=msg,
        )
        event.source = SessionSource(
            platform=Platform.VK,
            chat_id=chat_id, user_id=user_id, user_name=user_name,
            chat_name=chat_name, chat_type=chat_type,
            message_id=str(msg_id),
        )
        logger.info("LP: dispatching to Hermes: peer=%s user=%s text=%s", peer_id, user_id, text[:80])
        await self.handle_message(event)
        logger.info("LP: handle_message completed")

    # ── BasePlatformAdapter interface

    # ── BasePlatformAdapter interface ───────────────────────────────────

    async def connect(self, is_reconnect: bool = False) -> bool:
        """Connect to VK Bots Long Poll and start receiving messages."""
        if not AIOHTTP_AVAILABLE:
            logger.error(
                "aiohttp is required for VK adapter. "
                "Install: pip install aiohttp"
            )
            return False

        if not self.token:
            logger.error(
                "VK_GROUP_TOKEN is not set. "
                "Set it in .env or config.yaml (gateway.platforms.vk.extra.token)"
            )
            return False

        if not self.group_id:
            logger.error(
                "VK_GROUP_ID is not set. "
                "Set it in .env or config.yaml (gateway.platforms.vk.extra.group_id)"
            )
            return False

        # Create HTTP session with IPv4 only (VK doesn't respond over IPv6 from WSL)
        import socket
        connector = aiohttp.TCPConnector(family=socket.AF_INET)
        self._session = aiohttp.ClientSession(connector=connector)

        # Get Long Poll server
        if not await self._get_long_poll_server():
            await self._session.close()
            self._session = None
            return False

        self._mark_connected()

        # Start polling in background
        self._poll_task = asyncio.create_task(self._poll_loop())

        logger.info(
            "VK adapter connected (group_id=%s, dm_policy=%s, group_policy=%s)",
            self.group_id, self.dm_policy, self.group_policy,
        )

        # Send welcome message on startup
        asyncio.create_task(self._send_welcome())
        return True

    async def _send_welcome(self) -> None:
        """Send welcome message on startup to home_channel."""
        # Determine target peer_id
        target = self.home_channel or "2000000001"

        try:
            data = await self._api_request_with_retry("messages.send", {
                "peer_id": int(target),
                "message": "🟢 Hermes запущен",
                "random_id": int(time.time() * 1000) & 0x7FFFFFFF,
            }, is_send=True)
            if "error" not in data:
                logger.info("Welcome '🟢 Hermes запущен' sent to %s", target)
            else:
                logger.error("Welcome failed: %s", data.get("error"))
        except Exception as e:
            logger.error("Welcome exception: %s", e)

    async def disconnect(self) -> None:
        """Disconnect from VK Bots Long Poll."""
        self._running = False

        if self._poll_task:
            self._poll_task.cancel()
            try:
                await self._poll_task
            except asyncio.CancelledError:
                pass
            self._poll_task = None

        if self._session:
            await self._session.close()
            self._session = None

        self._mark_disconnected()
        logger.info("VK adapter disconnected")

    async def send(
        self,
        chat_id: str,
        content: str,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        """Send a message to a VK chat.

        Automatically splits long messages and handles flood control.
        Routes reasoning/thinking and status messages to thinking_peer_id if configured.
        """
        if not content:
            return SendResult(success=True, message_id=None)

        # 1. Status/streaming messages go to thinking chat
        if self._is_status_message(content):
            if self.thinking_peer_id:
                try:
                    status_chunks = self._split_message(content, MAX_MESSAGE_LENGTH)
                    for chunk in status_chunks:
                        await self._api_request_with_retry("messages.send", {
                            "peer_id": self.thinking_peer_id,
                            "message": chunk,
                            "random_id": int(time.time() * 1000) & 0x7FFFFFFF,
                        }, is_send=True)
                        await asyncio.sleep(0.3)
                    logger.debug("Status message routed to thinking_peer %s", self.thinking_peer_id)
                except Exception as e:
                    logger.warning("Failed to send status to thinking_peer %s: %s", self.thinking_peer_id, e)
            return SendResult(success=True, message_id=None)

        # 2. Extract reasoning/thinking from content
        reasoning_text, response_text = self._extract_reasoning(content)

        # Send reasoning to thinking peer if configured
        if reasoning_text and self.thinking_peer_id:
            try:
                reasoning_chunks = self._split_message(
                    f"🧠 Reasoning:\n```\n{reasoning_text}\n```",
                    MAX_MESSAGE_LENGTH,
                )
                for chunk in reasoning_chunks:
                    await self._api_request_with_retry("messages.send", {
                        "peer_id": self.thinking_peer_id,
                        "message": chunk,
                        "random_id": int(time.time() * 1000) & 0x7FFFFFFF,
                    }, is_send=True)
                    await asyncio.sleep(0.3)
                logger.debug("Reasoning routed to %s (%d chars)", self.thinking_peer_id, len(reasoning_text))
            except Exception as e:
                logger.warning("Failed to send reasoning to %s: %s", self.thinking_peer_id, e)

        # Send response to original chat
        display_content = response_text if response_text else content
        chunks = self._split_message(display_content, MAX_MESSAGE_LENGTH)

        last_msg_id = None
        for i, chunk in enumerate(chunks):
            params: Dict[str, Any] = {
                "peer_id": chat_id,
                "message": chunk,
                "random_id": int(time.time() * 1000) & 0x7FFFFFFF,
            }

            if reply_to and i == 0:
                params["reply_to"] = reply_to

            data = await self._api_request_with_retry("messages.send", params, is_send=True)

            if "error" in data:
                err = data["error"]
                logger.warning("Failed to send message chunk %d/%d to %s: %s",
                             i + 1, len(chunks), chat_id, err)
                return SendResult(
                    success=False,
                    error=f"VK API error: {err.get('error_msg', 'unknown')}",
                )

            msg_id = data.get("response")
            last_msg_id = str(msg_id) if msg_id else None

            # Small delay between chunks
            if i < len(chunks) - 1:
                await asyncio.sleep(0.3)

        return SendResult(success=True, message_id=last_msg_id)

    def _is_status_message(self, content: str) -> bool:
        """Check if a message is a Hermes status/streaming message.

        Status messages from Hermes look like:
          ⏳ Working — 54 min — iteration 14/90, waiting for provider response (streaming)
          ⏳ Processing — 12 min — iteration 3/90...

        These are short intermediate status updates (typically <120 chars), not
        final responses. A normal assistant response that happens to mention
        "iteration" or "streaming" should NOT be caught.

        Only the ⏳ prefix pattern is used as the primary trigger — everything
        else is a secondary check only if ⏳ is present.
        """
        # Must start with ⏳ and be short (status line, not a full response)
        if re.match(r'⏳\s+(Working|Processing|Running|Thinking)', content):
            # Secondary: must be a relatively short message (status line)
            if len(content) < 300:
                return True
        return False

    def _extract_reasoning(self, content: str) -> Tuple[str, str]:
        """Extract reasoning/thinking text from Hermes response.

        Hermes prepends reasoning in these formats:
        - Code block: 💭 **Reasoning:**\\n```\\n...\\n```\\n\\n{response}
        - Subtext: -# 💭 Reasoning\\n...\\n\\n{response}
        - Blockquote: > 💭 **Reasoning:**\\n...\\n\\n{response}

        Returns (reasoning_text, response_text). If no reasoning found,
        returns ("", content).
        """
        # Code block style
        m = re.search(
            r"💭\s*\*\*Reasoning:\*\*\s*\n```\n(.+?)\n```\n\n(.+)",
            content,
            re.DOTALL,
        )
        if m:
            return m.group(1).strip(), m.group(2).strip()

        # Subtext style
        m = re.search(
            r"-#\s*💭\s*Reasoning\n((?:-#\s?.*\n?)+)\n\n(.+)",
            content,
            re.DOTALL,
        )
        if m:
            reasoning = re.sub(r"^-#\s?", "", m.group(1), flags=re.MULTILINE).strip()
            return reasoning, m.group(2).strip()

        # Blockquote style
        m = re.search(
            r">\s*💭\s*\*\*Reasoning:\*\*\n((?:>\s?.*\n?)+)\n\n(.+)",
            content,
            re.DOTALL,
        )
        if m:
            reasoning = re.sub(r"^>\s?", "", m.group(1), flags=re.MULTILINE).strip()
            return reasoning, m.group(2).strip()

        return "", content

    async def send_typing(self, chat_id: str, metadata=None, **kwargs) -> None:
        """Show typing indicator in a VK chat."""
        params = {
            "peer_id": chat_id,
            "type": "typing",
        }
        await self._api_request("messages.setActivity", params)

    async def get_chat_info(self, chat_id: str) -> Dict[str, Any]:
        """Get information about a VK chat."""
        peer_id = int(chat_id)

        if peer_id > 2000000000:
            # Group chat
            title = await self._get_chat_title(peer_id)
            return {"name": title, "type": "group"}
        else:
            # DM — try to get user name
            name = await self._get_user_name(chat_id)
            return {"name": name, "type": "dm"}

    def format_message(self, content: str) -> str:
        """Format message for VK. VK supports basic text only."""
        return content


# ── Plugin entry point ─────────────────────────────────────────────────


def check_requirements() -> bool:
    """Check if VK adapter requirements are met."""
    if not AIOHTTP_AVAILABLE:
        return False
    return bool(os.getenv("VK_GROUP_TOKEN"))


def validate_config(config) -> bool:
    """Validate VK adapter configuration."""
    extra = getattr(config, "extra", {}) or {}
    token = os.getenv("VK_GROUP_TOKEN") or extra.get("token", "")
    group_id = os.getenv("VK_GROUP_ID") or extra.get("group_id", "")
    return bool(token) and bool(group_id)


def _env_enablement() -> Optional[Dict[str, Any]]:
    """Auto-enable from environment variables."""
    token = os.getenv("VK_GROUP_TOKEN", "").strip()
    group_id = os.getenv("VK_GROUP_ID", "").strip()
    if not (token and group_id):
        return None

    seed: Dict[str, Any] = {"token": token, "group_id": group_id}

    # Home channel for cron delivery
    home = os.getenv("VK_HOME_CHANNEL", "").strip()
    if home:
        seed["home_channel"] = home
        seed["home_channel_obj"] = {"chat_id": home, "name": "VK Home"}

    return seed


def register(ctx):
    """Plugin entry point — called by the Hermes plugin system."""
    ctx.register_platform(
        name="vk",
        label="VK Messenger",
        adapter_factory=lambda cfg: VKAdapter(cfg),
        check_fn=check_requirements,
        validate_config=validate_config,
        required_env=["VK_GROUP_TOKEN"],
        install_hint="pip install aiohttp",
        env_enablement_fn=_env_enablement,
        cron_deliver_env_var="VK_HOME_CHANNEL",
        allowed_users_env="VK_ALLOWED_USERS",
        allow_all_env="VK_ALLOW_ALL_USERS",
        max_message_length=MAX_MESSAGE_LENGTH,
        platform_hint=(
            "You are chatting via VK Messenger (ВКонтакте). "
            "It supports plain text messages and attachments. "
            "Use /commands for Hermes controls."
        ),
        emoji="💬",
    )
