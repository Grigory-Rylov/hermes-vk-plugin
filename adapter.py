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
import time
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

        # Access policy
        self.dm_policy = extra.get("dm_policy", "open")
        self.group_policy = extra.get("group_policy", "open")
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

        Uses groups.getLongPollServer which returns events in array format.
        """
        params = {"group_id": self.group_id}
        data = await self._api_request("groups.getLongPollServer", params)

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

    # ── Long Poll loop ─────────────────────────────────────────────────

    async def _poll_loop(self) -> None:
        """Main Long Poll loop — receives events from VK.

        Uses groups.getLongPollServer which returns events in
        Callback API format: {"type": "message_new", "object": {...}}
        """
        while self._running:
            if not self._server or not self._key or not self._ts:
                logger.info("Re-initializing Long Poll server...")
                if not await self._get_long_poll_server():
                    await asyncio.sleep(LONG_POLL_RECONNECT_DELAY)
                    continue

            url = f"{self._server}?act=a_check&key={self._key}&ts={self._ts}&wait={LONG_POLL_TIMEOUT}"

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

        VK's groups.getLongPollServer returns events in Callback API format:
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
            else:
                logger.debug("Ignored VK event type: %s", event_type)
        elif isinstance(update, (list, tuple)):
            # Classic array format fallback
            if len(update) < 2:
                return
            event_code = update[0]
            if event_code == 4:  # new message
                await self._process_new_message_lp(update)
            else:
                logger.debug("Ignored VK event code: %s", event_code)
        else:
            logger.debug("Ignored non-dict/non-list update: %s", update)

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

        # Skip outbox messages (bot's own)
        if msg.get("out", 0) == 1:
            return

        # Skip empty messages
        if not text and not payload:
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

        # Create MessageEvent
        event = MessageEvent(
            text=display_text,
            message_type=MessageType.TEXT,
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
        """Process a new message from classic Long Poll array format (fallback).

        [4, flags, from_id, peer_id, timestamp, text, ...]
        """
        if len(update) < 6:
            return

        flags = update[1]
        from_id = update[2]
        peer_id = update[3]
        text = update[5] if len(update) > 5 else ""

        if flags & 2:  # outbox
            return
        if not text:
            return

        user_id = str(from_id)
        chat_id = str(peer_id)
        is_chat = peer_id > 2000000000

        if is_chat:
            chat_type = "group"
        else:
            chat_type = "dm"
            user_id = str(peer_id)

        if not self._is_user_allowed(user_id, chat_type):
            return

        user_name = await self._get_user_name(user_id)
        chat_name = await self._get_chat_title(peer_id) if is_chat else user_name
        display_text = f"[{user_name}] {text}" if is_chat else text

        msg = {"id": 0, "from_id": from_id, "peer_id": peer_id, "text": text, "out": 0}

        event = MessageEvent(
            text=display_text,
            message_type=MessageType.TEXT,
            message_id=str(int(time.time() * 1000)),
            raw_message=msg,
        )
        event.source = SessionSource(
            platform=Platform.VK,
            chat_id=chat_id, user_id=user_id, user_name=user_name,
            chat_name=chat_name, chat_type=chat_type,
            message_id=str(int(time.time() * 1000)),
        )
        await self.handle_message(event)

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
        return True

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
        """Send a message to a VK chat."""
        if not content:
            return SendResult(success=True, message_id=None)

        params: Dict[str, Any] = {
            "peer_id": chat_id,
            "message": content,
            "random_id": int(time.time() * 1000) & 0x7FFFFFFF,
        }

        if reply_to:
            params["reply_to"] = reply_to

        data = await self._api_request("messages.send", params)

        if "error" in data:
            err = data["error"]
            logger.warning("Failed to send message to %s: %s", chat_id, err)
            return SendResult(
                success=False,
                error=f"VK API error: {err.get('error_msg', 'unknown')}",
            )

        msg_id = data.get("response")
        return SendResult(success=True, message_id=str(msg_id) if msg_id else None)

    async def send_typing(self, chat_id: str) -> None:
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
        seed["home_channel"] = {"chat_id": home, "name": "VK Home"}

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
            "It supports plain text messages. "
            "Use /commands for Hermes controls."
        ),
        emoji="💬",
    )
