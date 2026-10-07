"""
VK Messenger platform adapter for Hermes Agent.

Uses VK Community Messages API (Long Poll) to receive and send messages.
https://dev.vk.com/en/api/community-messages/getting-started

Requires:
- VK_GROUP_TOKEN: community access token with messages permission
- httpx (for async HTTP requests)
"""

import asyncio
import json
import logging
import os
import re
import sys
import time
from typing import Any, Dict, List, Optional, Tuple

from gateway.config import Platform, PlatformConfig
from gateway.platforms.base import (
    BasePlatformAdapter,
    MessageEvent,
    MessageType,
    SendResult,
)

from .markdown_vk import markdown_format_data, extract_keyboard_marker, extract_carousel_marker
from .keyboard import (
    VKKeyboard,
    build_remove_keyboard,
    make_callback_payload,
    parse_callback_payload,
)
from .policy import (
    check_dm_policy,
    check_group_policy,
    get_pairing_message,
    is_awaiting_pairing,
    issue_pairing_challenge,
    validate_pairing_code,
)
from .media import download_vk_attachments, download_vk_image_by_url
from .ratelimit import get_rate_limiters

logger = logging.getLogger(__name__)

# VK API constants
VK_API_VERSION = "5.199"
VK_API_BASE = "https://api.vk.com/method"
LONGPOLL_TIMEOUT = 25  # Long poll wait time in seconds
MAX_MESSAGE_LENGTH = 9000  # VK message length limit in characters (API: Макс. длина = 9000)
POLL_RECONNECT_DELAY = 3  # Delay before reconnecting on poll error (seconds)
MAX_POLL_FAILURES = 5  # Max consecutive poll failures before re-initializing
MAX_INIT_RETRIES = 10  # Max retries with backoff before long-recovery mode
INIT_RETRY_BASE = 5    # Base delay for exponential backoff (seconds)
INIT_RETRY_MAX = 120   # Max delay between init retries (seconds)
LONG_RECOVERY_DELAY = 300  # Delay in long-recovery mode (5 min)


class VKAdapter(BasePlatformAdapter):
    """Adapter for VK Messenger / VKontakte Communities.

    Supports single-account (legacy) and multi-account configurations.

    Single account (backward compatible):
        VK_GROUP_TOKEN в .env

    Multi-account:
        VK_GROUP_TOKEN_DEFAULT, VK_GROUP_TOKEN_SALES и т.д. в .env
    """

    def __init__(self, config: PlatformConfig):
        super().__init__(config, Platform("vk"))
        extra = config.extra or {}

        self._api_version = extra.get("api_version", VK_API_VERSION)
        self._upload_url: Optional[str] = extra.get("upload_url")

        # --- Multi-account configuration ---
        self._accounts: Dict[str, Dict[str, Any]] = {}
        accounts_cfg = extra.get("accounts", {})

        if accounts_cfg:
            # Multi-account mode
            for acct_id, acct_cfg in accounts_cfg.items():
                if not isinstance(acct_cfg, dict):
                    continue
                token = os.getenv(f"VK_GROUP_TOKEN_{acct_id.upper()}", "")
                token_file = acct_cfg.get("tokenFile", "")
                if token_file and not token:
                    try:
                        with open(token_file, "r") as f:
                            token = f.read().strip()
                    except (FileNotFoundError, PermissionError) as e:
                        logger.warning("[VK] tokenFile %s for account %s: %s", token_file, acct_id, e)
                if not token:
                    logger.warning("[VK] Account '%s' has no token, skipping", acct_id)
                    continue
                self._accounts[acct_id] = {
                    "token": token,
                    "dm_policy": acct_cfg.get("dmPolicy", "open"),
                    "allow_from": acct_cfg.get("allowFrom", []),
                    "group_policy": acct_cfg.get("groupPolicy", "open"),
                    "group_allow_from": acct_cfg.get("groupAllowFrom", []),
                    "require_mention": acct_cfg.get("requireMention", False),
                    "bot_name": acct_cfg.get("botName", ""),
                }
        else:
            # Single-account (legacy) mode
            token = os.getenv("VK_GROUP_TOKEN")
            token_file = extra.get("tokenFile", "")
            if token_file and not token:
                try:
                    with open(token_file, "r") as f:
                        token = f.read().strip()
                except (FileNotFoundError, PermissionError) as e:
                    logger.warning("[VK] tokenFile %s not readable: %s", token_file, e)

            if token:
                # Legacy compat: VK_ALLOWED_USERS → allowlist
                allow_from = extra.get("allowFrom", [])
                if not allow_from:
                    allowed_env = os.getenv("VK_ALLOWED_USERS", "")
                    if allowed_env:
                        allow_from = [u.strip() for u in allowed_env.split(",") if u.strip()]
                dm_policy = extra.get("dmPolicy", "open")
                if os.getenv("VK_ALLOW_ALL_USERS", "").lower() in ("true", "1", "yes"):
                    dm_policy = "open"

                self._accounts["default"] = {
                    "token": token,
                    "dm_policy": dm_policy,
                    "allow_from": allow_from,
                    "group_policy": extra.get("groupPolicy", "open"),
                    "group_allow_from": extra.get("groupAllowFrom", []),
                    "require_mention": extra.get("requireMention", False),
                    "bot_name": extra.get("botName", ""),
                }

        # --- Backward-compatible flat accessors (for single account) ---
        default_acct = self._accounts.get("default", {})
        self._token = default_acct.get("token", "")
        self._dm_policy = default_acct.get("dm_policy", "open")
        self._allow_from = default_acct.get("allow_from", [])
        self._group_policy = default_acct.get("group_policy", "open")
        self._group_allow_from = default_acct.get("group_allow_from", [])
        self._require_mention = default_acct.get("require_mention", False)
        self._bot_name = default_acct.get("bot_name", "")

        # --- Per-account Long Poll state ---
        # Keyed by account_id; each entry has: server, key, ts, group_id, poll_task, poll_failures
        self._account_states: Dict[str, Dict[str, Any]] = {}

        # Backward-compat aliases for single-account mode
        self._server: Optional[str] = None
        self._key: Optional[str] = None
        self._ts: Optional[int] = None
        self._poll_task: Optional[asyncio.Task] = None
        self._poll_failures = 0
        self._group_id: Optional[int] = None

        # Watchdog task — restarts the poll loop if it dies
        self._watchdog_task: Optional[asyncio.Task] = None
        self._connected: bool = False
        self._running: bool = False  # set to True by connect()

        # Recursion guard: track recently sent message IDs to prevent
        # processing our own responses echoed back by Long Poll.
        self._recent_outgoing_ids: set = set()
        self._recent_outgoing_lock = asyncio.Lock()

        # Rate limiters for different operation types
        self._limiters = get_rate_limiters()

        # Typing indicator throttling: track last send time per chat
        # to avoid hitting VK flood control (error 9).
        self._last_typing_time: Dict[str, float] = {}
        # Minimum interval between typing indicator sends (seconds).
        # VK starts returning error 9 "Flood control" when setActivity
        # is called more than ~once per 5-6 seconds per peer.
        # при 5сек. индфитор успевает исчезнуть. Флудконтроль срабатывает при нескольких отправках в секунду
        self._typing_cooldown: float = 3.0

        self._http_client: Optional["httpx.AsyncClient"] = None

        # --- Progress/reasoning lane routing ---
        # Tool-progress bubbles ("💻 Running …"), "⏳ Working" heartbeats, busy
        # acks and interim commentary land in a separate chat so the main
        # conversation keeps only real replies. Empty string disables routing.
        self._progress_chat_id = str(
            extra.get("progressChatId") or os.getenv("VK_PROGRESS_CHAT_ID", "") or ""
        ).strip()
        # Ids of messages redirected to the progress chat: the gateway keeps
        # editing/deleting them with the ORIGINAL chat id, so peer_id must be
        # overridden from this set for messages.edit / messages.delete.
        self._progress_msg_ids: set = set()

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def connect(self, *, is_reconnect: bool = False) -> bool:
        try:
            import httpx
        except ImportError:
            logger.error("[VK] httpx is required. Install with: pip install httpx")
            self._set_fatal_error(
                "missing_dep", "httpx is required for VK adapter", retryable=True
            )
            return False

        self._http_client = httpx.AsyncClient(timeout=30.0)

        if len(self._accounts) > 1:
            # Multi-account mode: init Long Poll for each account
            for acct_id in self._accounts:
                ok = await self._init_longpoll_for_account(acct_id)
                if not ok:
                    logger.warning("[VK] Failed to init account %s", acct_id)
            if not self._account_states:
                await self._http_client.aclose()
                self._http_client = None
                return False
        else:
            # Single-account mode
            ok = await self._init_longpoll()
            if not ok:
                await self._http_client.aclose()
                self._http_client = None
                return False

        # Start polling loop(s) and watchdog
        self._running = True
        if len(self._accounts) > 1:
            for acct_id in self._account_states:
                state = self._account_states[acct_id]
                state["poll_task"] = asyncio.create_task(
                    self._poll_loop_for_account(acct_id)
                )
        else:
            self._poll_task = asyncio.create_task(self._poll_loop())

        self._watchdog_task = asyncio.create_task(self._poll_watchdog())
        self._mark_connected()

        active_ids = list(self._account_states.keys()) or [str(self._group_id)]
        logger.info(
            "[VK] Connected (accounts=%s)",
            ", ".join(str(a) for a in active_ids),
        )

        # Send startup notification to VK home channel
        vk_home = os.getenv("VK_HOME_CHANNEL")
        if vk_home:
            try:
                await self.send(
                    chat_id=vk_home,
                    content="✅ VK бот снова на связи после перезапуска.",
                )
            except Exception:
                pass

        return True

    async def disconnect(self) -> None:
        self._running = False

        # Cancel watchdog first
        if self._watchdog_task and not self._watchdog_task.done():
            self._watchdog_task.cancel()
            try:
                await self._watchdog_task
            except asyncio.CancelledError:
                pass
        self._watchdog_task = None

        # Cancel all poll tasks (single + multi-account)
        if self._poll_task and not self._poll_task.done():
            self._poll_task.cancel()
            try:
                await self._poll_task
            except asyncio.CancelledError:
                pass
        self._poll_task = None

        # Cancel multi-account poll tasks
        for acct_id, state in list(self._account_states.items()):
            task = state.get("poll_task")
            if task and not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
        self._account_states.clear()

        if self._http_client:
            await self._http_client.aclose()
            self._http_client = None

        self._mark_disconnected()
        logger.info("[VK] Disconnected")

    async def _poll_watchdog(self) -> None:
        """Watchdog that restarts the poll loop if it dies unexpectedly.

        Runs as a background task alongside _poll_loop.
        If _poll_task finishes (completed, cancelled, or exception),
        and the adapter is still running, it re-creates the poll loop.

        Also sends a periodic heartbeat every 2 hours to confirm VK is alive.
        """
        heartbeat_interval = 7200  # 2 hours
        last_heartbeat = time.monotonic()
        last_watchdog_notify = ""  # dedup repeated watchdog restarts

        while self._running:
            now = time.monotonic()

            # --- Periodic heartbeat ---
            if self._connected and (now - last_heartbeat) >= heartbeat_interval:
                last_heartbeat = now
                await self._notify_user(
                    "info", "Watchdog: сердцебиение",
                    "VK Long Poll работает штатно."
                )

            try:
                if self._poll_task and self._poll_task.done():
                    if not self._running:
                        break

                    exc = self._poll_task.exception()
                    if exc:
                        reason = f"Исключение: {exc}"
                        logger.error(
                            "[VK] Poll loop died with exception: %s", exc
                        )
                    else:
                        reason = "Останов без исключения"
                        logger.warning("[VK] Poll loop stopped unexpectedly, restarting...")

                    # Notify once per unique reason to avoid spam
                    notify_key = f"restart_{reason}"
                    if notify_key != last_watchdog_notify:
                        last_watchdog_notify = notify_key
                        await self._notify_user(
                            "error", "Watchdog: перезапуск poll loop",
                            f"Long Poll упал. Причина: {reason}. "
                            "Пытаюсь восстановить соединение."
                        )

                    await self._init_longpoll()
                    self._poll_task = asyncio.create_task(self._poll_loop())
                    logger.info("[VK] Poll loop restarted by watchdog")

                await asyncio.sleep(30)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("[VK] Watchdog error: %s", e)
                await asyncio.sleep(30)

    # ------------------------------------------------------------------
    # Formatting — native format_data (no Unicode hacks)
    # ------------------------------------------------------------------
    # VK API messages.send accepts format_data — a JSON structure with
    # formatted spans (bold, italic, underline, url) keyed by UTF-16 offsets.
    # This is the NATIVE VK formatting mechanism.
    #
    # DEPRECATED (kept for reference):
    #   _UNICODE_* maps and _apply_unicode_styling()
    # These were used before format_data support was discovered and may be
    # removed in a future version after stabilization.

    # Legacy Unicode maps (DEPRECATED — not used in active code path)
    _UNICODE_BOLD: dict[str, str] = {
        **{chr(i): chr(0x1D400 + i - ord('A')) for i in range(ord('A'), ord('Z') + 1)},
        **{chr(i): chr(0x1D41A + i - ord('a')) for i in range(ord('a'), ord('z') + 1)},
        **{str(i): chr(0x1D7CE + i) for i in range(10)},
    }
    _UNICODE_ITALIC: dict[str, str] = {
        **{chr(i): chr(0x1D434 + i - ord('A')) for i in range(ord('A'), ord('Z') + 1)},
        **{chr(i): chr(0x1D44E + i - ord('a')) for i in range(ord('a'), ord('z') + 1)},
    }
    _UNICODE_BOLD_ITALIC: dict[str, str] = {
        **{chr(i): chr(0x1D468 + i - ord('A')) for i in range(ord('A'), ord('Z') + 1)},
        **{chr(i): chr(0x1D482 + i - ord('a')) for i in range(ord('a'), ord('z') + 1)},
        **{str(i): chr(0x1D7CE + i) for i in range(10)},
    }

    @staticmethod
    def _apply_unicode_styling(plain_text: str, format_items: list[dict]) -> str:
        """DEPRECATED: Apply Unicode styling to plain text.

        Replaced by native format_data parameter passing.
        Kept for backward compatibility during migration.
        """
        if not format_items:
            return plain_text

        items = sorted(format_items, key=lambda x: x.get("offset", 0), reverse=True)
        chars = list(plain_text)

        for item in items:
            offset = item.get("offset", 0)
            length = item.get("length", 0)
            ftype = item.get("type", "")

            if length <= 0 or offset >= len(chars):
                continue
            if offset + length > len(chars):
                length = len(chars) - offset

            if ftype == "bold":
                for i in range(offset, offset + length):
                    old = chars[i]
                    if old in VKAdapter._UNICODE_ITALIC:
                        # Character already italicized → use bold-italic
                        chars[i] = VKAdapter._UNICODE_BOLD_ITALIC.get(old, VKAdapter._UNICODE_BOLD.get(old, old))
                    else:
                        chars[i] = VKAdapter._UNICODE_BOLD.get(old, old)

            elif ftype == "italic":
                for i in range(offset, offset + length):
                    old = chars[i]
                    if old in VKAdapter._UNICODE_BOLD:
                        # Character already bolded → use bold-italic
                        chars[i] = VKAdapter._UNICODE_BOLD_ITALIC.get(old, VKAdapter._UNICODE_ITALIC.get(old, old))
                    else:
                        chars[i] = VKAdapter._UNICODE_ITALIC.get(old, old)

            elif ftype == "underline":
                # Combining low line U+0332 after each character
                for i in range(offset, offset + length):
                    chars[i] = chars[i] + "\u0332"

            # url — pass through (VK auto-detects URLs)
            # code — not handled here (pre-converted by agent or markdown parser)

        return "".join(chars)

    def _format_content_raw(self, content: str) -> tuple[str, str | None]:
        """Parse Markdown → (plain_text, format_data_json).

        Returns plain text with all Markdown markers stripped, and
        the native VK format_data JSON (or None if no formatting found).

        This is the primary formatting pipeline since VK plugin 1.2.0.
        format_data is passed directly to VK API messages.send as a
        separate parameter — no Unicode character substitution needed.
        """
        try:
            plain_text, format_data_json = markdown_format_data(content)
            return plain_text, format_data_json
        except Exception:
            return content, None

    def _format_content(self, content: str) -> str:
        """Parse Markdown → plain text with markers stripped.

        Does NOT apply Unicode styling — formatting is handled by
        the native format_data parameter passed to VK API.
        """
        plain_text, _ = self._format_content_raw(content)
        return plain_text

    # ------------------------------------------------------------------
    # Messaging
    # ------------------------------------------------------------------

    def format_message(self, content: str) -> str:
        """Override: strip Markdown markers for VK display.

        Native formatting (bold/italic/underline/link) is passed
        via format_data parameter in send().
        """
        return self._format_content(content)

    async def _register_outgoing(self, msg_id: str) -> None:
        """Register a sent message ID to prevent recursion.

        When VK echoes our own message back via Long Poll (known edge case
        in group chats), the recursion guard in _process_update will discard
        it based on this set.

        The set is periodically pruned to prevent memory growth.
        """
        if not msg_id:
            return
        async with self._recent_outgoing_lock:
            self._recent_outgoing_ids.add(msg_id)
            # Prune if over 200 entries (keep last 100)
            if len(self._recent_outgoing_ids) > 200:
                # Convert to list, sort numerically (desc), keep newest 100
                sorted_ids = sorted(
                    self._recent_outgoing_ids,
                    key=lambda x: int(x) if x.isdigit() else 0,
                    reverse=True,
                )
                self._recent_outgoing_ids = set(sorted_ids[:100])

    async def send_keyboard(
        self,
        chat_id: str,
        content: str,
        keyboard: "VKKeyboard",
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        """Send a message with an inline keyboard.

        Args:
            chat_id: VK peer_id
            content: Message text (Markdown — formatting via native format_data)
            keyboard: VKKeyboard instance (call .build() to get JSON)
            reply_to: Message ID to reply to
        """
        if not self._http_client or not self._token:
            return SendResult(success=False, error="Not connected")

        # Parse Markdown → plain text + native format_data
        plain_text, format_data = self._format_content_raw(content)
        kb_json = keyboard.build() if isinstance(keyboard, VKKeyboard) else keyboard

        params = {
            "peer_id": chat_id,
            "message": plain_text,
            "keyboard": kb_json,
            "random_id": int(time.time() * 1000) & 0x7FFFFFFF,
        }
        if format_data:
            params["format_data"] = format_data
        if reply_to:
            params["reply_to"] = reply_to

        async with self._limiters.send:
            try:
                result = await self._vk_api_call("messages.send", params)
                if result and "response" in result:
                    msg_id = str(result["response"])
                    await self._register_outgoing(msg_id)
                    return SendResult(
                        success=True,
                        message_id=msg_id,
                    )
                if result:
                    error = result.get("error", {}).get("error_msg", str(result))
                else:
                    error = "VK API returned no response (connection error or timeout)"
                logger.warning("[VK] send_keyboard failed: %s", error)
                return SendResult(success=False, error=error)
            except Exception as e:
                logger.error("[VK] send_keyboard exception: %s", e)
                return SendResult(success=False, error=str(e), retryable=True)

    async def remove_keyboard(
        self,
        chat_id: str,
        content: str = "",
    ) -> SendResult:
        """Remove the inline keyboard from the last bot message."""
        if not self._http_client or not self._token:
            return SendResult(success=False, error="Not connected")

        params = {
            "peer_id": chat_id,
            "message": content or "\u2063",  # invisible separator if empty
            "keyboard": build_remove_keyboard(),
            "random_id": int(time.time() * 1000) & 0x7FFFFFFF,
        }

        async with self._limiters.send:
            try:
                result = await self._vk_api_call("messages.send", params)
                if result and "response" in result:
                    msg_id = str(result["response"])
                    await self._register_outgoing(msg_id)
                    return SendResult(success=True, message_id=msg_id)
                return SendResult(success=False, error=str(result))
            except Exception as e:
                return SendResult(success=False, error=str(e), retryable=True)

    # ------------------------------------------------------------------
    # Progress-lane routing
    # ------------------------------------------------------------------

    # Gateway functions that render the progress/status lane. Hermes sends
    # those through the same adapter.send() as turn-final replies without
    # any platform-scoped metadata; the stable seam is the caller frame in
    # gateway/run_turn_runner.py / run_turn.py / run_busy.py. If upstream
    # renames them the check degrades to "main chat" — never misroutes.
    _PROGRESS_LANE_FUNCS = frozenset({
        "send_progress_messages",       # tool-progress bubble loop
        "_progress_send_or_edit",       # bubble first send
        "_edit_progress_message",       # bubble edits
        "_run_agent_notify_long_running",  # "⏳ Working — N min" heartbeat
        "_send_busy_reply",             # busy ack ("⏳ Working — … iteration …")
        "_send_busy_ack_reply",
        "_send_watcher_message",        # background process started/finished/failed
    })

    def _progress_lane_caller(self) -> bool:
        """True when a gateway progress/status function is on our call stack."""
        try:
            frame = sys._getframe(3)
        except (ValueError, AttributeError):
            return False
        for _ in range(8):
            if frame is None:
                break
            code = frame.f_code
            if (
                code.co_name in self._PROGRESS_LANE_FUNCS
                and "/gateway/" in code.co_filename.replace("\\", "/")
            ):
                return True
            frame = frame.f_back
        return False

    def _route_progress(self, chat_id: str, metadata: Optional[Dict[str, Any]]) -> str:
        """Redirect progress/status sends to the reasoning chat when enabled."""
        if not self._progress_chat_id or str(chat_id) == self._progress_chat_id:
            return chat_id
        if (metadata or {}).get("_interim_send") or self._progress_lane_caller():
            return self._progress_chat_id
        return chat_id

    async def send(
        self,
        chat_id: str,
        content: str,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        if not self._http_client or not self._token:
            return SendResult(success=False, error="Not connected")

        chat_id = self._route_progress(chat_id, metadata)

        # Check for keyboard in metadata
        keyboard = None
        if metadata and "keyboard" in metadata:
            keyboard = metadata["keyboard"]

        # Extract [[keyboard:...]] marker from RAW content BEFORE markdown parsing
        # (parse_markdown treats [[ as markdown link syntax and corrupts the JSON)
        if not keyboard:
            clean_content, kbd_data = extract_keyboard_marker(content)
            if kbd_data:
                keyboard = kbd_data
                content = clean_content

        # Extract [[carousel:...]] marker — if present, delegate to send_carousel()
        clean_content, carousel_data = extract_carousel_marker(content)
        if carousel_data:
            return await self.send_carousel(
                chat_id=chat_id,
                content=clean_content,
                template=carousel_data,
                keyboard=keyboard,
                reply_to=reply_to,
                metadata=metadata,
            )

        # Parse Markdown → plain text + native format_data
        plain_text, format_data = self._format_content_raw(content)

        params = {
            "peer_id": chat_id,
            "message": plain_text,
            "random_id": int(time.time() * 1000) & 0x7FFFFFFF,
        }
        if format_data:
            params["format_data"] = format_data
        if reply_to:
            params["reply_to"] = reply_to
        if keyboard:
            if isinstance(keyboard, VKKeyboard):
                params["keyboard"] = keyboard.build()
            elif isinstance(keyboard, dict):
                params["keyboard"] = json.dumps(keyboard, ensure_ascii=False)
            else:
                params["keyboard"] = str(keyboard)

        async with self._limiters.send:
            try:
                result = await self._vk_api_call("messages.send", params)
                if result and "response" in result:
                    msg_id = str(result["response"])
                    await self._register_outgoing(msg_id)
                    if self._progress_chat_id and str(chat_id) == self._progress_chat_id:
                        self._progress_msg_ids.add(msg_id)
                    return SendResult(
                        success=True,
                        message_id=msg_id,
                    )
                if result:
                    error = result.get("error", {}).get("error_msg", str(result))
                else:
                    error = "VK API returned no response (connection error or timeout)"
                logger.warning("[VK] send failed: %s", error)
                return SendResult(success=False, error=error)
            except Exception as e:
                logger.error("[VK] send exception: %s", e)
                return SendResult(success=False, error=str(e), retryable=True)

    async def send_typing(self, chat_id: str, metadata=None) -> None:
        """Send typing indicator via VK messages.setActivity with throttling."""
        if not self._http_client or not self._token:
            return
        chat_id = self._route_progress(chat_id, metadata)

        try:
            peer_id = int(chat_id)
        except (ValueError, TypeError):
            logger.warning("[VK] send_typing invalid chat_id=%s", chat_id)
            return

        logger.info("[VK] send_typing called for chat_id=%s", chat_id)

        # Throttle: skip if we sent typing recently for this chat.
        # Prevents VK flood control (error 9) from aggressive _keep_typing.
        now = time.monotonic()
        last = self._last_typing_time.get(chat_id, 0.0)
        if now - last < self._typing_cooldown:
            logger.info("[VK] send_typing THROTTLED for chat_id=%s (last=%.1fs ago)", chat_id, now - last)
            return
        self._last_typing_time[chat_id] = now

        params: Dict[str, Any] = {
            "peer_id": peer_id,
            "type": "typing",
        }

        # При отправке без group_id перестает работать прямое общение 1 на 1 в чате с пользователем.
        # group_id обязательно указывать если откравляешь сообьщение пользователю (0 < peer_id < 2000000000)
        if self._group_id: # and 0 < peer_id < 2000000000: Фильтровал для исключения ошибки при отправке в групповой чат. Но это внутренняя ошибка ВК. Сожет починят
            params["group_id"] = self._group_id
            
        logger.info("[VK] send_typing EXECUTING for chat_id=%s", chat_id)
        try:
            # Bypass the global API limiter: typing is time-sensitive and the
            # per-peer throttle above already prevents floods.
            result = await self._vk_api_call_with_token(
                "messages.setActivity", params, token=self._token
            )
            logger.info("[VK] send_typing RESULT for chat_id=%s: %s", chat_id, result)
        except Exception as e:
            logger.error("[VK] send_typing ERROR for chat_id=%s: %s", chat_id, e)

    async def send_carousel(
        self,
        chat_id: str,
        content: str,
        template: dict,
        keyboard: Optional[dict] = None,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        """Send a carousel (template) message via VK API.

        VK carousels are horizontal-scrollable elements with
        title, description, photo, action, and optional buttons.

        The first element defines the structure for all others.

        Args:
            chat_id: VK peer_id
            content: Text above the carousel (Markdown)
            template: Carousel dict: {"type":"carousel","elements":[...]}
            keyboard: Optional inline keyboard below the carousel
            reply_to: Message ID to reply to
        """
        if not self._http_client or not self._token:
            return SendResult(success=False, error="Not connected")

        plain_text, format_data = self._format_content_raw(content)
        template_json = json.dumps(template, ensure_ascii=False)

        params = {
            "peer_id": chat_id,
            "message": plain_text,
            "template": template_json,
            "random_id": int(time.time() * 1000) & 0x7FFFFFFF,
        }
        if format_data:
            params["format_data"] = format_data
        if keyboard:
            params["keyboard"] = json.dumps(keyboard, ensure_ascii=False)
        if reply_to:
            params["reply_to"] = reply_to

        async with self._limiters.send:
            try:
                result = await self._vk_api_call("messages.send", params)
                if result and "response" in result:
                    msg_id = str(result["response"])
                    await self._register_outgoing(msg_id)
                    return SendResult(success=True, message_id=msg_id)
                if result:
                    error = result.get("error", {}).get("error_msg", str(result))
                else:
                    error = "VK API returned no response (connection error or timeout)"
                logger.warning("[VK] send_carousel failed: %s", error)
                return SendResult(success=False, error=error)
            except Exception as e:
                logger.error("[VK] send_carousel exception: %s", e)
                return SendResult(success=False, error=str(e), retryable=True)

    async def send_image(
        self,
        chat_id: str,
        image_url: str,
        caption: Optional[str] = None,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        """Send image - download from URL and upload as VK attachment.

        Falls back to text link + caption if download/upload fails.
        """
        if not self._http_client or not self._token:
            return SendResult(success=False, error="Not connected")

        # Try download + upload
        try:
            local_path = await download_vk_image_by_url(
                self._http_client, image_url,
                download_limiter=self._limiters.download,
            )
            if local_path:
                result = await self.send_image_file(
                    chat_id=chat_id,
                    image_path=local_path,
                    caption=caption,
                    reply_to=reply_to,
                )
                # Clean up temp file
                try:
                    os.remove(local_path)
                except OSError:
                    pass
                if result.success:
                    return result
        except Exception as e:
            logger.warning("[VK] Image URL download/upload failed: %s", e)

        # Fallback: text link
        text = f"{caption}\n{image_url}" if caption else image_url
        return await self.send(
            chat_id=chat_id, content=text, reply_to=reply_to
        )

    async def send_image_file(
        self,
        chat_id: str,
        image_path: str,
        caption: Optional[str] = None,
        reply_to: Optional[str] = None,
        **kwargs,
    ) -> SendResult:
        """Send a local image file via VK photos.messagesUploadServer + messages.send."""
        return await self._send_file_attachment(
            chat_id=chat_id,
            file_path=image_path,
            caption=caption,
            reply_to=reply_to,
            attachment_type="photo",
        )

    async def send_document(
        self,
        chat_id: str,
        file_path: str,
        caption: Optional[str] = None,
        file_name: Optional[str] = None,
        reply_to: Optional[str] = None,
        **kwargs,
    ) -> SendResult:
        """Send a document via VK docs.getMessagesUploadServer + messages.send."""
        return await self._send_file_attachment(
            chat_id=chat_id,
            file_path=file_path,
            caption=caption,
            reply_to=reply_to,
            attachment_type="doc",
        )

    async def get_chat_info(self, chat_id: str) -> Dict[str, Any]:
        """Get info about a VK chat/peer."""
        try:
            result = await self._vk_api_call("messages.getConversationsById", {
                "peer_ids": chat_id,
            })
            if result and "response" in result:
                items = result["response"].get("items", [])
                if items:
                    item = items[0]
                    chat_type = "group" if item.get("chat_settings") else "dm"
                    name = (
                        item.get("chat_settings", {}).get("title")
                        or f"User {item.get('from_id', chat_id)}"
                    )
                    return {"name": name, "type": chat_type, "chat_id": chat_id}
        except Exception as e:
            logger.warning("[VK] get_chat_info failed for %s: %s", chat_id, e)

        return {"name": chat_id, "type": "dm", "chat_id": chat_id}

    async def edit_message(
        self,
        chat_id: str,
        message_id: str,
        content: str,
        *,
        finalize: bool = False,
    ) -> SendResult:
        """Edit a previously sent message via VK messages.edit API."""
        if not self._http_client or not self._token:
            return SendResult(success=False, error="Not connected")

        plain_text, format_data = self._format_content_raw(content)
        params = {
            "peer_id": (
                self._progress_chat_id
                if str(message_id) in self._progress_msg_ids
                else chat_id
            ),
            "message_id": message_id,
            "message": plain_text,
        }
        if format_data:
            params["format_data"] = format_data

        try:
            result = await self._vk_api_call("messages.edit", params)
            if result and "response" in result and result["response"] == 1:
                return SendResult(success=True, message_id=message_id)
            error = result.get("error", {}).get("error_msg", str(result)) if result else "No response"
            return SendResult(success=False, error=error)
        except Exception as e:
            return SendResult(success=False, error=str(e), retryable=True)

    async def delete_message(
        self,
        chat_id: str,
        message_id: str,
    ) -> bool:
        """Delete a previously sent message via VK messages.delete API."""
        if not self._http_client or not self._token:
            return False

        params = {
            "peer_id": (
                self._progress_chat_id
                if str(message_id) in self._progress_msg_ids
                else chat_id
            ),
            "cmids": message_id,
            "delete_for_all": 1,
        }

        try:
            result = await self._vk_api_call("messages.delete", params)
            if result and "response" in result:
                resp = result["response"]
                # API returns {peer_id: 1} on success
                return bool(resp.get(str(chat_id)) or resp.get(chat_id))
            return False
        except Exception as e:
            logger.warning("[VK] delete_message failed: %s", e)
            return False

    async def probe(self) -> Dict[str, Any]:
        """Health probe: check if the adapter is healthy."""
        if not self._http_client or not self._token:
            return {"ok": False, "error": "Not connected"}

        try:
            result = await self._vk_api_call("groups.getById", {})
            if result and "response" in result:
                groups = result["response"].get("groups", [])
                return {
                    "ok": True,
                    "group_id": self._group_id,
                    "group_name": groups[0].get("name", "") if groups else "",
                    "ts": self._ts,
                    "connected": self._connected,
                }
            return {"ok": False, "error": "Auth check failed"}
        except Exception as e:
            return {"ok": False, "error": str(e)}

    # ------------------------------------------------------------------
    # Long Poll
    # ------------------------------------------------------------------

    @staticmethod
    def _calc_init_delay(attempt: int) -> int:
        """Exponential backoff delay for init retries."""
        return min(INIT_RETRY_BASE * (2 ** (attempt - 1)), INIT_RETRY_MAX)

    async def _notify_user(self, level: str, title: str, message: str) -> None:
        """Send notification to user via Telegram (+ VK if level is info/recovery).

        Telegram is always attempted (best-effort).
        VK — only for recovery/info notifications (when the adapter is connected).
        """
        log_fn = logger.error if level == "error" else logger.warning
        log_fn("[VK] %s: %s — %s", level.upper(), title, message)

        # --- Telegram (always, best-effort) ---
        bot_token = os.getenv("TELEGRAM_BOT_TOKEN")
        tg_chat = os.getenv("TELEGRAM_HOME_CHANNEL")
        if bot_token and tg_chat:
            try:
                text = (
                    f"⚠️ VK — {title}:\n"
                    f"{message}"
                )
                async with httpx.AsyncClient(timeout=10.0) as client:
                    await client.post(
                        f"https://api.telegram.org/bot{bot_token}/sendMessage",
                        json={"chat_id": tg_chat, "text": text},
                    )
            except Exception:
                pass

        # --- VK — only for recovery (adapter is connected at this point) ---
        if level == "info":
            vk_chat = os.getenv("VK_HOME_CHANNEL")
            if vk_chat and self._connected:
                try:
                    await self.send(
                        chat_id=vk_chat,
                        content=f"✅ VK — {title}:\n{message}",
                    )
                except Exception:
                    pass

    async def _init_longpoll(self, account_id: str = "default") -> bool:
        """Initialise or re-initialise Long Poll connection.

        In multi-account mode, init for a specific account.
        In single-account mode, uses the default token.
        """
        # Get token for this account
        acct = self._accounts.get(account_id, {})
        token = acct.get("token", self._token)
        if not token:
            logger.error("[VK] No token for account %s", account_id)
            return False

        try:
            # Step 1: get group info using account-specific token
            group_result = await self._vk_api_call_with_token(
                "groups.getById", {}, token=token
            )
            if not group_result or "response" not in group_result:
                error = group_result.get("error", {}) if group_result else {}
                logger.error(
                    "[VK] Failed to get group info for %s: %s",
                    account_id,
                    error.get("error_msg", group_result) if error else group_result,
                )
                if account_id == "default":
                    self._set_fatal_error(
                        "auth_failed",
                        "Invalid VK_GROUP_TOKEN or missing group access",
                        retryable=True,
                    )
                return False

            groups = group_result["response"].get("groups", [])
            if not groups:
                logger.error("[VK] No groups found for account %s", account_id)
                if account_id == "default":
                    self._set_fatal_error(
                        "no_groups", "Token has no associated group", retryable=False
                    )
                return False
            group_id = groups[0].get("id")

            # Step 2: get Long Poll server
            lp_result = await self._vk_api_call_with_token(
                "groups.getLongPollServer",
                {"group_id": group_id},
                token=token,
            )
            if not lp_result or "response" not in lp_result:
                error = lp_result.get("error", {}) if lp_result else {}
                logger.error(
                    "[VK] Failed to get Long Poll server for %s: %s",
                    account_id,
                    error.get("error_msg", lp_result) if error else lp_result,
                )
                return False

            lp_data = lp_result["response"]
            server = lp_data.get("server")
            key = lp_data.get("key")
            ts = lp_data.get("ts")

            # Store state
            if len(self._accounts) > 1 or account_id != "default":
                self._account_states[account_id] = {
                    "server": server,
                    "key": key,
                    "ts": ts,
                    "group_id": group_id,
                    "token": token,
                    "poll_task": None,
                    "poll_failures": 0,
                }
            else:
                self._server = server
                self._key = key
                self._ts = ts
                self._group_id = group_id

            logger.info(
                "[VK] Long Poll initialised for %s: server=%s, ts=%s",
                account_id, server, ts,
            )
            return True
        except Exception as e:
            logger.error("[VK] Long Poll init error for %s: %s", account_id, e)
            return False

    async def _init_longpoll_for_account(self, account_id: str) -> bool:
        """Initialize Long Poll for a specific multi-account."""
        return await self._init_longpoll(account_id=account_id)

    async def _poll_loop_for_account(self, account_id: str) -> None:
        """Long Poll loop for a specific account in multi-account mode."""
        init_retries = 0
        notified_error = False

        while self._running:
            try:
                await self._poll_once_for_account(account_id)
                state = self._account_states.get(account_id, {})
                state["poll_failures"] = 0
                init_retries = 0
                if notified_error:
                    notified_error = False
                    await self._notify_user(
                        "info", f"Соединение восстановлено ({account_id})",
                        "Long Poll с VK работает штатно."
                    )
            except asyncio.CancelledError:
                break
            except Exception as e:
                state = self._account_states.get(account_id, {})
                state["poll_failures"] = state.get("poll_failures", 0) + 1
                failures = state["poll_failures"]
                logger.error(
                    "[VK] Poll error for %s (%d/%d): %s",
                    account_id, failures, MAX_POLL_FAILURES, e,
                )

                if failures >= MAX_POLL_FAILURES:
                    init_retries += 1
                    if not notified_error:
                        notified_error = True
                        await self._notify_user(
                            "error", f"Ошибка соединения ({account_id})",
                            f"Long Poll недоступен ({e}). Пытаюсь восстановить."
                        )
                    if init_retries > MAX_INIT_RETRIES:
                        await self._notify_user(
                            "error", f"Watchdog: long-recovery ({account_id})",
                            f"Не удалось переподключиться после {MAX_INIT_RETRIES} попыток. "
                            f"Проверка каждые {LONG_RECOVERY_DELAY}с."
                        )
                        await asyncio.sleep(LONG_RECOVERY_DELAY)
                        await self._init_longpoll(account_id=account_id)
                        continue
                    delay = self._calc_init_delay(init_retries)
                    if not await self._init_longpoll(account_id=account_id):
                        await asyncio.sleep(delay)
                        continue

                await asyncio.sleep(POLL_RECONNECT_DELAY)

    async def _poll_once_for_account(self, account_id: str) -> None:
        """Single Long Poll for a specific account."""
        state = self._account_states.get(account_id)
        if not state or not self._http_client:
            if not self._http_client:
                raise RuntimeError("HTTP client not available")
            raise RuntimeError(f"Account {account_id} has no poll state")

        server = state.get("server")
        key = state.get("key")
        ts = state.get("ts")
        if not server or not key:
            raise RuntimeError(f"Account {account_id} not initialised (server/key missing)")

        try:
            response = await self._http_client.get(
                server,
                params={
                    "act": "a_check",
                    "key": key,
                    "ts": ts,
                    "wait": LONGPOLL_TIMEOUT,
                },
                timeout=LONGPOLL_TIMEOUT + 10,
            )
            response.raise_for_status()
            data = response.json()
        except asyncio.TimeoutError:
            return
        except Exception as e:
            raise RuntimeError(f"Long Poll HTTP error for {account_id}: {e}") from e

        await self._process_poll_response(data, state, account_id)

    async def _poll_loop(self) -> None:
        """Main Long Poll loop with exponential backoff and user notifications.

        Never stops retrying — after exhausting MAX_INIT_RETRIES with backoff,
        enters long-recovery mode (checking every LONG_RECOVERY_DELAY seconds).
        """
        init_retries = 0
        notified_error = False  # avoid spam — notify once per error burst
        while self._running:
            try:
                await self._poll_once()
                self._poll_failures = 0
                init_retries = 0
                if notified_error:
                    notified_error = False
                    await self._notify_user(
                        "info", "Соединение восстановлено",
                        "Long Poll с VK работает штатно."
                    )
            except asyncio.CancelledError:
                break
            except Exception as e:
                self._poll_failures += 1
                logger.error(
                    "[VK] Poll error (%d/%d): %s",
                    self._poll_failures,
                    MAX_POLL_FAILURES,
                    e,
                )

                if self._poll_failures >= MAX_POLL_FAILURES:
                    init_retries += 1

                    if not notified_error:
                        notified_error = True
                        await self._notify_user(
                            "error", "Ошибка соединения",
                            f"Long Poll недоступен ({e}). "
                            "Пытаюсь восстановить соединение."
                        )

                    if init_retries > MAX_INIT_RETRIES:
                        # Never give up — switch to long-recovery mode
                        await self._notify_user(
                            "error", "Watchdog: long-recovery mode",
                            f"Не удалось переподключиться после {MAX_INIT_RETRIES} попыток. "
                            f"Проверка каждые {LONG_RECOVERY_DELAY}с."
                        )
                        logger.warning(
                            "[VK] Long recovery mode: retrying every %ds...",
                            LONG_RECOVERY_DELAY,
                        )
                        await asyncio.sleep(LONG_RECOVERY_DELAY)
                        await self._init_longpoll()
                        continue

                    delay = self._calc_init_delay(init_retries)
                    logger.warning(
                        "[VK] Re-init failed (%d/%d), retrying in %ds...",
                        init_retries,
                        MAX_INIT_RETRIES,
                        delay,
                    )
                    if not await self._init_longpoll():
                        await asyncio.sleep(delay)
                        continue

                await asyncio.sleep(POLL_RECONNECT_DELAY)

    async def _poll_once(self) -> None:
        """Single Long Poll request (single-account mode)."""
        if not self._http_client or not self._server or not self._key:
            if not self._http_client:
                raise RuntimeError("HTTP client not available")
            raise RuntimeError("Long Poll not initialised (server/key missing)")

        try:
            response = await self._http_client.get(
                self._server,
                params={
                    "act": "a_check",
                    "key": self._key,
                    "ts": self._ts,
                    "wait": LONGPOLL_TIMEOUT,
                },
                timeout=LONGPOLL_TIMEOUT + 10,
            )
            response.raise_for_status()
            data = response.json()
        except asyncio.TimeoutError:
            return
        except Exception as e:
            raise RuntimeError(f"Long Poll HTTP error: {e}") from e

        await self._process_poll_response(data, self, "default")

    async def _process_poll_response(
        self,
        data: dict,
        state_target: Any,
        account_id: str,
    ) -> None:
        """Process a Long Poll response, shared by single and multi-account paths.

        Args:
            data: The JSON response from Long Poll server.
            state_target: Either 'self' (single-account) or the account state dict.
            account_id: Account identifier for logging.
        """
        if "failed" in data:
            failed = data["failed"]
            if failed == 1:
                # History outdated — update ts
                if state_target is self:
                    self._ts = data.get("ts", self._ts)
                else:
                    state_target["ts"] = data.get("ts", state_target.get("ts"))
                return
            elif failed in (2, 3):
                logger.info("[VK] Long Poll %s for %s, re-initialising...",
                           "key expired" if failed == 2 else "info lost", account_id)
                await self._init_longpoll(account_id=account_id)
                return
            elif failed == 4:
                # Version mismatch — update ts
                if state_target is self:
                    self._ts = data.get("ts", self._ts)
                else:
                    state_target["ts"] = data.get("ts", state_target.get("ts"))
                return
            raise RuntimeError(f"Long Poll failure code: {failed}")

        # Update timestamp
        if state_target is self:
            self._ts = data.get("ts", self._ts)
        else:
            state_target["ts"] = data.get("ts", state_target.get("ts"))

        # Determine the per-account group_id for self-filtering
        group_id_for_filter = None
        if state_target is not self and isinstance(state_target, dict):
            group_id_for_filter = state_target.get("group_id")
        else:
            group_id_for_filter = self._group_id

        updates = data.get("updates", [])
        for update in updates:
            await self._process_update(update, group_id_for_filter=group_id_for_filter)

    async def _process_update(self, update: dict, group_id_for_filter: int = None) -> None:
        """Process a single Long Poll update.

        Args:
            update: The Long Poll update dict.
            group_id_for_filter: Per-account group_id for self-message filtering.
                                 If None, falls back to self._group_id.
        """
        update_type = update.get("type", "")
        if update_type == "message_event":
            await self._process_callback_event(update.get("object", {}))
            return

        if update_type != "message_new":
            return

        obj = update.get("object", {})
        message = obj.get("message", obj)

        # Skip outgoing messages (out=1 means sent by this bot/group)
        if message.get("out", 0) == 1:
            return

        # ── Self-message filtering (multi-layered) ──
        from_id = message.get("from_id")
        msg_id = str(message.get("id", ""))

        # Layer 1: from_id == negative group_id (VK convention for community messages)
        effective_group_id = group_id_for_filter or self._group_id
        if effective_group_id and from_id and from_id < 0 and abs(from_id) == effective_group_id:
            logger.debug("[VK] Filtered self-message by from_id: group=%s from=%s",
                        effective_group_id, from_id)
            return

        # Layer 2: recursion guard — check recently sent message IDs
        # Prevents re-entry when VK echoes our own message back without proper
        # out/from_id flags (known edge case in group chats).
        if msg_id:
            async with self._recent_outgoing_lock:
                if msg_id in self._recent_outgoing_ids:
                    self._recent_outgoing_ids.discard(msg_id)
                    logger.debug("[VK] Filtered self-message by recursion guard: msg_id=%s", msg_id)
                    return

        peer_id = message.get("peer_id")
        text = message.get("text", "")
        user_id = str(from_id) if from_id else None

        if not peer_id:
            return

        # Check for keyboard callback payload
        callback_data = parse_callback_payload(message)
        if callback_data:
            # Treat keyboard callback as a command-like message
            # Prepend /vk_ prefix for gateway routing
            text = f"/vk_{callback_data}" if not text else text
            # Store the raw callback data
            message["_callback_data"] = callback_data

        # Determine chat type
        chat_type = "group" if peer_id > 2000000000 else "dm"

        # Build chat info
        chat_name = None
        if chat_type == "group":
            chat_settings = message.get("chat_settings", {})
            chat_name = chat_settings.get("title", f"Chat {peer_id}")
        else:
            # Try to get user name
            user_info = await self._get_user_name(str(user_id)) if user_id else None
            chat_name = user_info or f"User {user_id}"

        source = self.build_source(
            chat_id=str(peer_id),
            chat_name=chat_name,
            chat_type=chat_type,
            user_id=user_id,
            user_name=chat_name,
            message_id=msg_id,
        )

        event = MessageEvent(
            text=text,
            message_type=MessageType.TEXT,
            source=source,
            message_id=msg_id,
        )

        # Handle attachments (photos, docs, etc.)
        attachments = message.get("attachments", [])
        if attachments:
            # Extract raw URLs
            raw_media = await self._extract_media_urls(attachments)
            if raw_media:
                event.media_urls = [url for url, _ in raw_media]
                event.media_types = [mtype for _, mtype in raw_media]
                # If there's no text but we have photos, mark as PHOTO
                if not text and any(
                    t.startswith("photo") for t in event.media_types
                ):
                    event.message_type = MessageType.PHOTO

            # Download attachments to local cache for AI analysis
            if self._http_client:
                try:
                    downloaded = await download_vk_attachments(
                        self._http_client, attachments,
                        download_limiter=self._limiters.download,
                    )
                    if downloaded:
                        # Add downloaded paths to event
                        event.media_urls = [path for path, _, _ in downloaded]
                        event.media_types = [mtype for _, mtype, _ in downloaded]
                        # Update message type based on downloaded media.
                        # VK voice notes arrive as audio_message attachments:
                        # mark them VOICE (not AUDIO) so the gateway routes
                        # them into the automatic STT pipeline.
                        att_types = {a.get("type") for a in attachments}
                        if any(t == "image" for t in event.media_types):
                            event.message_type = MessageType.PHOTO
                        elif "audio_message" in att_types:
                            event.message_type = MessageType.VOICE
                        elif any(t == "audio" for t in event.media_types):
                            event.message_type = MessageType.AUDIO
                        elif any(t == "video" for t in event.media_types):
                            event.message_type = MessageType.VOICE if False else MessageType.VIDEO
                        elif any(t == "document" for t in event.media_types):
                            event.message_type = MessageType.DOCUMENT
                except Exception as e:
                    logger.warning("[VK] Failed to download attachments: %s", e)

            # Voice notes: transcribe eagerly via the local Whisper server so
            # the command reaches the agent as text even if core STT is off.
            if event.message_type == MessageType.VOICE:
                from .stt import transcribe_audio
                for i, mtype in enumerate(event.media_types):
                    if mtype != "audio" or i >= len(event.media_urls):
                        continue
                    tx = await transcribe_audio(
                        self._http_client, event.media_urls[i],
                        limiter=self._limiters.download,
                    )
                    if tx:
                        event.text = f"{text}\n{tx}".strip() if text else tx
                        break

        # Handle forwarded messages
        fwd_messages = message.get("fwd_messages", [])
        if fwd_messages and not text:
            event.text = self._format_forwarded(fwd_messages)

        # --- Policy checks ---
        if chat_type == "dm":
            allowed, reason = check_dm_policy(
                user_id=str(user_id) if user_id else "0",
                dm_policy=self._dm_policy,
                allow_from=self._allow_from,
            )
            if not allowed:
                if reason == "Pairing required":
                    # Handle pairing flow
                    if not is_awaiting_pairing(str(user_id)):
                        code = issue_pairing_challenge(str(user_id))
                        try:
                            await self.send(
                                chat_id=str(peer_id),
                                content=get_pairing_message(code),
                            )
                        except Exception:
                            pass
                    else:
                        # User is awaiting pairing — check if this message is the code
                        if validate_pairing_code(str(user_id), text.strip()):
                            try:
                                await self.send(
                                    chat_id=str(peer_id),
                                    content="✅ Подтверждение успешно! Добро пожаловать.",
                                )
                            except Exception:
                                pass
                        else:
                            # Still wrong code — remind
                            try:
                                await self.send(
                                    chat_id=str(peer_id),
                                    content="❌ Неверный код. Попробуйте ещё раз или запросите новый код командой /pair.",
                                )
                            except Exception:
                                pass
                    return
                else:
                    logger.debug(
                        "[VK] DM rejected for user %s: %s", user_id, reason
                    )
                    return
        else:
            # Group chat
            allowed, reason = check_group_policy(
                user_id=str(user_id) if user_id else "0",
                group_policy=self._group_policy,
                group_allow_from=self._group_allow_from,
                require_mention=self._require_mention,
                message_text=text,
                bot_name=self._bot_name,
            )
            if not allowed:
                logger.debug(
                    "[VK] Group message rejected for user %s: %s", user_id, reason
                )
                return

        await self.handle_message(event)

    async def _process_callback_event(self, obj: dict) -> None:
        """
        Process a message_event (callback button press).

        VK sends message_event when user clicks a callback/inline button.
        The event contains payload but no visible message text.

        We:
          1. Parse the payload from the button
          2. Answer the event (acknowledge to VK)
          3. Create a virtual /vk_callback:... message and dispatch it
        """
        user_id = obj.get("user_id")
        peer_id = obj.get("peer_id")
        event_id = obj.get("event_id")
        payload_raw = obj.get("payload", "")

        if not peer_id or not event_id:
            return

        # Parse payload
        command = None
        extra_data = ""
        if payload_raw:
            try:
                payload = json.loads(payload_raw) if isinstance(payload_raw, str) else payload_raw
                if isinstance(payload, dict):
                    command = payload.pop("command", None) or payload.pop("cmd", None)
                    if payload:
                        extra_data = json.dumps(payload, ensure_ascii=False)
                else:
                    command = str(payload)
            except (json.JSONDecodeError, TypeError):
                command = str(payload_raw)

        if not command:
            command = "callback"

        # Answer the event — acknowledge so VK doesn't show error
        await self._answer_callback_event(event_id, peer_id, user_id)

        # Determine chat type
        chat_type = "group" if peer_id > 2000000000 else "dm"

        # Build virtual message text
        virtual_text = f"/vk_callback:{command}"
        if extra_data:
            virtual_text += f" {extra_data}"

        # Get user name for source context
        user_name = None
        if user_id:
            user_info = await self._get_user_name(str(user_id))
            user_name = user_info or f"User {user_id}"

        chat_name = user_name or f"Chat {peer_id}"

        source = self.build_source(
            chat_id=str(peer_id),
            chat_name=chat_name,
            chat_type=chat_type,
            user_id=str(user_id) if user_id else None,
            user_name=user_name,
            message_id=event_id,
        )

        event = MessageEvent(
            text=virtual_text,
            message_type=MessageType.TEXT,
            source=source,
            message_id=event_id,
            raw_message=obj,
        )

        logger.debug(
            "[VK] Callback event from user %s in chat %s: %s",
            user_id, peer_id, virtual_text,
        )

        await self.handle_message(event)

    async def _answer_callback_event(
        self,
        event_id: str,
        peer_id: int,
        user_id: Optional[int] = None,
        event_data: Optional[str] = None,
    ) -> None:
        """
        Acknowledge a callback event so VK doesn't show an error toast.

        Uses messages.sendMessageEventAnswer API.
        Optionally sends event_data for client-side actions:
          - {"type":"show_snackbar","text":"Готово"}
          - {"type":"open_link","link":"https://..."}
          - {"type":"open_app","app_id":...,"owner_id":...}
        """
        if not self._http_client or not self._token:
            return

        params = {
            "event_id": event_id,
            "peer_id": peer_id,
            "user_id": user_id if user_id else peer_id,
        }
        if event_data:
            params["event_data"] = event_data

        try:
            await self._vk_api_call("messages.sendMessageEventAnswer", params)
        except Exception as e:
            logger.warning("[VK] Failed to answer callback event %s: %s", event_id, e)

    # ------------------------------------------------------------------
    # File Attachments
    # ------------------------------------------------------------------

    async def _send_file_attachment(
        self,
        chat_id: str,
        file_path: str,
        caption: Optional[str] = None,
        reply_to: Optional[str] = None,
        attachment_type: str = "doc",
    ) -> SendResult:
        """Upload a file to VK and send as attachment."""
        if not os.path.exists(file_path):
            return SendResult(
                success=False, error=f"File not found: {file_path}"
            )

        try:
            # Step 1: Get upload URL
            if attachment_type == "photo":
                upload_result = await self._vk_api_call(
                    "photos.getMessagesUploadServer", {"peer_id": chat_id}
                )
            else:
                upload_result = await self._vk_api_call(
                    "docs.getMessagesUploadServer", {"peer_id": chat_id, "type": "doc"}
                )

            if not upload_result:
                return SendResult(
                    success=False,
                    error="Upload server request failed (no response)",
                )
            if "response" not in upload_result:
                return SendResult(
                    success=False,
                    error=upload_result.get("error", {}).get("error_msg", "Upload failed"),
                )

            upload_url = upload_result["response"]["upload_url"]

            # Throttle upload operations
            await self._limiters.upload.acquire()

            # Step 2: Upload file
            import httpx

            with open(file_path, "rb") as f:
                upload_response = await self._http_client.post(
                    upload_url,
                    files={"file": f},
                    timeout=60.0,
                )
            upload_response.raise_for_status()
            upload_data = upload_response.json()

            # Step 3: Save on VK servers
            if attachment_type == "photo":
                save_result = await self._vk_api_call(
                    "photos.saveMessagesPhoto",
                    {
                        "photo": upload_data.get("photo"),
                        "server": upload_data.get("server"),
                        "hash": upload_data.get("hash"),
                    },
                )
                if save_result and "response" in save_result:
                    photo = save_result["response"][0]
                    attachment_str = f"photo{photo['owner_id']}_{photo['id']}"
                else:
                    return SendResult(success=False, error="Photo save failed")
            else:
                save_result = await self._vk_api_call(
                    "docs.save",
                    {
                        "file": upload_data.get("file"),
                        "title": os.path.basename(file_path),
                    },
                )
                if save_result and "response" in save_result:
                    doc = save_result["response"]["doc"]
                    attachment_str = f"doc{doc['owner_id']}_{doc['id']}"
                else:
                    return SendResult(success=False, error="Doc save failed")

            # Step 4: Send with attachment
            params = {
                "peer_id": chat_id,
                "attachment": attachment_str,
                "random_id": int(time.time() * 1000) & 0x7FFFFFFF,
            }
            if caption:
                caption_text, caption_fmt = self._format_content_raw(caption)
                params["message"] = caption_text
                if caption_fmt:
                    params["format_data"] = caption_fmt
            if reply_to:
                params["reply_to"] = reply_to

            send_result = await self._vk_api_call("messages.send", params)
            if send_result and "response" in send_result:
                msg_id = str(send_result["response"])
                await self._register_outgoing(msg_id)
                return SendResult(
                    success=True,
                    message_id=msg_id,
                )
            return SendResult(success=False, error=str(send_result))

        except Exception as e:
            logger.error("[VK] File upload failed: %s", e)
            return SendResult(success=False, error=str(e), retryable=True)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    async def _vk_api_call(
        self, method: str, params: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """Make a VK API call using the default token."""
        return await self._vk_api_call_with_token(
            method, params, token=self._token
        )

    async def _vk_api_call_with_token(
        self,
        method: str,
        params: Dict[str, Any],
        *,
        token: str,
    ) -> Optional[Dict[str, Any]]:
        """Make a VK API call with a specific token.

        Args:
            method: VK API method name (e.g., 'messages.send').
            params: API parameters.
            token: Account-specific access token.
        """
        if not self._http_client:
            return None

        call_params = {
            "access_token": token,
            "v": self._api_version,
            **params,
        }

        # Throttle all API calls (except messages.send which is throttled at send() level)
        if method != "messages.send":
            await self._limiters.api.acquire()

        try:
            response = await self._http_client.post(
                f"{VK_API_BASE}/{method}",
                data=call_params,
            )
            response.raise_for_status()
            data = response.json()

            # Check for API errors
            if "error" in data:
                error = data["error"]
                error_code = error.get("error_code")
                error_msg = error.get("error_msg", "")

                # Token expired / invalid — fatal
                if error_code in (5, 27, 28, 29):
                    logger.error("[VK] Auth error (%d): %s", error_code, error_msg)
                    self._set_fatal_error(
                        f"auth_error_{error_code}",
                        f"VK API auth error: {error_msg}",
                        retryable=(error_code == 6),  # Only rate-limit is retryable
                    )
                    return data

                # Rate limit
                if error_code == 6:
                    logger.warning("[VK] Rate limited, retrying...")
                    await asyncio.sleep(1)
                    return await self._vk_api_call(method, params)

                logger.warning(
                    "[VK] API error %d: %s", error_code, error_msg
                )

            return data
        except Exception as e:
            logger.error("[VK] API call %s failed: %s", method, e)
            return None

    async def _get_user_name(self, user_id: str) -> Optional[str]:
        """Get user display name from VK API."""
        try:
            result = await self._vk_api_call("users.get", {
                "user_ids": user_id,
                "fields": "first_name,last_name",
            })
            if result and "response" in result:
                users = result["response"]
                if users:
                    user = users[0]
                    return f"{user.get('first_name', '')} {user.get('last_name', '')}".strip()
        except Exception:
            pass
        return None

    async def _extract_media_urls(
        self, attachments: List[Dict[str, Any]]
    ) -> List[Tuple[str, str]]:
        """Extract media URLs from VK message attachments."""
        media_urls = []
        for att in attachments:
            att_type = att.get("type")
            if att_type == "photo":
                photo = att.get("photo", {})
                # Get the largest photo size
                sizes = photo.get("sizes", [])
                if sizes:
                    # VK sorts sizes smallest first — take the last
                    best = sizes[-1]
                    url = best.get("url", "")
                    if url:
                        media_urls.append((url, "photo"))
            elif att_type == "doc":
                doc = att.get("doc", {})
                url = doc.get("url", "")
                if url:
                    media_urls.append((url, "doc"))
            elif att_type == "video":
                video = att.get("video", {})
                # VK video requires player URL for inline viewing
                player = video.get("player", "")
                if player:
                    media_urls.append((player, "video"))
        return media_urls

    @staticmethod
    def _format_forwarded(fwd_messages: List[Dict[str, Any]]) -> str:
        """Format forwarded messages into a text block."""
        parts = []
        for i, fwd in enumerate(fwd_messages):
            from_id = fwd.get("from_id", "")
            fwd_text = fwd.get("text", "")
            parts.append(f"[Forwarded {i+1}] (from: {from_id}):\n{fwd_text}")
        return "\n\n".join(parts)


# ------------------------------------------------------------------
# Plugin entry points
# ------------------------------------------------------------------


def check_requirements() -> bool:
    """Check if all dependencies are available."""
    try:
        import httpx  # noqa: F401
        return True
    except ImportError:
        return False


def validate_config(config) -> bool:
    """Validate that the VK token is present — from .env only."""
    if os.getenv("VK_GROUP_TOKEN"):
        return True
    # Optional: check for multi-account env vars
    return False


def register(ctx):
    """Plugin entry point — called by the Hermes plugin system."""
    try:
        from .tools import register_tools
        register_tools(ctx)
    except Exception:
        logger.warning("[VK] failed to register vk-files tools", exc_info=True)
    ctx.register_platform(
        name="vk",
        label="VK Messenger",
        adapter_factory=lambda cfg: VKAdapter(cfg),
        check_fn=check_requirements,
        validate_config=validate_config,
        required_env=["VK_GROUP_TOKEN"],
        install_hint="pip install httpx",
        allowed_users_env="VK_ALLOWED_USERS",
        allow_all_env="VK_ALLOW_ALL_USERS",
        max_message_length=MAX_MESSAGE_LENGTH,
        platform_hint=(
            "You are chatting via VK Messenger (VK Мессенджер). "
            "Messages support native formatting: **bold**, *italic*, "
            "<u>underline</u>, ***nested***, and [linked text](url). "
            "\n\n=== Keyboard ===\n"
            "Inline keyboard: [[keyboard:{\"buttons\":...,\"inline\":true}]] "
            "in send_message text. Buttons: text (sends message), "
            "callback (silent), open_link, location, vkpay, open_app. "
            "Color: primary, secondary, positive, negative. "
            "Max 10 buttons inline, 40 chat. "
            "\n\n=== Callback ===\n"
            "Text buttons with payload → `/vk_<command>` message. "
            "Callback buttons → `/vk_callback:<command>` message. "
            "Reply with show_snackbar (toast), edit_message (navigation). "
            "\n\n=== Carousel ===\n"
            "Use send_carousel() with template={\"type\":\"carousel\",\"elements\":[...]}. "
            "Each element: title(≤80), description(≤80), photo_id, action, buttons(≤3). "
            "\n\n=== Files & voice ===\n"
            "To send a local file to the chat use the vk_send_file tool "
            "(file_path, optional chat_id/caption): audio (.mp3/.ogg/.wav/.m4a/"
            ".aac/.flac/.opus) arrives as a playable voice message, anything "
            "else as a document. Prefer it over MEDIA: tags."
        ),
        emoji="💬",
    )
