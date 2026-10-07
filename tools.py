"""VK file-send tools (``vk-files`` toolset): vk_send_file uploads a local file
to the current (or given) VK chat as a voice message or document.

Logic ported from ~/projects/go/mcp-vk-files (SendAudioMessage / SendFile):
  docs.getMessagesUploadServer -> multipart upload -> docs.save -> messages.send
Audio extensions upload with type=audio_message (voice bubble in VK Messenger);
everything else uploads as type=doc. docs.save must receive ONLY the upload
fields (file, title) — extra fields make VK reject with "file is undefined".
"""

from __future__ import annotations

import json
import logging
import os
import time
from typing import Any

logger = logging.getLogger(__name__)

VK_API_URL = "https://api.vk.com/method/"
VK_API_VERSION = "5.199"

# Extensions VK renders as a playable voice bubble (mirrors mcp-vk-files isAudioFile).
_AUDIO_EXTS = {".mp3", ".ogg", ".m4a", ".aac", ".wav", ".flac", ".opus"}

# Extensions the VK upload server rejects (405 / wrong_file) — renamed to <name>.txt.
_BLOCKED_EXTS = {
    ".html", ".htm", ".svg", ".js", ".mjs", ".php", ".asp", ".aspx",
    ".jsp", ".exe", ".bat", ".cmd", ".sh", ".py",
}


def _token() -> str:
    """Group token: env first (gateway loads plugin .env), then plugin .env files."""
    tok = os.getenv("VK_GROUP_TOKEN", "").strip()
    if tok:
        return tok
    here = os.path.dirname(os.path.abspath(__file__))
    for env_path in (os.path.join(here, ".env"),
                     os.path.expanduser("~/.hermes/plugins/vk/.env")):
        try:
            with open(env_path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line.startswith("VK_GROUP_TOKEN=") and not line.startswith("VK_GROUP_TOKEN_"):
                        return line.split("=", 1)[1].strip().strip('"').strip("'")
        except OSError:
            continue
    return ""


def _current_chat_id() -> str:
    """Current session's VK peer id (raw digits), or '' outside a VK session."""
    try:
        from gateway.session_context import get_session_env
        raw = get_session_env("HERMES_SESSION_CHAT_ID", "") or ""
    except Exception:
        raw = os.getenv("HERMES_SESSION_CHAT_ID", "") or ""
    # VK session ids may arrive as "group:<id>" / "user:<id>" — peer_id is numeric.
    raw = raw.strip()
    if ":" in raw:
        raw = raw.rsplit(":", 1)[-1]
    return raw


def _safe_upload_name(filename: str) -> str:
    base = os.path.basename(filename)
    ext = os.path.splitext(base)[1].lower()
    return base + ".txt" if ext in _BLOCKED_EXTS else base


def _error(msg: str) -> str:
    return json.dumps({"ok": False, "error": msg}, ensure_ascii=False)


def _success(peer_id: str, msg_id: str, kind: str, filename: str) -> str:
    return json.dumps(
        {"ok": True, "peer_id": peer_id, "message_id": msg_id,
         "kind": kind, "file": filename},
        ensure_ascii=False,
    )


async def vk_send_file(args: dict | None = None, **_: Any) -> str:
    """Upload a local file to a VK chat as voice message or document."""
    args = args or {}
    import httpx

    file_path = str(args.get("file_path") or "").strip()
    if not file_path:
        return _error("file_path is required (absolute path to a local file)")
    if not os.path.isfile(file_path):
        return _error(f"File not found: {file_path}")

    token = _token()
    if not token:
        return _error("VK group token not found (VK_GROUP_TOKEN env or plugin .env)")

    chat_id = str(args.get("chat_id") or "").strip()
    if ":" in chat_id:
        chat_id = chat_id.rsplit(":", 1)[-1]
    chat_id = chat_id or _current_chat_id()
    if not chat_id or not chat_id.lstrip("-").isdigit():
        return _error(
            f"chat_id is required and could not be resolved from the current session "
            f"(got {chat_id!r}); pass the numeric VK peer_id explicitly")

    caption = str(args.get("caption") or "").strip()
    ext = os.path.splitext(file_path)[1].lower()
    is_audio = ext in _AUDIO_EXTS
    doc_type = "audio_message" if is_audio else "doc"
    upload_name = _safe_upload_name(file_path)

    try:
        async with httpx.AsyncClient(timeout=60.0) as client:
            # 1. Upload server for this peer
            r = await client.get(VK_API_URL + "docs.getMessagesUploadServer", params={
                "access_token": token, "v": VK_API_VERSION,
                "peer_id": chat_id, "type": doc_type,
            })
            data = r.json()
            if "error" in data:
                return _error(f"getMessagesUploadServer: {data['error'].get('error_msg', data['error'])}")
            upload_url = data.get("response", {}).get("upload_url", "")
            if not upload_url:
                return _error("getMessagesUploadServer returned no upload_url")

            # 2. Multipart upload
            with open(file_path, "rb") as f:
                r = await client.post(upload_url, files={"file": (upload_name, f)})
            try:
                upload_data = r.json()
            except ValueError:
                return _error(f"upload server returned non-JSON (HTTP {r.status_code}): {r.text[:200]}")
            if "error" in upload_data or "file" not in upload_data:
                return _error(f"upload failed: {json.dumps(upload_data, ensure_ascii=False)[:300]}")

            # 3. docs.save — ONLY file+title (extra fields => "file is undefined")
            r = await client.post(VK_API_URL + "docs.save", data={
                "access_token": token, "v": VK_API_VERSION,
                "file": upload_data["file"], "title": upload_name,
            })
            data = r.json()
            if "error" in data:
                return _error(f"docs.save: {data['error'].get('error_msg', data['error'])}")
            doc = data.get("response", {})
            if is_audio:
                am = doc.get("audio_message")
                if not am:
                    return _error(f"docs.save returned no audio_message: {json.dumps(doc)[:200]}")
                attachment = f"audio_message{am['owner_id']}_{am['id']}_{am['access_key']}"
            else:
                d = doc.get("doc")
                if not d:
                    return _error(f"docs.save returned no doc: {json.dumps(doc)[:200]}")
                attachment = f"doc{d['owner_id']}_{d['id']}"
                if d.get("access_key"):
                    attachment += f"_{d['access_key']}"

            # 4. Send with attachment
            params = {
                "access_token": token, "v": VK_API_VERSION,
                "peer_id": chat_id, "attachment": attachment,
                "random_id": int(time.time() * 1000) & 0x7FFFFFFF,
            }
            if caption:
                params["message"] = caption[:4000]
            r = await client.post(VK_API_URL + "messages.send", data=params)
            data = r.json()
            if "error" in data:
                return _error(f"messages.send: {data['error'].get('error_msg', data['error'])}")
            msg_id = str(data.get("response", ""))
            logger.info("[vk-files] sent %s to peer %s (msg %s)", doc_type, chat_id, msg_id)
            return _success(chat_id, msg_id, doc_type, upload_name)
    except httpx.HTTPError as e:
        return _error(f"HTTP error: {e}")
    except Exception as e:  # noqa: BLE001
        logger.exception("[vk-files] vk_send_file failed")
        return _error(f"{type(e).__name__}: {e}")


_TOOLS = {
    "vk_send_file": (
        vk_send_file,
        "Send a local file to a VK chat as an attachment. Audio files (.mp3/.ogg/.m4a/"
        ".aac/.wav/.flac/.opus) upload as VK voice messages (playable bubble); any other "
        "file uploads as a document. Defaults to the CURRENT chat — pass chat_id to "
        "target another peer. Returns the VK message id.",
        {
            "file_path": {"type": "string", "description": "Absolute path to the local file"},
            "chat_id": {"type": "string", "description": "Target VK peer id (numeric). Omit to send to the current chat."},
            "caption": {"type": "string", "description": "Optional text message to attach alongside the file."},
        },
        ["file_path"],
    ),
}


def register_tools(ctx) -> None:
    """Register the vk-files toolset (available whenever the VK plugin loads)."""
    for name, (handler, description, properties, required) in _TOOLS.items():
        parameters: dict[str, Any] = {"type": "object", "properties": properties}
        if required:
            parameters["required"] = required
        ctx.register_tool(
            name=name, toolset="vk-files", handler=handler, description=description,
            schema={"name": name, "description": description, "parameters": parameters},
            is_async=True, emoji="📎",
            check_fn=lambda: bool(_token()),
        )
