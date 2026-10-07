"""Inbound voice-message transcription via the local Whisper STT server.

The gpu-switcher /tts stack runs faster-whisper at http://127.0.0.1:8791
(POST /transcribe, multipart "file" -> {"text", "language", "duration"}).
Override the endpoint with VK_STT_URL; set VK_STT_URL="" to disable.
"""

import logging
import os
from typing import Optional

logger = logging.getLogger(__name__)

DEFAULT_STT_URL = "http://127.0.0.1:8791/transcribe"


def stt_url() -> str:
    return os.getenv("VK_STT_URL", DEFAULT_STT_URL).strip()


async def transcribe_audio(client, file_path: str, limiter=None) -> Optional[str]:
    """Transcribe a local audio file; returns text or None (never raises)."""
    url = stt_url()
    if not url:
        return None
    try:
        if limiter:
            await limiter.acquire()
        with open(file_path, "rb") as f:
            resp = await client.post(
                url,
                files={"file": (os.path.basename(file_path), f)},
                timeout=180.0,
            )
        resp.raise_for_status()
        data = resp.json()
        text = str(data.get("text") or "").strip()
        if text:
            logger.info("[VK] STT ok (%s): %d chars", data.get("language"), len(text))
            return text
        logger.info("[VK] STT returned empty transcript for %s", file_path)
        return None
    except Exception as e:  # noqa: BLE001
        logger.warning("[VK] STT failed for %s: %s", file_path, e)
        return None
