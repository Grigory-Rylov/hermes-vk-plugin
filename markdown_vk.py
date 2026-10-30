"""
Markdown-to-VK-format parser.

Converts Markdown text to VK Format objects (format_data) via a stack-based
tokenizer.  Supports the formatting types supported by VK API:

  **bold**       → bold span
  *italic*       → italic span
  <u>underline</u> → underline span
  ***bold italic*** → nested bold+italic
  [text](url)    → clickable link
  \\*escaped\\*    → literal asterisks

Nested formatting is supported (e.g. **bold *italic* text**).

Inspired by vkbottle markdown_parser.py:
  https://github.com/vkbottle/vkbottle/blob/master/vkbottle/tools/markdown_parser.py

vkbottle is MIT-licensed:
  Copyright (c) 2019 timoniq
  Copyright (c) 2022-2024 feeeek (Axd1x8a)
  Copyright (c) 2024 luwqz1
  https://github.com/vkbottle/vkbottle/blob/master/LICENSE
"""

from __future__ import annotations

import dataclasses
import json
import re
from typing import cast

from .format_data import Format, bold, italic, underline, url

# ---- Tokenizer -------------------------------------------------------------

# Tokenizer patterns, in priority order:
#   triple (***) must NOT match within **** — use (?!\*) lookahead
#   bold (**) matches after triple
#   italic (*) always matches; underscore (_) only at word boundaries
#   bracket_close matches bare ] not followed by (
#   text excludes all special chars EXCEPT underscore (which italic_underscore handles)
_TOKEN_RE = re.compile(
    r"(?P<esc>\\.)|"
    r"(?P<bs>\\)|"
    r"(?P<triple>\*\*\*(?!\*)|___(?!_))|"
    r"(?P<bold>\*\*|__)|"
    r"(?P<italic>\*|(?<!\w)_|_(?!\w))|"
    r"(?P<u_open><u>)|"
    r"(?P<u_close></u>)|"
    r"(?P<url_open>\[)|"
    r"(?P<url_mid>\]\()|"
    r"(?P<url_close>\))|"
    r"(?P<lparen>\()|"
    r"(?P<bracket_close>\](?!\())|"
    r"(?P<word_underscore>(?<=\w)_(?=\w))|"
    r"(?P<text>[^\*__\[\]()<>\\]+)"
)

# Format type → factory function
_FMT_MAP = {"bold": bold, "italic": italic, "u": underline}


class Token(tuple):
    """Immutable token: (type, value)."""
    type: str
    value: str

    def __new__(cls, type: str, value: str):  # noqa: A002
        return super().__new__(cls, (type, value))

    @property
    def type(self) -> str:  # noqa: A003
        return self[0]

    @property
    def value(self) -> str:
        return self[1]


# ---- Stack frame -----------------------------------------------------------

@dataclasses.dataclass
class StackFrame:
    """Parser stack frame for tracking open formatting contexts."""
    ctx_type: str | None
    parts: list[str | Format] = dataclasses.field(default_factory=list)
    url_data: str | Format | None = None
    open_marker: str = ""


# ---- Helpers ---------------------------------------------------------------

def _tokenize(text: str) -> list[Token]:
    return [Token(cast("str", m.lastgroup), m.group()) for m in _TOKEN_RE.finditer(text)]


def _unescape(text: str) -> str:
    return (
        text.replace("\\*", "*")
        .replace("\\_", "_")
        .replace("\\[", "[")
        .replace("\\]", "]")
        .replace("\\`", "`")
    )


def _join(parts: list[str | Format]) -> str | Format:
    if not parts:
        return ""
    # If only one piece, return as-is
    if len(parts) == 1:
        return parts[0]
    # Check if there are any Format objects among the parts
    has_formats = any(isinstance(p, Format) for p in parts)
    if not has_formats:
        # All plain strings — simple concatenation
        return "".join(p for p in parts)  # type: ignore[misc]
    # Mix of str and Format — wrap in a container so sibling formats
    # stay independent and don't overlap in format_data
    return Format.container(parts)


def _close_frame(stack: list[StackFrame], closing_marker: str) -> None:
    """Close the current formatting frame.

    If the frame has no content, emit the literal markers as text.
    """
    frame = stack.pop()
    inner = _join(frame.parts)
    if inner:
        stack[-1].parts.append(_FMT_MAP[cast("str", frame.ctx_type)](inner))
    else:
        stack[-1].parts.append(frame.open_marker + closing_marker)


def _resolve_triple(token: Token, stack: list[StackFrame]) -> None:
    """Resolve a pending triple marker on top of the stack.

    Called when the next marker (\\* or \\*\\*) arrives after \\***.
    Handles both full closure (\\*\\*\\* matches \\*\\*\\*) and partial closure.
    """
    ctx = stack[-1]
    parts = ctx.parts
    marker = token.value
    fmt_type = token.type

    # Full closure: *** matches ***
    if marker == ctx.open_marker:
        inner = _join(parts)
        stack.pop()
        # If empty -> literals, otherwise -> nested formatting
        stack[-1].parts.append(bold(italic(inner)) if inner else ctx.open_marker + marker)
        return

    # Partial closure
    rem_type = "italic" if fmt_type == "bold" else "bold"
    remainder = ctx.open_marker[len(marker):]
    inner = _join(parts)

    if inner:
        # Nest formatted content into remainder (preserve LIFO nesting)
        closed_content = _FMT_MAP[fmt_type](inner)
        stack[-1] = StackFrame(rem_type, parts=[closed_content], open_marker=remainder)
    else:
        # Empty content -> emit literals and remove frame
        stack.pop()
        stack[-1].parts.append(ctx.open_marker + marker)


# ---- Token handlers --------------------------------------------------------

def _handle_literal(token: Token, stack: list[StackFrame]) -> None:
    """Process escapes, backslashes, and literal text."""
    ctx = stack[-1]
    if token.type == "esc":
        ctx.parts.append(token.value[1])
    elif token.type == "bs":
        ctx.parts.append("\\")
    else:
        ctx.parts.append(token.value)


def _handle_format(token: Token, stack: list[StackFrame]) -> None:
    """Handle bold, italic, and triple markers with lazy resolution."""
    # Try to resolve pending triple on stack top
    if stack[-1].ctx_type == "triple" and token.value[0] == stack[-1].open_marker[0]:
        _resolve_triple(token, stack)
        return

    # Incoming triple
    if token.type == "triple":
        ctx = stack[-1]
        if ctx.ctx_type in ("bold", "italic") and token.value.startswith(ctx.open_marker):
            _close_frame(stack, ctx.open_marker)
            marker = token.value[len(ctx.open_marker):]
            token = Token("italic" if ctx.ctx_type == "bold" else "bold", marker)

    # Standard open/close
    ctx = stack[-1]
    if ctx.ctx_type == token.type and ctx.open_marker == token.value:
        _close_frame(stack, token.value)
    else:
        stack.append(StackFrame(token.type, open_marker=token.value))


def _handle_underline(token: Token, stack: list[StackFrame]) -> None:
    """Handle opening/closing <u> and </u>."""
    ctx = stack[-1]
    if token.type == "u_open":
        stack.append(StackFrame("u", open_marker="<u>"))
    elif ctx.ctx_type == "u":
        _close_frame(stack, token.value)
    else:
        ctx.parts.append("<u>")


def _search_stack(stack: list[StackFrame], ctx_type: str) -> int | None:
    """Search the stack for a frame with the given ctx_type, from top down.

    Returns the index of the matching frame, or None if not found.
    """
    for i in range(len(stack) - 1, -1, -1):
        if stack[i].ctx_type == ctx_type:
            return i
    return None


def _handle_url(token: Token, stack: list[StackFrame]) -> None:
    """Process URL states: open, text, href, close."""
    ctx = stack[-1]
    if token.type == "url_open":
        stack.append(StackFrame("url_text", open_marker="["))
    elif token.type == "lparen":
        # Standalone '(' — treat as literal
        ctx.parts.append(token.value)
    elif token.type == "bracket_close":
        # Bare ']' not followed by '(': if a link-text frame is open it was a
        # literal bracket (e.g. "[model]" in help text) — close it as literal
        # text instead of leaving it open (unclosed frames leaked "](" garbage).
        url_idx = _search_stack(stack, "url_text")
        if url_idx is not None:
            frames = []
            while len(stack) > url_idx:
                frames.append(stack.pop())
            stack[-1].parts.append("[")
            for fr in reversed(frames):
                stack[-1].parts.extend(fr.parts)
            stack[-1].parts.append("]")
        else:
            ctx.parts.append(token.value)
    elif token.type == "url_mid":
        # Search the stack for url_text frame (handles formatting inside links)
        url_idx = _search_stack(stack, "url_text")
        if url_idx is not None:
            url_frame = stack[url_idx]
            link_text = _join(url_frame.parts)
            # Pop everything down to and including url_text
            while len(stack) > url_idx:
                stack.pop()
            stack.append(StackFrame("url_href", url_data=link_text))
        else:
            # No url_text frame — treat as literal
            ctx.parts.append(token.value)
    elif token.type == "url_close":
        # Search the stack for url_href frame
        url_idx = _search_stack(stack, "url_href")
        if url_idx is not None:
            url_frame = stack[url_idx]
            href_raw = _join(url_frame.parts)
            lt_raw = url_frame.url_data

            close_link_text: str | Format = lt_raw if lt_raw is not None else ""
            if isinstance(close_link_text, str):
                close_link_text = _unescape(close_link_text)

            href_str = str(href_raw)
            href_str = _unescape(href_str)

            # Pop everything down to and including url_href
            while len(stack) > url_idx:
                stack.pop()
            stack[-1].parts.append(url(close_link_text, href=href_str))
        else:
            # Standalone ')' outside link context — treat as literal
            ctx.parts.append(token.value)


def _process_token(token: Token, stack: list[StackFrame]) -> None:
    """Dispatch token to the appropriate handler based on token type."""
    match token.type:
        case "esc" | "bs" | "text":
            _handle_literal(token, stack)
        case "bold" | "italic" | "triple":
            _handle_format(token, stack)
        case "u_open" | "u_close":
            _handle_underline(token, stack)
        case "url_open" | "url_mid" | "url_close" | "lparen" | "bracket_close":
            _handle_url(token, stack)
        case "word_underscore":
            _handle_literal(token, stack)


def _apply_fallback(stack: list[StackFrame]) -> None:
    """Convert unclosed frames into literal text."""
    while len(stack) > 1:
        frame = stack.pop()
        if frame.ctx_type in ("url_text", "url_href"):
            marker = "[" if frame.ctx_type == "url_text" else f"[{frame.url_data or ''}]("
            stack[-1].parts.append(marker)
            stack[-1].parts.extend(frame.parts)
            # Never emit "](" for an unclosed link-text frame — it renders as
            # garbage in VK. Close with ']' (url_text) / ')' (url_href) instead.
            if frame.ctx_type == "url_text":
                stack[-1].parts.append("]")
            else:
                stack[-1].parts.append(")")
            continue
        if frame.ctx_type == "triple":
            stack[-1].parts.append(frame.open_marker)
            stack[-1].parts.extend(frame.parts)
            continue
        if frame.ctx_type not in _FMT_MAP:
            continue
        stack[-1].parts.append(frame.open_marker)
        stack[-1].parts.extend(frame.parts)


# ---- Public API ------------------------------------------------------------

def parse_markdown(text: str) -> str | Format:
    """Parse a Markdown string into VK Format objects.

    Supports **bold**, *italic*, <u>underline</u>, [url](link), and
    nested combinations like **bold *italic***.  Escape sequences like
    \\* are honored.

    Returns:
        A Format tree if formatting was found, or a plain str if the
        text had no formatting markers.
    """
    tokens = _tokenize(text)
    stack: list[StackFrame] = [StackFrame(ctx_type=None)]

    for token in tokens:
        _process_token(token, stack)

    _apply_fallback(stack)

    root_parts = stack[0].parts
    if not root_parts:
        return ""

    result = _join(root_parts)

    if isinstance(result, Format):
        return result if result.string else ""

    return _unescape(cast("str", result))


def markdown_to_plain(text: str) -> str:
    """Convert Markdown to plain text by stripping formatting markers."""
    result = parse_markdown(text)
    return str(result) if isinstance(result, Format) else result


def markdown_format_data(text: str) -> tuple[str, str | None]:
    """Convert Markdown to (plain_text, format_data_json).

    Args:
        text: Markdown-formatted text.

    Returns:
        (plain_text, format_data_json) — plain_text has all formatting
        markers stripped; format_data_json is a JSON string for the
        format_data parameter of messages.send, or None if no formatting
        was found.
    """
    fmt = parse_markdown(text)

    if isinstance(fmt, Format):
        return fmt.string, fmt.as_raw_data()
    else:
        return fmt or "", None


# Keyboard marker extraction
# The AI can embed a keyboard in the message using:
#   [[keyboard:{"buttons":[[{...}],[{...}]],"inline":true}]]
# The marker is stripped from the visible text and the keyboard is attached
# to the VK messages.send call.

_KEYBOARD_MARKER_RE = re.compile(
    r"\[\[keyboard:\s*(\{.*\})\s*\]\]", re.DOTALL
)

_CAROUSEL_MARKER_RE = re.compile(
    r"\[\[carousel:\s*(\{.*\})\s*\]\]", re.DOTALL
)


def extract_keyboard_marker(text: str) -> tuple[str, dict | None]:
    """Extract [[keyboard:...]] marker from message text.

    Args:
        text: Raw message text potentially containing [[keyboard:...]]

    Returns:
        (clean_text, keyboard_dict_or_None)
    """
    if not text:
        return text, None

    match = _KEYBOARD_MARKER_RE.search(text)
    if not match:
        return text, None

    try:
        keyboard_data = json.loads(match.group(1))
        clean_text = _KEYBOARD_MARKER_RE.sub("", text).strip()
        return clean_text, keyboard_data
    except (json.JSONDecodeError, KeyError):
        return text, None


def extract_carousel_marker(text: str) -> tuple[str, dict | None]:
    """Extract [[carousel:...]] marker from message text.

    The AI can embed a carousel in the message using:
      [[carousel:{"type":"carousel","elements":[...]}]]
    The marker is stripped and the carousel is attached
    to the VK messages.send call via send_carousel().

    Args:
        text: Raw message text potentially containing [[carousel:...]]

    Returns:
        (clean_text, carousel_dict_or_None)
    """
    if not text:
        return text, None

    match = _CAROUSEL_MARKER_RE.search(text)
    if not match:
        return text, None

    try:
        carousel_data = json.loads(match.group(1))
        clean_text = _CAROUSEL_MARKER_RE.sub("", text).strip()
        return clean_text, carousel_data
    except (json.JSONDecodeError, KeyError):
        return text, None
