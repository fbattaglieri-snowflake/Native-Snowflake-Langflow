"""Deterministic pre-sink redaction for traces, spans, and audit logs.

Implements ADR-007 and [G-A2]:
- Strips Bearer/OAuth/PAT/JWT tokens, passwords, private keys, auth headers, and
  Snowflake Postgres `access_roles` before writing to local storage or Snowflake.
- Strips opaque/signed multi-turn replay blocks (`encrypted_content`,
  `reasoning.encrypted_content`, `thinking.signature`, `redacted_thinking.data`)
  so telemetry sinks cannot be used to replay signed reasoning state.
- Masks PII spans returned by `AI_REDACT` in `detect` mode (`[G-A2]`) before any
  trace or audit sink persists the payload, preserving span metadata (`category`,
  `start`, `end`, `confidence`) while replacing raw PII substrings.
"""

from __future__ import annotations

import copy
import re
from typing import Any


REDACTED_SECRET = "[REDACTED_SECRET]"
REDACTED_HEADER = "[REDACTED_AUTH_HEADER]"
REDACTED_REPLAY_STATE = "[REDACTED_REPLAY_STATE]"
REDACTED_ACCESS_ROLES = "[REDACTED_ACCESS_ROLES]"

# Sensitive keys (case-insensitive normalized)
_SENSITIVE_SECRET_KEYS = frozenset(
    {
        "password",
        "passwd",
        "pwd",
        "secret",
        "client_secret",
        "api_key",
        "apikey",
        "access_token",
        "refresh_token",
        "session_token",
        "master_token",
        "id_token",
        "token",
        "pat",
        "private_key",
        "private_key_passphrase",
        "passcode",
        "credentials",
        "pgpassword",
    }
)

_SENSITIVE_HEADER_KEYS = frozenset(
    {
        "authorization",
        "proxy-authorization",
        "x-api-key",
        "x-auth-token",
        "x-snowflake-authorization-token-type",
        "cookie",
        "set-cookie",
    }
)

_REPLAY_SIGNATURE_KEYS = frozenset(
    {
        "encrypted_content",
        "reasoning.encrypted_content",
        "signature",
        "thinking.signature",
        "thinking_signature",
    }
)

_AI_REDACT_SPAN_LIST_KEYS = frozenset(
    {
        "spans",
        "detected_spans",
        "pii_spans",
        "findings",
        "entities",
        "detections",
        "matches",
    }
)

_AI_REDACT_SPAN_TEXT_KEYS = (
    "text",
    "value",
    "matched_text",
    "span_text",
    "original_text",
    "pii_value",
    "mention",
)

# Regex patterns for inline secret scrubbing in free text
_BEARER_RE = re.compile(r"(?i)\b(Bearer\s+)[A-Za-z0-9._~+/=-]{8,}")
_JWT_RE = re.compile(r"\beyJ[A-Za-z0-9_-]{8,}\.eyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\b")
_PAT_RE = re.compile(r"\b(?:ver:\d+-hint:\S+|pat_[A-Za-z0-9_-]{12,}|sfpat_[A-Za-z0-9_-]{12,})\b")
_PEM_RE = re.compile(
    r"-----BEGIN [A-Z0-9 ]*PRIVATE KEY-----[\s\S]*?-----END [A-Z0-9 ]*PRIVATE KEY-----"
)
_PG_URI_CRED_RE = re.compile(r"(?i)(postgres(?:ql)?://[^:/\s]+:)([^@\s]+)(@)")
_KV_SECRET_RE = re.compile(
    r"(?i)\b(password|passwd|secret|api_key|access_token|session_token|private_key)\s*([=:])\s*(['\"]?)([^\s,'\";]+)\3"
)


def redact_text_secrets(text: str) -> str:
    """Scrub inline tokens, JWTs, PATs, PEM blocks, and URI passwords from a string."""
    if not text or not isinstance(text, str):
        return text
    scrubbed = _PEM_RE.sub(REDACTED_SECRET, text)
    scrubbed = _BEARER_RE.sub(rf"\1{REDACTED_SECRET}", scrubbed)
    scrubbed = _JWT_RE.sub(REDACTED_SECRET, scrubbed)
    scrubbed = _PAT_RE.sub(REDACTED_SECRET, scrubbed)
    scrubbed = _PG_URI_CRED_RE.sub(rf"\1{REDACTED_SECRET}\3", scrubbed)
    scrubbed = _KV_SECRET_RE.sub(rf"\1\2\3{REDACTED_SECRET}\3", scrubbed)
    return scrubbed


def _is_span_dict(item: Any) -> bool:
    """Check whether a dict represents a detected PII span from AI_REDACT detect mode."""
    if not isinstance(item, dict):
        return False
    has_category_or_bounds = any(
        k in item for k in ("category", "pii_category", "type", "entity_type", "label", "start", "end")
    )
    has_text_field = any(k in item for k in _AI_REDACT_SPAN_TEXT_KEYS)
    return has_category_or_bounds and (has_text_field or ("start" in item and "end" in item))


def _extract_span_category(span: dict[str, Any]) -> str:
    for key in ("category", "pii_category", "type", "entity_type", "label"):
        val = span.get(key)
        if isinstance(val, str) and val.strip():
            return val.strip().upper()
    return "PII"


def _mask_single_span_dict(span: dict[str, Any]) -> tuple[dict[str, Any], list[str], tuple[int, int, str] | None]:
    """Mask raw PII fields inside a single span dict and return raw texts/offsets to scrub."""
    masked = dict(span)
    category = _extract_span_category(masked)
    marker = f"[REDACTED_PII:{category}]"
    discovered_raw_values: list[str] = []

    for text_key in _AI_REDACT_SPAN_TEXT_KEYS:
        raw_val = masked.get(text_key)
        if isinstance(raw_val, str) and raw_val and not raw_val.startswith("[REDACTED_PII:"):
            discovered_raw_values.append(raw_val)
            masked[text_key] = marker

    offset_tuple: tuple[int, int, str] | None = None
    start = masked.get("start")
    end = masked.get("end")
    if isinstance(start, int) and isinstance(end, int) and 0 <= start < end:
        offset_tuple = (start, end, marker)

    return masked, discovered_raw_values, offset_tuple


def _apply_offsets_and_substrings_to_text(
    text: str,
    offsets: list[tuple[int, int, str]],
    raw_substrings: list[str],
) -> str:
    """Replace PII spans in source text using offsets (if valid) and literal substrings."""
    if not isinstance(text, str) or not text:
        return text

    updated = text
    # First, if offsets match the original text length, replace from right to left
    valid_offsets = [
        (s, e, m) for (s, e, m) in offsets if 0 <= s < e <= len(updated)
    ]
    if valid_offsets:
        # Sort descending by start offset to avoid shifting earlier indices
        for start, end, marker in sorted(valid_offsets, key=lambda x: x[0], reverse=True):
            segment = updated[start:end]
            if not segment.startswith("[REDACTED_PII:"):
                raw_substrings.append(segment)
                updated = updated[:start] + marker + updated[end:]

    # Also scrub any remaining occurrences of discovered raw PII strings
    for raw in sorted(set(raw_substrings), key=len, reverse=True):
        if raw and raw in updated and not raw.startswith("[REDACTED_PII:"):
            updated = updated.replace(raw, "[REDACTED_PII]")

    return updated


def mask_ai_redact_detect_spans(payload: Any, *, force_detect_context: bool = False) -> Any:
    """Mask PII spans emitted by AI_REDACT in `detect` mode ([G-A2]).

    Preserves structural metadata (`category`, `start`, `end`, `confidence`) while
    replacing raw PII values in both the span entries and any sibling text fields.
    """
    if isinstance(payload, list):
        if payload and all(_is_span_dict(item) for item in payload):
            masked_list = []
            for item in payload:
                masked_item, _, _ = _mask_single_span_dict(item)
                masked_list.append(masked_item)
            return masked_list
        return [mask_ai_redact_detect_spans(item, force_detect_context=force_detect_context) for item in payload]

    if not isinstance(payload, dict):
        return payload

    result = copy.deepcopy(payload)
    mode_val = str(result.get("mode") or result.get("redact_mode") or "").strip().lower()
    func_val = str(result.get("function") or result.get("ai_function") or "").strip().upper()
    is_detect = (
        force_detect_context
        or mode_val == "detect"
        or (func_val == "AI_REDACT" and mode_val != "redact")
    )

    collected_raw_pii: list[str] = []
    collected_offsets: list[tuple[int, int, str]] = []

    for key, val in list(result.items()):
        norm_key = str(key).strip().lower()
        if norm_key in _AI_REDACT_SPAN_LIST_KEYS and isinstance(val, list):
            new_spans = []
            for elem in val:
                if _is_span_dict(elem):
                    masked_span, raws, offset = _mask_single_span_dict(elem)
                    new_spans.append(masked_span)
                    collected_raw_pii.extend(raws)
                    if offset is not None:
                        collected_offsets.append(offset)
                else:
                    new_spans.append(
                        mask_ai_redact_detect_spans(elem, force_detect_context=is_detect)
                    )
            result[key] = new_spans
        elif _is_span_dict(val) and (is_detect or norm_key in {"span", "finding", "entity"}):
            masked_span, raws, offset = _mask_single_span_dict(val)
            result[key] = masked_span
            collected_raw_pii.extend(raws)
            if offset is not None:
                collected_offsets.append(offset)
        elif isinstance(val, (dict, list)):
            result[key] = mask_ai_redact_detect_spans(val, force_detect_context=is_detect)

    # If this object held detected PII spans, scrub sibling text fields (e.g. input_text, raw_text)
    if collected_raw_pii or collected_offsets:
        for key, val in list(result.items()):
            if isinstance(val, str) and str(key).strip().lower() not in {"mode", "function", "ai_function", "status"}:
                result[key] = _apply_offsets_and_substrings_to_text(
                    val, collected_offsets, collected_raw_pii
                )

    return result


def redact_payload_for_sink(payload: Any, *, context: dict[str, Any] | None = None) -> Any:
    """Deterministically redact secrets, headers, replay state, and AI_REDACT detect spans."""
    force_detect = False
    if isinstance(context, dict):
        ctx_mode = str(context.get("mode") or context.get("ai_redact_mode") or "").strip().lower()
        ctx_fn = str(context.get("function") or context.get("ai_function") or "").strip().upper()
        if ctx_mode == "detect" or (ctx_fn == "AI_REDACT" and ctx_mode == "detect"):
            force_detect = True

    stage1 = mask_ai_redact_detect_spans(payload, force_detect_context=force_detect)
    return _redact_recursive(stage1)


def _redact_recursive(value: Any) -> Any:
    if isinstance(value, str):
        return redact_text_secrets(value)

    if isinstance(value, list):
        return [_redact_recursive(item) for item in value]

    if isinstance(value, tuple):
        return [_redact_recursive(item) for item in value]

    if not isinstance(value, dict):
        return value

    redacted: dict[str, Any] = {}
    block_type = str(value.get("type") or "").strip().lower()

    for key, val in value.items():
        norm_key = str(key).strip().lower()

        if norm_key == "access_roles":
            redacted[key] = REDACTED_ACCESS_ROLES
            continue

        if norm_key in _SENSITIVE_HEADER_KEYS:
            redacted[key] = REDACTED_HEADER
            continue

        if norm_key in _REPLAY_SIGNATURE_KEYS:
            redacted[key] = REDACTED_REPLAY_STATE
            continue

        # Handle Anthropic redacted_thinking block data payload
        if block_type == "redacted_thinking" and norm_key == "data":
            redacted[key] = REDACTED_REPLAY_STATE
            continue

        if norm_key in _SENSITIVE_SECRET_KEYS:
            redacted[key] = REDACTED_SECRET
            continue

        redacted[key] = _redact_recursive(val)

    return redacted
