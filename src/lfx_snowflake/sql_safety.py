"""Shared SQL safety helpers: identifier validation/quoting, enum allowlists, SQL API bindings.

Single source of truth for building Snowflake SQL from untrusted input:

- Identifiers (``validate_identifier``, ``quote_identifier``, ``validate_fqn``, ``quote_fqn``,
  ``validate_stage_ref``) only accept the unquoted Snowflake grammar
  ``^[A-Za-z_][A-Za-z0-9_$]{0,254}$`` per part and are upper-cased, so a validated name can be
  emitted double-quoted without changing its resolution.
- Enumerated keywords (``validate_enum``) must come from a closed allowlist.
- Values never go into SQL text: ``BindingBuilder`` / ``to_sql_bindings`` build the
  ``/api/v2/statements`` ``bindings`` object (``{"1": {"type": "TEXT", "value": "..."}}``), where every
  value is serialized as a string and the type is one of the documented SQL API binding types.
- Literals that Snowflake requires as string constants (e.g. the ``AI_EMBED`` model argument) are
  emitted with ``sql_string_literal`` only after an allowlist check.
"""

from __future__ import annotations

import datetime as _dt
import json
import re
from decimal import Decimal
from typing import Any, Iterable

IDENTIFIER_PATTERN = r"[A-Za-z_][A-Za-z0-9_$]{0,254}"
IDENTIFIER_RE = re.compile(rf"^{IDENTIFIER_PATTERN}$")

# Documented SQL API binding types (/api/v2/statements "bindings"). DATE/TIME/TIMESTAMP_* expect
# epoch-based values (an ISO-8601 string is rejected with 002086), so date/datetime objects are
# bound as TEXT and Snowflake casts them on use.
SQL_API_BINDING_TYPES = frozenset(
    {
        "FIXED",
        "DECFLOAT",
        "REAL",
        "TEXT",
        "BINARY",
        "BOOLEAN",
        "DATE",
        "TIME",
        "TIMESTAMP_LTZ",
        "TIMESTAMP_NTZ",
        "TIMESTAMP_TZ",
    }
)

# Single source of truth for AI_EMBED (BUG-MEM-EMB-01/02), verified against
# https://docs.snowflake.com/en/sql-reference/functions/ai_embed and live ARRAY_SIZE(AI_EMBED(...)).
AI_EMBED_TEXT_MODEL_DIMENSIONS: dict[str, int] = {
    "snowflake-arctic-embed-m-v1.5": 768,
    "snowflake-arctic-embed-m": 768,
    "e5-base-v2": 768,
    "snowflake-arctic-embed-l-v2.0": 1024,
    "snowflake-arctic-embed-l-v2.0-8k": 1024,
    "multilingual-e5-large": 1024,
    "voyage-multilingual-2": 1024,
    "nv-embed-qa-4": 1024,
}
AI_EMBED_TEXT_MODELS = frozenset(AI_EMBED_TEXT_MODEL_DIMENSIONS)
# Image-only AI_EMBED model (FILE input); never offered as a text embedder.
AI_EMBED_IMAGE_MODEL_DIMENSIONS: dict[str, int] = {"voyage-multimodal-3": 1024}
AI_EMBED_MODELS = frozenset(AI_EMBED_TEXT_MODELS | set(AI_EMBED_IMAGE_MODEL_DIMENSIONS))
AI_MULTI_EMBED_MODELS = frozenset({"twelvelabs-marengo-embed-3-0"})

# Argument types accepted in routine signatures (DESCRIBE FUNCTION/PROCEDURE ... (<types>)).
ROUTINE_ARG_TYPES = frozenset(
    {
        "NUMBER", "DECIMAL", "NUMERIC", "INT", "INTEGER", "BIGINT", "SMALLINT", "TINYINT", "BYTEINT",
        "FLOAT", "FLOAT4", "FLOAT8", "DOUBLE", "DOUBLE PRECISION", "REAL",
        "VARCHAR", "CHAR", "CHARACTER", "STRING", "TEXT", "BINARY", "VARBINARY",
        "BOOLEAN", "DATE", "TIME", "DATETIME", "TIMESTAMP", "TIMESTAMP_LTZ", "TIMESTAMP_NTZ", "TIMESTAMP_TZ",
        "VARIANT", "OBJECT", "ARRAY", "GEOGRAPHY", "GEOMETRY", "VECTOR", "FILE", "MAP",
    }
)
_ROUTINE_ARG_TYPE_RE = re.compile(r"^([A-Z_ ]+?)\s*(\(\s*[A-Z0-9_ ,]{0,40}\s*\))?$")


class SqlSafetyError(ValueError):
    """Raised when an identifier, enum or value is not safe to place in SQL."""

    def __init__(self, message: str, field_name: str = "") -> None:
        super().__init__(message)
        self.field_name = field_name


def is_valid_identifier(value: Any) -> bool:
    return isinstance(value, str) and bool(IDENTIFIER_RE.match(value.strip()))


def validate_identifier(value: Any, field_name: str = "identifier") -> str:
    """Upper-cased plain identifier, else ``SqlSafetyError`` (quoted or exotic names are rejected)."""
    cleaned = (value if isinstance(value, str) else "").strip()
    if not IDENTIFIER_RE.match(cleaned):
        raise SqlSafetyError(f"Invalid Snowflake identifier for {field_name}: {value!r}", field_name)
    return cleaned.upper()


def quote_identifier(value: Any, field_name: str = "identifier") -> str:
    return f'"{validate_identifier(value, field_name)}"'


def validate_fqn(value: Any, field_name: str = "object", *, min_parts: int = 1, max_parts: int = 3) -> str:
    """Validated dotted name (``DB.SCH.OBJ``) upper-cased; each part must be a plain identifier."""
    raw = (value if isinstance(value, str) else "").strip()
    parts = raw.split(".") if raw else []
    if not (min_parts <= len(parts) <= max_parts):
        raise SqlSafetyError(
            f"Invalid Snowflake object name for {field_name}: expected {min_parts}-{max_parts} parts, got {value!r}",
            field_name,
        )
    return ".".join(validate_identifier(p, field_name) for p in parts)


def quote_fqn(value: Any, field_name: str = "object", *, min_parts: int = 1, max_parts: int = 3) -> str:
    return ".".join(f'"{p}"' for p in validate_fqn(value, field_name, min_parts=min_parts, max_parts=max_parts).split("."))


def join_fqn(*parts: Any, field_name: str = "object") -> str:
    """Validate each part separately and join them (``DB``, ``SCH``, ``OBJ`` -> ``DB.SCH.OBJ``)."""
    return ".".join(validate_identifier(p, field_name) for p in parts)


_STAGE_PATH_RE = re.compile(r"^[A-Za-z0-9_\-./=]*$")


def validate_stage_ref(value: Any, field_name: str = "stage", *, allow_path: bool = False) -> str:
    """``@DB.SCH.STAGE[/path]`` -> ``@DB.SCH.STAGE[/path]`` with validated name parts.

    User (``@~``) and table (``@%T``) stages are rejected; the optional path may only contain
    ``[A-Za-z0-9_-./=]`` and no ``..`` segment.
    """
    raw = (value if isinstance(value, str) else "").strip()
    body = raw[1:] if raw.startswith("@") else raw
    name, _, path = body.partition("/")
    fqn = validate_fqn(name, field_name, min_parts=1, max_parts=3)
    if path:
        if not allow_path:
            raise SqlSafetyError(f"Stage path not allowed for {field_name}: {value!r}", field_name)
        if not _STAGE_PATH_RE.match(path) or any(seg == ".." for seg in path.split("/")):
            raise SqlSafetyError(f"Invalid stage path for {field_name}: {value!r}", field_name)
        return f"@{fqn}/{path}"
    return f"@{fqn}"


def validate_enum(value: Any, allowed: Iterable[str], field_name: str = "value", *, case_insensitive: bool = True) -> str:
    """Return the canonical allowlist member matching ``value``, else ``SqlSafetyError``."""
    cleaned = (value if isinstance(value, str) else str(value if value is not None else "")).strip()
    for option in allowed:
        if cleaned == option or (case_insensitive and cleaned.upper() == option.upper()):
            return option
    raise SqlSafetyError(f"Unsupported value for {field_name}: {value!r}", field_name)


def sql_string_literal(value: str) -> str:
    """Single-quoted SQL string constant. Use only for allowlisted values that must be literals."""
    if not isinstance(value, str) or "\x00" in value:
        raise SqlSafetyError("Invalid string literal.")
    return "'" + value.replace("\\", "\\\\").replace("'", "''") + "'"


def validate_embed_model(model: Any, *, multi: bool = False) -> str:
    """Allowlisted AI_EMBED / AI_MULTI_EMBED model name (raises ``SqlSafetyError``)."""
    name = (model if isinstance(model, str) else "").strip().split(" ")[0]
    allowed = AI_MULTI_EMBED_MODELS if multi else AI_EMBED_MODELS
    if name not in allowed:
        raise SqlSafetyError(f"Unsupported embedding model: {model!r}", "embedding_model")
    return name


def validate_routine_arg_type(value: Any, field_name: str = "argument type") -> str:
    """Upper-cased Snowflake data type from the routine signature allowlist (``VARCHAR(16)`` ok)."""
    cleaned = re.sub(r"\s+", " ", (value if isinstance(value, str) else "").strip().upper())
    m = _ROUTINE_ARG_TYPE_RE.match(cleaned)
    if m and m.group(1).strip() in ROUTINE_ARG_TYPES:
        return cleaned
    # ``<arg_name> <TYPE>`` form (e.g. ``PAYLOAD OBJECT``): drop the validated name, keep the type.
    name, _, rest = cleaned.partition(" ")
    if rest and IDENTIFIER_RE.match(name):
        m = _ROUTINE_ARG_TYPE_RE.match(rest)
        if m and m.group(1).strip() in ROUTINE_ARG_TYPES:
            return rest
    raise SqlSafetyError(f"Unsupported {field_name}: {value!r}", field_name)


def validate_routine_signature(types_part: Any) -> str:
    """``"(VARCHAR, NUMBER)"`` -> normalized ``"(VARCHAR, NUMBER)"``; empty -> ``""``."""
    raw = (types_part if isinstance(types_part, str) else "").strip()
    if not raw:
        return ""
    if not (raw.startswith("(") and raw.endswith(")")):
        raise SqlSafetyError(f"Invalid routine signature: {types_part!r}", "signature")
    inner = raw[1:-1].strip()
    if not inner:
        return "()"
    # Split on commas that are not inside a type's own parentheses, e.g. NUMBER(38, 0).
    parts, depth, cur = [], 0, ""
    for ch in inner:
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
            if depth < 0:
                raise SqlSafetyError(f"Invalid routine signature: {types_part!r}", "signature")
        if ch == "," and depth == 0:
            parts.append(cur)
            cur = ""
        else:
            cur += ch
    parts.append(cur)
    return "(" + ", ".join(validate_routine_arg_type(p) for p in parts) + ")"


def binding_for(value: Any, typ: str | None = None) -> dict[str, Any]:
    """One SQL API binding entry. Values are serialized as strings; ``None`` -> SQL NULL."""
    if typ is not None:
        typ = typ.upper()
        if typ not in SQL_API_BINDING_TYPES:
            raise SqlSafetyError(f"Unsupported SQL API binding type: {typ!r}", "binding")
    if value is None:
        return {"type": typ or "TEXT", "value": None}
    if typ is None:
        if isinstance(value, bool):
            typ = "BOOLEAN"
        elif isinstance(value, int):
            typ = "FIXED"
        elif isinstance(value, (float, Decimal)):
            typ = "REAL" if isinstance(value, float) else "FIXED"
        else:
            typ = "TEXT"
    if isinstance(value, bool):
        text = "true" if value else "false"
    elif isinstance(value, (dict, list)):
        text = json.dumps(value, default=str)
    elif isinstance(value, (_dt.datetime, _dt.date, _dt.time)):
        if typ in ("DATE", "TIME", "TIMESTAMP_LTZ", "TIMESTAMP_NTZ", "TIMESTAMP_TZ"):
            raise SqlSafetyError(
                f"{typ} bindings take epoch values; bind date/time objects as TEXT (ISO-8601).", "binding"
            )
        text = value.isoformat()
    else:
        text = str(value)
    return {"type": typ, "value": text}


def to_sql_bindings(values: Iterable[Any]) -> dict[str, dict[str, Any]]:
    """Positional ``?`` bindings: ``[a, b]`` -> ``{"1": {...}, "2": {...}}``.

    Items may be raw values (type inferred) or ``(type, value)`` tuples.
    """
    out: dict[str, dict[str, Any]] = {}
    for i, item in enumerate(values, start=1):
        if isinstance(item, tuple) and len(item) == 2 and isinstance(item[0], str) and item[0].upper() in SQL_API_BINDING_TYPES:
            out[str(i)] = binding_for(item[1], item[0])
        else:
            out[str(i)] = binding_for(item)
    return out


class BindingBuilder:
    """Collects ``?`` placeholders while composing SQL: ``b = BindingBuilder(); f"x = {b.add(v)}"``."""

    def __init__(self) -> None:
        self._entries: list[dict[str, Any]] = []

    def add(self, value: Any, typ: str | None = None) -> str:
        self._entries.append(binding_for(value, typ))
        return "?"

    def __len__(self) -> int:
        return len(self._entries)

    @property
    def bindings(self) -> dict[str, dict[str, Any]] | None:
        return {str(i): e for i, e in enumerate(self._entries, start=1)} or None


def bounded_int(value: Any, default: int, lo: int, hi: int) -> int:
    """Integer clamp for LIMIT/TOP clauses (never interpolates the raw input)."""
    try:
        n = int(value)
    except (TypeError, ValueError):
        n = default
    return max(lo, min(hi, n))


# ---------------------------------------------------------------------------
# Structured filters (Data Source components: never execute free SQL text)
# ---------------------------------------------------------------------------
FILTER_OPERATORS = ("<=", ">=", "<>", "!=", "=", "<", ">", "NOT ILIKE", "NOT LIKE", "ILIKE", "LIKE")
_PRED_RE = re.compile(
    rf"^\s*({IDENTIFIER_PATTERN})\s*(<=|>=|<>|!=|=|<|>|\bNOT\s+ILIKE\b|\bNOT\s+LIKE\b|\bILIKE\b|\bLIKE\b)\s*"
    r"('(?:[^'\\]|'')*'|-?\d+(?:\.\d+)?|TRUE|FALSE)\s*$",
    re.IGNORECASE,
)
_NULL_PRED_RE = re.compile(rf"^\s*({IDENTIFIER_PATTERN})\s+IS\s+(NOT\s+)?NULL\s*$", re.IGNORECASE)
_AND_SPLIT_RE = re.compile(r"\s+AND\s+(?=(?:[^']*'[^']*')*[^']*$)", re.IGNORECASE)


def _literal_to_value(token: str) -> Any:
    up = token.upper()
    if up in ("TRUE", "FALSE"):
        return up == "TRUE"
    if token.startswith("'"):
        return token[1:-1].replace("''", "'")
    return float(token) if "." in token else int(token)


def build_filter_predicate(filter_input: Any, binder: BindingBuilder) -> str:
    """Predicate text with ``?`` placeholders from a structured filter.

    Accepted forms: a JSON object ``{"COL": value | [values] | null}`` (equality / IN / IS NULL), or
    ``COL <op> <literal> [AND ...]`` where ``<op>`` is a comparison or (NOT) (I)LIKE and the literal is
    a single-quoted string, a number, TRUE or FALSE. Anything else (sub-queries, statements, OR,
    functions) raises ``SqlSafetyError``.
    """
    if filter_input is None:
        return ""
    if isinstance(filter_input, str):
        text = filter_input.strip()
        if not text:
            return ""
        if text.startswith("{"):
            try:
                filter_input = json.loads(text)
            except json.JSONDecodeError as exc:
                raise SqlSafetyError("Filter JSON is not valid.", "filter") from exc
        else:
            clauses: list[str] = []
            for part in _AND_SPLIT_RE.split(text):
                m = _NULL_PRED_RE.match(part)
                if m:
                    clauses.append(f'"{validate_identifier(m.group(1), "filter column")}" IS {"NOT " if m.group(2) else ""}NULL')
                    continue
                m = _PRED_RE.match(part)
                if not m:
                    raise SqlSafetyError(
                        "Unsupported filter: use 'COLUMN <op> <literal> [AND ...]' or a JSON object of column values.",
                        "filter",
                    )
                op = re.sub(r"\s+", " ", m.group(2).upper())
                clauses.append(f'"{validate_identifier(m.group(1), "filter column")}" {op} {binder.add(_literal_to_value(m.group(3)))}')
            return " AND ".join(clauses)
    if not isinstance(filter_input, dict):
        raise SqlSafetyError("Filter must be a JSON object of column values.", "filter")
    clauses = []
    for col, val in filter_input.items():
        qcol = f'"{validate_identifier(col, "filter column")}"'
        if val is None:
            clauses.append(f"{qcol} IS NULL")
        elif isinstance(val, list):
            if not val or len(val) > 1000:
                raise SqlSafetyError("Filter IN list must contain 1-1000 values.", "filter")
            clauses.append(f"{qcol} IN ({', '.join(binder.add(v if isinstance(v, (int, float, bool)) else str(v)) for v in val)})")
        elif isinstance(val, (dict,)):
            raise SqlSafetyError("Nested filter objects are not supported.", "filter")
        else:
            clauses.append(f"{qcol} = {binder.add(val if isinstance(val, (int, float, bool)) else str(val))}")
    return " AND ".join(clauses)


def build_filtered_select(
    object_name: Any,
    *,
    database: Any = None,
    schema: Any = None,
    filter_input: Any = None,
    max_rows: Any = 100,
    max_limit: int = 10000,
) -> tuple[str, dict[str, Any] | None, str]:
    """``(sql, bindings, quoted_fqn)`` for ``SELECT * FROM "DB"."SCH"."OBJ" [WHERE <binds>] LIMIT n``."""
    name = (object_name if isinstance(object_name, str) else "").strip()
    if "." in name:
        fqn = quote_fqn(name, "object_name", min_parts=3, max_parts=3)
    else:
        fqn = ".".join(
            quote_identifier(v, f) for v, f in ((database, "database"), (schema, "schema"), (name, "object_name"))
        )
    binder = BindingBuilder()
    predicate = build_filter_predicate(filter_input, binder)
    where = f" WHERE {predicate}" if predicate else ""
    limit = bounded_int(max_rows, 100, 1, max_limit)
    return f"SELECT * FROM {fqn}{where} LIMIT {limit}", binder.bindings, fqn


def inline_bound_predicate(filter_input: Any) -> str:
    """Structured filter (see ``build_filter_predicate``) rendered with safe literals, for legacy
    builders that return plain SQL text. Prefer ``build_filter_predicate`` + bindings for new code."""
    binder = BindingBuilder()
    fragment = build_filter_predicate(filter_input, binder)
    entries = list((binder.bindings or {}).values())
    out, idx = [], 0
    for ch in fragment:
        if ch != "?":
            out.append(ch)
            continue
        entry = entries[idx]
        idx += 1
        val = entry["value"]
        if val is None:
            out.append("NULL")
        elif entry["type"] == "BOOLEAN":
            out.append("TRUE" if val == "true" else "FALSE")
        elif entry["type"] in ("FIXED", "REAL") and re.match(r"^-?\d+(\.\d+)?(e[-+]?\d+)?$", val, re.IGNORECASE):
            out.append(val)
        else:
            out.append(sql_string_literal(val))
    return "".join(out)


__all__ = [
    "inline_bound_predicate",
    "build_filter_predicate",
    "build_filtered_select",
    "AI_EMBED_MODELS",
    "AI_EMBED_TEXT_MODELS",
    "AI_EMBED_TEXT_MODEL_DIMENSIONS",
    "AI_EMBED_IMAGE_MODEL_DIMENSIONS",
    "AI_MULTI_EMBED_MODELS",
    "BindingBuilder",
    "IDENTIFIER_RE",
    "ROUTINE_ARG_TYPES",
    "SQL_API_BINDING_TYPES",
    "SqlSafetyError",
    "binding_for",
    "bounded_int",
    "is_valid_identifier",
    "join_fqn",
    "quote_fqn",
    "quote_identifier",
    "sql_string_literal",
    "to_sql_bindings",
    "validate_embed_model",
    "validate_enum",
    "validate_fqn",
    "validate_identifier",
    "validate_routine_arg_type",
    "validate_routine_signature",
    "validate_stage_ref",
]
