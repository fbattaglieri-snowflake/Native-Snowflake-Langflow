"""Typed Snowflake agent memories: persistent metadata, write statements and cleanup statements.

A typed memory is one Snowflake table created from the Memories page. Its configuration travels with
the table as a JSON ``COMMENT``, so the memory is rediscovered from Snowflake after a restart.
"""

from __future__ import annotations

import json
import re
from typing import Any

MARKER = "lf_typed_memory"
TABLE_PREFIX = "MEM_"
CLEANUP_SUFFIX = "_CLEANUP"

CHECKPOINTER = "graph_state_checkpointer"
SUMMARY = "summary_memory"
PROFILE = "user_profile_entity_memory"
EPISODIC = "episodic_memory"
WRITABLE_KINDS = (CHECKPOINTER, SUMMARY, PROFILE, EPISODIC)

DEFAULT_CAPTURE_INSTRUCTIONS = {
    EPISODIC: (
        "Save an episode when a task is finished and its outcome is known: what was asked, which tool or "
        "approach was used, and whether it worked. Do not save small talk or unconfirmed guesses."
    ),
    CHECKPOINTER: "Save the intermediate state of the run after each completed step so it can be resumed.",
    SUMMARY: "Save the updated running summary of the conversation.",
    PROFILE: "Save stable facts and preferences about the user or entity.",
}

REQUIRED_COLUMNS = {
    CHECKPOINTER: ("thread_id", "checkpoint_ns", "step_id", "state_blob", "created_at"),
    SUMMARY: ("session_id", "running_summary", "last_turn_id", "updated_at"),
    PROFILE: ("user_id", "entity_namespace", "profile_json", "created_at"),
    EPISODIC: ("chunk_id", "topic", "tool_used", "outcome_score", "episode_text", "created_at"),
}

_META_KEYS = (
    "name",
    "memory_id",
    "canonical_memory_type",
    "capture_instructions",
    "retention_days",
    "max_rows",
    "min_outcome_score",
    "keep_last_steps",
    "ttl_hours",
    "few_shot_k",
    "outcome_score_threshold",
    "enable_embedding",
    "embedding_model",
    "dimension",
    "cleanup_task",
    "cleanup_schedule",
    "managed",
    "flow_id",
    "created_by_role",
)
_MAX_INSTRUCTIONS = 1500
_STOP_WORDS = frozenset(
    {"that", "this", "with", "from", "have", "what", "when", "were", "which", "about", "does", "into", "your"}
)


class TypedMemoryError(ValueError):
    """Raised when a typed memory request cannot be turned into a statement."""


def cleanup_task_fqn(table_fqn: str) -> str:
    db, sch, tbl = table_fqn.split(".")
    return f"{db}.{sch}.{tbl[:240]}{CLEANUP_SUFFIX}"


def sql_literal(text: str) -> str:
    return "'" + str(text).replace("\\", "\\\\").replace("'", "''") + "'"


def build_comment(meta: dict[str, Any]) -> str:
    """Serialize the memory configuration stored on the table."""
    out: dict[str, Any] = {MARKER: 1}
    for key in _META_KEYS:
        val = meta.get(key)
        if val is None or val == "":
            continue
        if key == "capture_instructions":
            val = str(val)[:_MAX_INSTRUCTIONS]
        out[key] = val
    return json.dumps(out, separators=(",", ":"), ensure_ascii=True)


def parse_comment(comment: Any) -> dict[str, Any] | None:
    """Return the memory configuration when ``comment`` was written by ``build_comment``."""
    text = str(comment or "").strip()
    if not text.startswith("{") or MARKER not in text:
        return None
    try:
        meta = json.loads(text)
    except ValueError:
        return None
    if not isinstance(meta, dict) or not meta.get(MARKER):
        return None
    if meta.get("canonical_memory_type") not in WRITABLE_KINDS:
        return None
    return meta


def entry_from_table(database: str, schema: str, table: str, meta: dict[str, Any], *, is_hybrid: bool = False) -> dict[str, Any]:
    """Registry entry for a typed memory discovered in Snowflake."""
    kind = str(meta["canonical_memory_type"])
    name = str(meta.get("name") or table.removeprefix(TABLE_PREFIX))
    entry = {k: v for k, v in meta.items() if k != MARKER}
    entry.update(
        {
            "name": name,
            "kb_name": name,
            "database": database,
            "schema": schema,
            "table_fqn": f"{database}.{schema}.{table}",
            "storage_family": "snowflake_sql",
            "agent_memory_type": kind,
            "canonical_memory_type": kind,
            "table_type": "HYBRID" if is_hybrid else "STANDARD",
            "vector_database": "Hybrid Table" if is_hybrid else "Standard Table",
            "enable_embedding": bool(meta.get("enable_embedding")),
            "is_vector_memory": bool(meta.get("enable_embedding")),
            "snowflake_created": True,
            "discovered_from_snowflake": True,
            "storage_mode": "create_physical" if meta.get("managed", True) else "bind_existing",
        }
    )
    if entry.get("memory_id"):
        entry["id"] = entry["langflow_memory_id"] = entry["memory_id"]
    return entry


def _json_text(value: Any, *, wrap_key: str = "text") -> str:
    """Return valid JSON text; plain text is wrapped so ``PARSE_JSON`` never fails."""
    if isinstance(value, (dict, list)):
        return json.dumps(value, default=str)
    text = str(value if value is not None else "").strip()
    if not text:
        return "{}"
    try:
        parsed = json.loads(text)
    except ValueError:
        return json.dumps({wrap_key: text})
    if isinstance(parsed, (dict, list)):
        return text
    return json.dumps({wrap_key: parsed})


class _Binds:
    def __init__(self) -> None:
        self.items: list[tuple[str, Any]] = []

    def add(self, value: Any, typ: str = "TEXT") -> str:
        self.items.append((typ, value))
        return "?"

    def to_api(self) -> dict[str, dict[str, Any]]:
        return {str(i + 1): {"type": t, "value": str(v)} for i, (t, v) in enumerate(self.items)}


def build_write_request(
    kind: str,
    table_fqn: str,
    *,
    memory_name: str,
    role: str,
    content: Any,
    thread_id: str = "default",
    checkpoint_ns: str = "default",
    ttl_hours: int = 24,
    session_id: str = "default",
    last_turn_id: str = "",
    user_id: str = "default",
    entity_namespace: str = "default",
    topic: str = "general",
    tool_used: str = "",
    outcome_score: float = 1.0,
    metadata: Any = None,
    embed_model_literal: str = "",
    embed_function: str = "AI_EMBED",
) -> tuple[str, dict[str, dict[str, Any]]]:
    """Build the single statement that stores ``content`` in a typed memory table."""
    b = _Binds()
    meta_json = _json_text(metadata or {})
    if kind == CHECKPOINTER:
        t_id, ns = str(thread_id or "default"), str(checkpoint_ns or "default")
        sql = (
            f"INSERT INTO {table_fqn} (thread_id, checkpoint_ns, step_id, memory_name, state_blob, ttl_hours, "
            f"metadata, created_by_role) SELECT {b.add(t_id)}, {b.add(ns)}, "
            f"COALESCE((SELECT MAX(step_id) FROM {table_fqn} WHERE thread_id = {b.add(t_id)} "
            f"AND checkpoint_ns = {b.add(ns)}), 0) + 1, {b.add(memory_name)}, "
            f"PARSE_JSON({b.add(_json_text(content, wrap_key='value'))}), {b.add(int(ttl_hours or 24), 'FIXED')}, "
            f"PARSE_JSON({b.add(meta_json)}), {b.add(role)}"
        )
    elif kind == SUMMARY:
        text = str(content or "").strip()
        if not text:
            raise TypedMemoryError("A summary memory needs non-empty text to save.")
        sql = (
            f"MERGE INTO {table_fqn} t USING (SELECT {b.add(str(session_id or 'default'))} AS session_id, "
            f"{b.add(text)} AS running_summary, {b.add(str(last_turn_id or ''))} AS last_turn_id, "
            f"{b.add(memory_name)} AS memory_name, {b.add(role)} AS created_by_role) s "
            f"ON t.session_id = s.session_id "
            f"WHEN MATCHED THEN UPDATE SET running_summary = s.running_summary, last_turn_id = s.last_turn_id, "
            f"updated_at = CURRENT_TIMESTAMP() "
            f"WHEN NOT MATCHED THEN INSERT (session_id, memory_name, running_summary, last_turn_id, created_by_role) "
            f"VALUES (s.session_id, s.memory_name, s.running_summary, s.last_turn_id, s.created_by_role)"
        )
    elif kind == PROFILE:
        sql = (
            f"MERGE INTO {table_fqn} t USING (SELECT {b.add(str(user_id or 'default'))} AS user_id, "
            f"{b.add(str(entity_namespace or 'default'))} AS entity_namespace, "
            f"PARSE_JSON({b.add(_json_text(content, wrap_key='note'))}) AS profile_json, "
            f"{b.add(memory_name)} AS memory_name, {b.add(role)} AS created_by_role) s "
            f"ON t.user_id = s.user_id AND t.entity_namespace = s.entity_namespace "
            f"WHEN MATCHED THEN UPDATE SET profile_json = s.profile_json, created_at = CURRENT_TIMESTAMP() "
            f"WHEN NOT MATCHED THEN INSERT (chunk_id, user_id, entity_namespace, memory_name, profile_json, "
            f"created_by_role) VALUES (UUID_STRING(), s.user_id, s.entity_namespace, s.memory_name, "
            f"s.profile_json, s.created_by_role)"
        )
    elif kind == EPISODIC:
        text = str(content or "").strip()
        if not text:
            raise TypedMemoryError("An episodic memory needs non-empty episode text to save.")
        score = max(0.0, min(float(outcome_score if outcome_score is not None else 1.0), 1.0))
        cols = "chunk_id, user_id, memory_name, topic, tool_used, outcome_score, episode_text, text_chunk, metadata, created_by_role"
        vals = (
            f"UUID_STRING(), {b.add(str(user_id or 'default'))}, {b.add(memory_name)}, "
            f"{b.add(str(topic or 'general')[:256])}, {b.add(str(tool_used or '')[:256])}, {b.add(score, 'REAL')}, "
            f"{b.add(text)}, {b.add(text)}, PARSE_JSON({b.add(meta_json)}), {b.add(role)}"
        )
        if embed_model_literal:
            cols += ", embedding"
            vals += f", {embed_function}({embed_model_literal}, {b.add(text)})"
        sql = f"INSERT INTO {table_fqn} ({cols}) SELECT {vals}"
    else:
        raise TypedMemoryError(f"Memory type '{kind}' cannot be written from a flow.")
    return sql, b.to_api()


def is_scoped_key(key: Any) -> bool:
    """True when ``key`` names a specific owner; empty and ``default`` mean the shared scope."""
    return str(key or "").strip().lower() not in ("", "default")


def keyword_terms(text: str, *, limit: int = 5) -> list[str]:
    """Distinct search words of a free-text query, longest first."""
    words = {w.lower() for w in re.findall(r"[A-Za-z0-9_]{4,}", str(text or ""))} - _STOP_WORDS
    return sorted(words, key=lambda w: (-len(w), w))[:limit]


def build_episodic_recall(
    table_fqn: str,
    *,
    query: str = "",
    topic: str = "",
    min_score: float | None = None,
    limit: int = 3,
    user_id: str = "",
) -> tuple[str, dict[str, dict[str, Any]]]:
    """Recall the best past episodes by topic and keywords, highest outcome first.

    ``user_id`` scopes the recall to the episodes saved under that key; empty or ``default`` reads them all.
    """
    b = _Binds()
    conds: list[str] = []
    if is_scoped_key(user_id):
        conds.append(f"user_id = {b.add(str(user_id).strip())}")
    clean_topic = str(topic or "").strip()
    if clean_topic and clean_topic.lower() != "general" and clean_topic != str(query or "").strip():
        conds.append(f"topic ILIKE {b.add('%' + clean_topic + '%')}")
    terms = keyword_terms(query)
    if terms:
        ors = " OR ".join(
            f"episode_text ILIKE {b.add('%' + t + '%')} OR topic ILIKE {b.add('%' + t + '%')}" for t in terms
        )
        conds.append(f"({ors})")
    if min_score is not None:
        conds.append(f"outcome_score >= {b.add(float(min_score), 'REAL')}")
    where = f" WHERE {' AND '.join(conds)}" if conds else ""
    sql = (
        f"SELECT topic, tool_used, outcome_score, episode_text, created_at FROM {table_fqn}{where} "
        f"ORDER BY outcome_score DESC, created_at DESC LIMIT {max(1, min(int(limit or 3), 200))}"
    )
    return sql, b.to_api()


def build_cleanup_statement(kind: str, table_fqn: str, policy: dict[str, Any]) -> str | None:
    """Return the single ``DELETE`` that enforces the retention policy, or None when there is no policy.

    A row cap is ranked in a ``USING`` subquery: Snowflake rejects ``IN (subquery)`` combined with ``OR``.
    """
    conds: list[str] = []
    using = ""
    join = ""
    days = int(policy.get("retention_days") or 0)
    if kind == CHECKPOINTER:
        conds.append("t.created_at < DATEADD(hour, -COALESCE(t.ttl_hours, 24), CURRENT_TIMESTAMP())")
        keep = int(policy.get("keep_last_steps") or 0)
        if keep > 0:
            using = (
                f" USING (SELECT thread_id, checkpoint_ns, step_id, ROW_NUMBER() OVER "
                f"(PARTITION BY thread_id, checkpoint_ns ORDER BY step_id DESC) AS rn FROM {table_fqn}) r"
            )
            join = "t.thread_id = r.thread_id AND t.checkpoint_ns = r.checkpoint_ns AND t.step_id = r.step_id"
            conds.append(f"r.rn > {keep}")
    elif kind == EPISODIC:
        if days > 0:
            conds.append(f"t.created_at < DATEADD(day, -{days}, CURRENT_TIMESTAMP())")
        min_score = policy.get("min_outcome_score")
        if min_score is not None and float(min_score) > 0:
            conds.append(f"t.outcome_score < {float(min_score)}")
        max_rows = int(policy.get("max_rows") or 0)
        if max_rows > 0:
            using = (
                f" USING (SELECT chunk_id, ROW_NUMBER() OVER (ORDER BY outcome_score DESC, created_at DESC) AS rn "
                f"FROM {table_fqn}) r"
            )
            join = "t.chunk_id = r.chunk_id"
            conds.append(f"r.rn > {max_rows}")
    elif kind == SUMMARY:
        if days > 0:
            conds.append(f"t.updated_at < DATEADD(day, -{days}, CURRENT_TIMESTAMP())")
    elif kind == PROFILE:
        if days > 0:
            conds.append(f"t.created_at < DATEADD(day, -{days}, CURRENT_TIMESTAMP())")
    if not conds:
        return None
    where = " OR ".join(conds)
    if join:
        where = f"{join} AND ({where})"
    return f"DELETE FROM {table_fqn} t{using} WHERE {where}"


_CRON_RE = re.compile(r"^[0-9*/,\- ]{9,40}$")


def build_cleanup_task_ddl(table_fqn: str, statement: str, *, warehouse: str = "", cron: str = "0 3 * * *") -> str:
    """``CREATE OR REPLACE TASK`` running the cleanup statement on a schedule (serverless without a warehouse)."""
    clean_cron = " ".join(str(cron or "0 3 * * *").split())
    if not _CRON_RE.match(clean_cron) or len(clean_cron.split(" ")) != 5:
        raise TypedMemoryError("Cleanup schedule must be a 5-field cron expression, for example '0 3 * * *'.")
    wh = f" WAREHOUSE = {warehouse}" if warehouse else ""
    return (
        f"CREATE OR REPLACE TASK {cleanup_task_fqn(table_fqn)}{wh} "
        f"SCHEDULE = 'USING CRON {clean_cron} UTC' AS {statement}"
    )


def build_browse_statement(kind: str, table_fqn: str, *, limit: int = 200) -> str:
    """Latest rows of a typed memory as (session_key, label, body, ts, outcome_score) for the Memories page."""
    key, label, body, ts_col, score = {
        CHECKPOINTER: ("thread_id", "'Step ' || step_id", "TO_VARCHAR(state_blob)", "created_at", "NULL"),
        SUMMARY: ("session_id", "'Summary'", "running_summary", "updated_at", "NULL"),
        PROFILE: ("user_id", "entity_namespace", "TO_VARCHAR(profile_json)", "created_at", "NULL"),
        EPISODIC: ("user_id", "COALESCE(topic, 'episode')", "episode_text", "created_at", "outcome_score"),
    }[kind]
    return (
        f"SELECT {key} AS session_key, {label} AS label, {body} AS body, "
        f"TO_VARCHAR({ts_col}, 'YYYY-MM-DD\"T\"HH24:MI:SS.FF3TZH:TZM') AS ts, {score} AS outcome_score "
        f"FROM {table_fqn} ORDER BY {ts_col} DESC LIMIT {max(1, min(int(limit or 200), 1000))}"
    )
