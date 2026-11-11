r"""Runtime enforcement of a flow's `[ ❄️ Context ]` for SQL issued by flow components (BUG-CTX-03).

Flow components wrap their SQL in :func:`flow_component_sql`; ``LiveSnowflakeBridge.execute_sql`` then
checks every Snowflake object the statement references against the authorized context of
``current_snowflake_flow_id`` and fails closed with ``403 AUTHZ_OBJECT_NOT_IN_FLOW_CONTEXT``.

The analyzer is an ALLOWLIST, not a best-effort scanner (PASSO 3 / A6 veto):

* a real lexer: double-quoted identifiers (``""`` escapes), string literals (``\`` and ``''`` escapes),
  ``$$`` bodies, ``--`` / ``//`` / ``/* */`` comments, binds (``?``, ``:name``, ``$1``, ``$var``);
* only a fixed set of statement kinds is accepted; every object position (FROM / JOIN / comma joins /
  parenthesized joins / LATERAL / TABLE() / INTO t(cols) / UPDATE / MERGE / USING / ON <kind> / CLONE /
  LIKE / REFERENCES / CALL / sequences) is resolved to a fully qualified name;
* CTE names shadow tables only inside the scope of the ``WITH`` that declares them (a CTE that references
  its own name is ambiguous and rejected); ``MATCH_RECOGNIZE ... DEFINE x AS (...)`` declares nothing;
* function calls: Snowflake built-ins (``_snowflake_builtin_functions``) and ``SNOWFLAKE.*`` are allowed,
  every other call is a user routine and must be in the context; stage functions need a literal stage;
* anything that cannot be classified with certainty (``TABLE(<literal/bind>)``, ``RESULT_SCAN``,
  ``IDENTIFIER()``, ``SYSTEM$*``, scripting, ``EXECUTE IMMEDIATE``, ``USE``, ``ALTER SESSION``, ``SHOW``
  without ``IN <scope>``, non-SQL routine bodies, governance DDL) raises :class:`UnverifiableStatement`.

Snowflake's ``EXPLAIN`` is not used: it drops references the optimizer elides (e.g. metadata-only
``EXISTS`` subqueries), so it cannot be used for authorization.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from typing import Any

from lfx_snowflake._snowflake_builtin_functions import BUILTIN_FUNCTIONS

AUTHZ_OBJECT_NOT_IN_FLOW_CONTEXT = "AUTHZ_OBJECT_NOT_IN_FLOW_CONTEXT"

# True only while a flow component is issuing its own SQL; bridge-internal SQL never sets it.
FLOW_COMPONENT_SQL: ContextVar[bool] = ContextVar("snowflake_flow_component_sql", default=False)


@contextmanager
def flow_component_sql() -> Iterator[None]:
    """Mark SQL executed inside this block as coming from a flow component."""
    token = FLOW_COMPONENT_SQL.set(True)
    try:
        yield
    finally:
        FLOW_COMPONENT_SQL.reset(token)


class FlowContextDenied(PermissionError):
    """E2E T1: a flow component's SQL was refused by the flow-context guard (Langflow-native component error).

    Raised by component SQL wrappers so the refusal reaches the run output / HTTP response with its code,
    instead of a silent ``ok: false`` result.
    """

    def __init__(self, result: dict[str, Any]):
        self.result = dict(result or {})
        self.error_code = str(self.result.get("error_code") or AUTHZ_OBJECT_NOT_IN_FLOW_CONTEXT)
        super().__init__(f"{self.error_code}: {self.result.get('message') or 'statement outside the flow context'}")


def raise_if_flow_context_denied(result: Any) -> Any:
    """Return ``result`` unchanged unless it is a flow-context guard refusal, which is raised instead."""
    if isinstance(result, dict) and result.get("ok") is False and result.get("error_code") == AUTHZ_OBJECT_NOT_IN_FLOW_CONTEXT:
        raise FlowContextDenied(result)
    return result


class UnverifiableStatement(ValueError):
    """The statement's referenced objects cannot be determined with certainty (fail closed)."""


@dataclass(frozen=True)
class ObjectRef:
    database: str
    schema: str
    name: str
    kind: str = "object"  # object | stage | routine | schema | database
    access: str = "read"  # read | append (INSERT/MERGE/COPY INTO target) | write | ddl

    @property
    def fqn(self) -> str:
        return ".".join(p for p in (self.database, self.schema, self.name) if p)


# --------------------------------------------------------------------------- lexer


@dataclass(frozen=True)
class _Tok:
    kind: str  # id | qid | str | dollar | num | p | stage | bind
    val: str  # id: UPPER; qid: verbatim name; str/dollar: decoded body; p: punctuation; stage: text after '@'

    def is_name(self) -> bool:
        return self.kind in ("id", "qid")

    def kw(self, *words: str) -> bool:
        return self.kind == "id" and self.val in words


_PUNCT2 = ("=>", "::", "||", "<=", ">=", "<>", "!=", "->")
_IDENT_START = set("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz_")
_IDENT_CHARS = _IDENT_START | set("0123456789$")


def _lex(sql: str) -> list[_Tok]:
    out: list[_Tok] = []
    i, n = 0, len(sql)
    while i < n:
        ch = sql[i]
        if ch.isspace():
            i += 1
            continue
        if sql.startswith("--", i) or sql.startswith("//", i):
            j = sql.find("\n", i)
            i = n if j == -1 else j + 1
            continue
        if sql.startswith("/*", i):
            j = sql.find("*/", i + 2)
            if j == -1:
                raise UnverifiableStatement("Unterminated comment.")
            i = j + 2
            continue
        if sql.startswith("$$", i):
            j = sql.find("$$", i + 2)
            if j == -1:
                raise UnverifiableStatement("Unterminated $$ body.")
            out.append(_Tok("dollar", sql[i + 2 : j]))
            i = j + 2
            continue
        if ch == "'":
            j, buf = i + 1, []
            while True:
                if j >= n:
                    raise UnverifiableStatement("Unterminated string literal.")
                c = sql[j]
                if c == "\\":
                    if j + 1 >= n:
                        raise UnverifiableStatement("Unterminated string literal.")
                    buf.append(sql[j + 1])
                    j += 2
                    continue
                if c == "'":
                    if j + 1 < n and sql[j + 1] == "'":
                        buf.append("'")
                        j += 2
                        continue
                    break
                buf.append(c)
                j += 1
            out.append(_Tok("str", "".join(buf)))
            i = j + 1
            continue
        if ch == '"':
            j, buf = i + 1, []
            while True:
                if j >= n:
                    raise UnverifiableStatement("Unterminated quoted identifier.")
                if sql[j] == '"':
                    if j + 1 < n and sql[j + 1] == '"':
                        buf.append('"')
                        j += 2
                        continue
                    break
                buf.append(sql[j])
                j += 1
            if not buf:
                raise UnverifiableStatement("Empty quoted identifier.")
            out.append(_Tok("qid", "".join(buf)))
            i = j + 1
            continue
        if ch == "@":
            j = i + 1
            while j < n and not sql[j].isspace() and sql[j] not in ";,()'":
                if sql[j] == '"':  # quoted part inside a stage path
                    k = sql.find('"', j + 1)
                    if k == -1:
                        raise UnverifiableStatement("Unterminated quoted identifier.")
                    j = k + 1
                    continue
                j += 1
            out.append(_Tok("stage", sql[i + 1 : j]))
            i = j
            continue
        if ch == "?":
            out.append(_Tok("bind", "?"))
            i += 1
            continue
        if ch == "$" or (ch == ":" and i + 1 < n and (sql[i + 1] in _IDENT_START) and not sql.startswith("::", i)):
            if ch == ":" and out and (out[-1].is_name() or out[-1].kind in ("num", "str") or out[-1].val in (")", "]")):
                # semi-structured path `col:field` — a ':' operator, not a bind
                out.append(_Tok("p", ":"))
                i += 1
                continue
            j = i + 1
            while j < n and sql[j] in _IDENT_CHARS:
                j += 1
            out.append(_Tok("bind", sql[i:j]))
            i = j
            continue
        if ch in _IDENT_START:
            j = i + 1
            while j < n and sql[j] in _IDENT_CHARS:
                j += 1
            out.append(_Tok("id", sql[i:j].upper()))
            i = j
            continue
        if ch.isdigit() or (ch == "." and i + 1 < n and sql[i + 1].isdigit() and not (out and out[-1].is_name())):
            j = i + 1
            while j < n and (sql[j].isalnum() or sql[j] == "." or (sql[j] in "+-" and sql[j - 1] in "eE")):
                j += 1
            out.append(_Tok("num", sql[i:j]))
            i = j
            continue
        two = sql[i : i + 2]
        if two in _PUNCT2:
            out.append(_Tok("p", two))
            i += 2
            continue
        out.append(_Tok("p", ch))
        i += 1
    return out


# --------------------------------------------------------------------------- vocabulary

_FROM_TERMINATORS = frozenset(
    {
        "WHERE", "GROUP", "ORDER", "HAVING", "LIMIT", "QUALIFY", "UNION", "EXCEPT", "MINUS", "INTERSECT",
        "WINDOW", "FETCH", "OFFSET", "WHEN", "CONNECT", "START", "RETURNING", "FOR", "SET", "VALUES", "SELECT",
    }
)
_JOIN_WORDS = frozenset({"JOIN", "INNER", "LEFT", "RIGHT", "FULL", "OUTER", "CROSS", "NATURAL", "ASOF"})
_ITEM_MODIFIERS = frozenset({"AT", "BEFORE", "CHANGES", "END", "SAMPLE", "TABLESAMPLE", "MATCH_RECOGNIZE", "PIVOT", "UNPIVOT"})
_NOT_ALIAS = _FROM_TERMINATORS | _JOIN_WORDS | _ITEM_MODIFIERS | {
    "ON", "USING", "AS", "LATERAL", "MATCH_CONDITION", "TABLE", "INTO", "FROM", "AND", "OR", "NOT", "IS", "IN",
}
# Words that may be followed by '(' without being a function call.
_NON_CALL_WORDS = frozenset(
    {
        "IN", "VALUES", "OVER", "AS", "EXISTS", "ANY", "ALL", "SOME", "AND", "OR", "NOT", "ON", "USING", "WITHIN",
        "FILTER", "QUALIFY", "WHERE", "SELECT", "FROM", "BY", "PARTITION", "ORDER", "GROUP", "HAVING", "WHEN",
        "THEN", "ELSE", "CASE", "PATTERN", "DEFINE", "MEASURES", "INTO", "KEY", "PRIMARY", "UNIQUE", "FOREIGN",
        "REFERENCES", "CLUSTER", "LIKE", "ILIKE", "RLIKE", "ESCAPE", "IF", "SEED", "REPEATABLE", "BERNOULLI",
        "ROW", "ROWS", "SYSTEM", "BLOCK", "RETURNS", "TABLE", "WITH", "LATERAL", "INCLUDE", "EXCLUDE", "RENAME",
        "REPLACE", "ILIKE", "IDENTITY", "AUTOINCREMENT", "CHECK", "CONSTRAINT", "UNION", "EXCEPT", "MINUS",
        "INTERSECT", "SETS", "CUBE", "ROLLUP", "GROUPING", "NULLS", "MATCH_RECOGNIZE", "PIVOT", "UNPIVOT",
        "DIRECTORY", "ENCRYPTION", "FILE_FORMAT", "FORMAT_TYPE_OPTIONS", "COPY_OPTIONS", "FILES",
        "ATTRIBUTES", "TABLES", "RELATIONSHIPS", "FACTS", "DIMENSIONS", "METRICS", "PACKAGES", "IMPORTS",
        "EXTERNAL_ACCESS_INTEGRATIONS", "SECRETS", "LAG", "TARGET_LAG", "AT", "BEFORE", "CHANGES", "END",
        "SAMPLE", "TABLESAMPLE", "MATCH_CONDITION", "DEFAULT", "SET", "LIMIT", "IS", "BETWEEN",
    }
)
_DATA_TYPES = frozenset(
    {
        "NUMBER", "DECIMAL", "NUMERIC", "INT", "INTEGER", "BIGINT", "SMALLINT", "TINYINT", "BYTEINT", "FLOAT",
        "FLOAT4", "FLOAT8", "DOUBLE", "REAL", "VARCHAR", "CHAR", "CHARACTER", "STRING", "TEXT", "NCHAR",
        "NVARCHAR", "NVARCHAR2", "BINARY", "VARBINARY", "BOOLEAN", "DATE", "DATETIME", "TIME", "TIMESTAMP",
        "TIMESTAMP_LTZ", "TIMESTAMP_NTZ", "TIMESTAMP_TZ", "VARIANT", "OBJECT", "ARRAY", "GEOGRAPHY",
        "GEOMETRY", "VECTOR", "MAP", "FILE", "DECFLOAT", "INTERVAL",
    }
)
_TABLE_FUNCTIONS = frozenset({"FLATTEN", "GENERATOR", "SPLIT_TO_TABLE", "STRTOK_SPLIT_TO_TABLE"})
# First argument must be a named stage (``@db.sch.stg`` or ``'@db.sch.stg'``).
_STAGE_FUNCTIONS = frozenset(
    {
        "TO_FILE", "BUILD_SCOPED_FILE_URL", "BUILD_STAGE_FILE_URL", "GET_PRESIGNED_URL", "GET_STAGE_LOCATION",
        "GET_RELATIVE_PATH", "GET_ABSOLUTE_PATH", "DIRECTORY",
    }
)
# Pure, object-free SYSTEM$ functions; every other SYSTEM$ function is rejected.
_SAFE_SYSTEM_FUNCTIONS = frozenset(
    {"SYSTEM$TYPEOF", "SYSTEM$GET_PREDECESSOR_RETURN_VALUE", "SYSTEM$SET_RETURN_VALUE", "SYSTEM$CURRENT_USER_TASK_NAME", "SYSTEM$TASK_RUNTIME_INFO"}
)
_REJECTED_WORDS = frozenset({"IDENTIFIER", "EXECUTE", "IMMEDIATE", "BEGIN", "DECLARE", "LET", "RESULT_SCAN", "EXCEPTION"})
_NON_SQL_LANGUAGES = frozenset({"PYTHON", "JAVASCRIPT", "JAVA", "SCALA"})

# CREATE / ALTER / DROP object kinds a flow component may manage, and how their target is authorized:
# "schema" = the object must live in a schema selected in the flow context (creation, or kinds that have no
# context category such as tasks/streams); otherwise the object itself must be part of the context.
_DDL_KINDS: dict[tuple[str, ...], str] = {
    ("CORTEX", "SEARCH", "SERVICE"): "object",
    ("SEMANTIC", "VIEW"): "object",
    ("MATERIALIZED", "VIEW"): "object",
    ("DYNAMIC", "TABLE"): "object",
    ("ICEBERG", "TABLE"): "object",
    ("FILE", "FORMAT"): "schema",
    ("TABLE",): "object",
    ("VIEW",): "object",
    ("STAGE",): "stage",
    ("FUNCTION",): "routine",
    ("PROCEDURE",): "routine",
    ("STREAM",): "object",
    ("TASK",): "schema",
    ("ALERT",): "schema",
    ("SEQUENCE",): "schema",
}
_CREATE_MODIFIERS = frozenset(
    {"TEMP", "TEMPORARY", "TRANSIENT", "SECURE", "RECURSIVE", "LOCAL", "GLOBAL", "VOLATILE", "HYBRID"}
)
_ROUTINE_KINDS = frozenset({("FUNCTION",), ("PROCEDURE",)})
_ON_TARGET_KINDS = frozenset({"TABLE", "VIEW", "STAGE", "DIRECTORY", "EXTERNAL"})
# Governance / account-level clauses a flow component may not attach (row access, masking, tags, integrations).
_DDL_FORBIDDEN_WORDS = frozenset(
    {"POLICY", "TAG", "STORAGE_INTEGRATION", "CREDENTIALS", "INTEGRATION", "EXTERNAL_ACCESS_INTEGRATIONS", "SECRETS", "OWNERSHIP", "GRANT", "GRANTS"}
)
_SHOW_OBJECT_CONTAINERS = frozenset({"TABLE", "VIEW"})
_DESCRIBE_KINDS: tuple[tuple[tuple[str, ...], str], ...] = (
    (("CORTEX", "SEARCH", "SERVICE"), "object"),
    (("SEMANTIC", "VIEW"), "object"),
    (("MATERIALIZED", "VIEW"), "object"),
    (("DYNAMIC", "TABLE"), "object"),
    (("ICEBERG", "TABLE"), "object"),
    (("FILE", "FORMAT"), "schema"),
    (("TABLE",), "object"),
    (("VIEW",), "object"),
    (("STREAM",), "object"),
    (("STAGE",), "stage"),
    (("FUNCTION",), "routine"),
    (("PROCEDURE",), "routine"),
    (("AGENT",), "routine"),
    (("TASK",), "schema"),
    (("ALERT",), "schema"),
    (("SEQUENCE",), "schema"),
    (("SCHEMA",), "schema"),
    (("DATABASE",), "database"),
)


# --------------------------------------------------------------------------- analyzer


@dataclass
class _CteScope:
    name: str
    start: int
    end: int


class _Analyzer:
    """Walks the tokens of ONE statement batch and collects every referenced object (fail closed)."""

    def __init__(self, default_db: str, default_schema: str) -> None:
        self.db = (default_db or "").upper()
        self.sch = (default_schema or "").upper()
        self.refs: list[ObjectRef] = []
        self.created: set[str] = set()  # objects created earlier in this batch (in an authorized schema)
        self.t: list[_Tok] = []
        self.ctes: list[_CteScope] = []
        self.skip_from: set[int] = set()
        self.lead = ""

    # ---- helpers
    def tok(self, i: int) -> _Tok:
        return self.t[i] if 0 <= i < len(self.t) else _Tok("eof", "")

    def is_p(self, i: int, ch: str) -> bool:
        tk = self.tok(i)
        return tk.kind == "p" and tk.val == ch

    def match(self, i: int) -> int:
        """Index of the ')' closing the '(' at ``i``."""
        depth = 0
        for j in range(i, len(self.t)):
            if self.is_p(j, "("):
                depth += 1
            elif self.is_p(j, ")"):
                depth -= 1
                if depth == 0:
                    return j
        raise UnverifiableStatement("Unbalanced parentheses.")

    def chain(self, i: int) -> tuple[list[str], int]:
        """Read ``a[.b[.c]]`` (``a..c`` = PUBLIC schema) starting at ``i``; ([], i) when not a name."""
        if not self.tok(i).is_name():
            return [], i
        parts = [self.tok(i).val]
        j = i + 1
        while self.is_p(j, "."):
            if self.is_p(j + 1, ".") and self.tok(j + 2).is_name() and len(parts) == 1:
                parts += ["PUBLIC", self.tok(j + 2).val]
                j += 3
                continue
            if not self.tok(j + 1).is_name():
                break
            parts.append(self.tok(j + 1).val)
            j += 2
        return parts, j

    def resolve(self, parts: list[str], kind: str, access: str = "read") -> ObjectRef:
        if len(parts) > 3 or not parts:
            raise UnverifiableStatement("Object name has an invalid number of parts.")
        db, sch = self.db, self.sch
        if len(parts) == 3:
            db, sch, name = parts
        elif len(parts) == 2:
            sch, name = parts
        else:
            name = parts[0]
        if not db or not sch:
            raise UnverifiableStatement("Unqualified object without a flow-context default database/schema.")
        return ObjectRef(db, sch, name, kind, access)

    def add(self, parts: list[str], kind: str, access: str = "read") -> None:
        ref = self.resolve(parts, kind, access)
        if ref.kind in ("object", "stage", "routine") and ref.fqn.upper() in self.created and access != "ddl":
            return  # created by an earlier statement of this batch in an authorized schema
        self.refs.append(ref)

    def add_schema(self, parts: list[str], access: str = "ddl") -> None:
        ref = self.resolve(parts, "schema")
        self.refs.append(ObjectRef(ref.database, ref.schema, "", "schema", access))

    def schema_name(self, parts: list[str], access: str = "read") -> None:
        """A schema name ``[db.]sch`` (SHOW ... IN SCHEMA, DESCRIBE SCHEMA)."""
        if len(parts) == 2:
            db, sch = parts
        elif len(parts) == 1 and self.db:
            db, sch = self.db, parts[0]
        else:
            raise UnverifiableStatement("Schema name without a flow-context default database.")
        self.refs.append(ObjectRef(db, sch, "", "schema", access))

    def stage(self, text: str, access: str = "read") -> None:
        raw = (text or "").lstrip("@").split("/", 1)[0].strip()
        if not raw or raw[0] in "~%":
            raise UnverifiableStatement("User/table stages are not part of a flow context.")
        sub = _lex(raw)
        if not sub or any(not (tk.is_name() or (tk.kind == "p" and tk.val == ".")) for tk in sub):
            raise UnverifiableStatement("Invalid stage reference.")
        saved, self.t = self.t, sub
        try:
            parts, end = self.chain(0)
        finally:
            self.t = saved
        if end != len(sub):
            raise UnverifiableStatement("Invalid stage reference.")
        self.add(parts, "stage", access)

    def literal_name(self, text: str, kind: str) -> None:
        sub = _lex(text or "")
        saved, self.t = self.t, sub
        try:
            parts, end = self.chain(0)
        finally:
            self.t = saved
        if not parts or end != len(sub):
            raise UnverifiableStatement("Object name literal is not a plain name.")
        if kind == "schema":
            self.add_schema(parts, "read")
        else:
            self.add(parts, kind)

    def in_cte(self, name: str, at: int) -> bool:
        return any(c.name == name and c.start <= at < c.end for c in self.ctes)

    # ---- generic walker
    def walk(self, s: int, e: int) -> None:
        """Classify every token in [s, e); ``e`` is the end of the enclosing group (CTE scope)."""
        i = s
        while i < e:
            tk = self.tok(i)
            if tk.kind == "dollar":
                raise UnverifiableStatement("$$ bodies are only allowed in SQL CREATE FUNCTION.")
            if tk.kind == "str":
                if tk.val.lstrip().startswith("@"):
                    self.stage(tk.val.strip())
                i += 1
                continue
            if tk.kind == "stage":
                self.stage(tk.val)
                i += 1
                continue
            if tk.kind != "id" and tk.kind != "qid":
                i += 1
                continue
            if tk.kind == "id":
                w = tk.val
                if w in _REJECTED_WORDS:
                    raise UnverifiableStatement(f"{w} is not verifiable.")
                if w == "WITH" and self.tok(i + 1).is_name():
                    i = self.with_clause(i + 1, e)
                    continue
                if w == "FROM" and i not in self.skip_from:
                    i = self.from_list(i + 1, e, single=self.lead == "COPY")
                    continue
                if w == "USING" and self.lead in ("MERGE", "DELETE"):
                    i = self.from_list(i + 1, e)
                    continue
                if w == "JOIN":
                    raise UnverifiableStatement("JOIN outside a FROM clause.")
                if w == "INTO":
                    i = self.into_target(i + 1)
                    continue
                if w == "UPDATE" and not self.tok(i + 1).kw("SET"):
                    parts, j = self.chain(i + 1)
                    if not parts or self.is_p(j, "("):
                        raise UnverifiableStatement("UPDATE target is not a plain object name.")
                    self.add(parts, "object", "write")
                    i = j
                    continue
                if w == "CALL":
                    i = self.call(i + 1)
                    continue
                if w == "ON" and self.tok(i + 1).kw(*_ON_TARGET_KINDS):
                    i = self.on_target(i + 1)
                    continue
                if w in ("CLONE", "REFERENCES"):
                    parts, j = self.chain(i + 1)
                    if not parts:
                        raise UnverifiableStatement(f"{w} source is not a plain name.")
                    self.add(parts, "object")
                    i = j
                    continue
                if w == "TABLE" and self.is_p(i + 1, "("):
                    raise UnverifiableStatement("TABLE() is only allowed as a FROM item.")
            parts, j = self.chain(i)
            if self.is_p(j, "("):
                self.function_call(parts, tk, j)
                i = j  # arguments are walked by this same loop
                continue
            if len(parts) >= 2 and parts[-1] == "NEXTVAL":  # sequence: authorized by its schema
                self.add_schema(parts[:-1], "read")
            i = j

    def function_call(self, parts: list[str], first: _Tok, paren: int) -> None:
        name = parts[-1]
        if len(parts) == 1 and first.kind == "id":
            if name in _NON_CALL_WORDS or name in _DATA_TYPES:
                return
            if name in _REJECTED_WORDS:
                raise UnverifiableStatement(f"{name} is not verifiable.")
            if name == "EXTRACT":
                for k in range(paren + 1, self.match(paren)):
                    if self.tok(k).kw("FROM"):
                        self.skip_from.add(k)
                        break
                return
            if name.startswith("SYSTEM$"):
                if name == "SYSTEM$STREAM_HAS_DATA":
                    self.literal_arg(paren, "object")
                    return
                if name in _SAFE_SYSTEM_FUNCTIONS:
                    return
                raise UnverifiableStatement(f"{name} is not allowed from flow components.")
            if name in _STAGE_FUNCTIONS:
                self.stage_arg(paren)
                return
            if name == "GET_DDL":
                self.get_ddl(paren)
                return
            if name == "GET_OBJECT_REFERENCES":
                raise UnverifiableStatement("GET_OBJECT_REFERENCES is not verifiable.")
            if name in BUILTIN_FUNCTIONS or name in _TABLE_FUNCTIONS:
                return
            self.add(parts, "routine")  # unqualified user UDF -> default schema
            return
        if parts[0] == "SNOWFLAKE" and len(parts) >= 2:
            if name == "SEARCH_PREVIEW":
                self.literal_arg(paren, "object")
            return
        if len(parts) >= 2 and parts[-2] == "INFORMATION_SCHEMA":
            if len(parts) != 3:
                raise UnverifiableStatement("INFORMATION_SCHEMA functions must be database-qualified.")
            self.refs.append(ObjectRef(parts[0], "", "", "database"))
            return
        self.add(parts, "routine")

    def first_arg(self, paren: int) -> _Tok:
        return self.tok(paren + 1)

    def literal_arg(self, paren: int, kind: str) -> None:
        arg = self.first_arg(paren)
        if arg.kind != "str" or not (self.is_p(paren + 2, ",") or self.is_p(paren + 2, ")")):
            raise UnverifiableStatement("Object name argument must be a string literal.")
        self.literal_name(arg.val, kind)

    def stage_arg(self, paren: int) -> None:
        arg = self.first_arg(paren)
        if arg.kind == "stage" or (arg.kind == "str" and arg.val.strip().startswith("@")):
            return  # recorded by the walker when it reaches the argument
        raise UnverifiableStatement("Stage functions need a literal named stage as first argument.")

    def get_ddl(self, paren: int) -> None:
        kind_tok, name_tok = self.tok(paren + 1), self.tok(paren + 3)
        if kind_tok.kind != "str" or not self.is_p(paren + 2, ",") or name_tok.kind != "str":
            raise UnverifiableStatement("GET_DDL arguments must be string literals.")
        k = kind_tok.val.strip().upper()
        if k in ("DATABASE", "SCHEMA") or "INTEGRATION" in k or k in ("ROLE", "USER", "WAREHOUSE", "POLICY"):
            raise UnverifiableStatement("GET_DDL of account-level objects is not allowed.")
        name = name_tok.val.split("(", 1)[0]
        self.literal_name(name, "routine" if k in ("FUNCTION", "PROCEDURE") else ("stage" if k == "STAGE" else "object"))

    # ---- clauses
    def with_clause(self, i: int, e: int) -> int:
        if self.tok(i).kw("RECURSIVE"):
            i += 1
        while True:
            name_tok = self.tok(i)
            if not name_tok.is_name() or name_tok.kw("TAG", "ROW", "MASKING", "CONTACT", "AGGREGATION", "PROJECTION"):
                raise UnverifiableStatement("WITH clause is not a plain CTE list.")
            j = i + 1
            if self.is_p(j, "("):  # column list
                j = self.match(j) + 1
            if not self.tok(j).kw("AS"):
                raise UnverifiableStatement("WITH clause is not a plain CTE list.")
            j += 1
            if self.tok(j).kw("MATERIALIZED"):
                j += 1
            if not self.is_p(j, "("):
                raise UnverifiableStatement("CTE body must be parenthesized.")
            close = self.match(j)
            self.ctes.append(_CteScope(name_tok.val, i, e))
            self.walk(j + 1, close)
            # A body referencing its own name is a recursive CTE OR the real table: ambiguous.
            if any(self._self_reference(name_tok.val, k) for k in range(j + 1, close)):
                raise UnverifiableStatement("CTE references its own name (ambiguous recursion).")
            i = close + 1
            if self.is_p(i, ","):
                i += 1
                continue
            return i

    def _self_reference(self, name: str, k: int) -> bool:
        if not (self.tok(k).is_name() and self.tok(k).val == name):
            return False
        prev = self.tok(k - 1)
        return prev.kw("FROM", "JOIN", "LATERAL") or (prev.kind == "p" and prev.val in (",", "(") and self._in_from_position(k))

    def _in_from_position(self, k: int) -> bool:
        depth = 0
        for j in range(k - 1, -1, -1):
            tk = self.tok(j)
            if tk.kind == "p" and tk.val == ")":
                depth += 1
            elif tk.kind == "p" and tk.val == "(":
                if depth == 0:
                    continue
                depth -= 1
            elif depth == 0 and tk.kind == "id" and tk.val in ("FROM", "JOIN"):
                return True
            elif depth == 0 and tk.kind == "id" and tk.val in ("SELECT", "WHERE", "ON", "SET"):
                return False
        return False

    def into_target(self, i: int) -> int:
        tk = self.tok(i)
        if tk.kind == "stage":
            self.stage(tk.val, "write")
            return i + 1
        if tk.kind == "str" and tk.val.strip().startswith("@"):
            self.stage(tk.val.strip(), "write")
            return i + 1
        parts, j = self.chain(i)
        if not parts or tk.kind == "bind":
            raise UnverifiableStatement("INTO target is not a plain object name.")
        self.add(parts, "object", "append" if self.lead in ("INSERT", "MERGE", "COPY") else "write")
        if self.is_p(j, "("):  # column list
            j = self.match(j) + 1
        return j

    def call(self, i: int) -> int:
        parts, j = self.chain(i)
        if not parts or not self.is_p(j, "("):
            raise UnverifiableStatement("CALL target is not a plain procedure call.")
        self.add(parts, "routine")
        return j

    def on_target(self, i: int) -> int:
        kind_word = self.tok(i).val
        j = i + 1
        if kind_word == "EXTERNAL":
            raise UnverifiableStatement("External tables are not part of a flow context.")
        if self.tok(j).kind == "stage":
            self.stage(self.tok(j).val)
            return j + 1
        parts, k = self.chain(j)
        if not parts:
            raise UnverifiableStatement("ON target is not a plain name.")
        self.add(parts, "stage" if kind_word in ("STAGE", "DIRECTORY") else "object")
        return k

    def from_list(self, i: int, e: int, single: bool = False) -> int:
        while True:
            i = self.from_item(i, e)
            i = self.item_suffix(i, e)
            if single:
                return i
            while True:
                tk = self.tok(i)
                if i >= e or tk.kind == "eof" or self.is_p(i, ")") or self.is_p(i, ";"):
                    return i
                if self.is_p(i, ","):
                    i += 1
                    break  # next item
                if tk.kind == "id" and tk.val in _JOIN_WORDS and not self.is_p(i + 1, "("):
                    while self.tok(i).kind == "id" and self.tok(i).val in _JOIN_WORDS and self.tok(i).val != "JOIN":
                        i += 1
                    if not self.tok(i).kw("JOIN"):
                        raise UnverifiableStatement("Malformed JOIN.")
                    i = self.from_item(i + 1, e)
                    i = self.item_suffix(i, e)
                    if self.tok(i).kw("MATCH_CONDITION") and self.is_p(i + 1, "("):
                        close = self.match(i + 1)
                        self.walk(i + 2, close)
                        i = close + 1
                    continue
                if tk.kw("ON"):
                    end = self.condition_end(i + 1, e)
                    self.walk(i + 1, end)
                    i = end
                    continue
                if tk.kw("USING") and self.is_p(i + 1, "("):
                    i = self.match(i + 1) + 1  # join column list
                    continue
                if tk.kind == "id" and (tk.val in _FROM_TERMINATORS or tk.val == "USING"):
                    return i
                raise UnverifiableStatement(f"Unexpected token in FROM clause: {tk.val or tk.kind}.")

    def condition_end(self, i: int, e: int) -> int:
        while i < e:
            tk = self.tok(i)
            if self.is_p(i, "("):
                i = self.match(i) + 1
                continue
            if self.is_p(i, ")") or self.is_p(i, ",") or self.is_p(i, ";"):
                return i
            if tk.kind == "id" and (tk.val in _FROM_TERMINATORS or tk.val in _JOIN_WORDS) and not self.is_p(i + 1, "("):
                if tk.val not in ("SELECT", "VALUES", "SET"):
                    return i
            i += 1
        return i

    def from_item(self, i: int, e: int) -> int:
        tk = self.tok(i)
        if self.is_p(i, "("):
            close = self.match(i)
            k = i + 1
            while self.is_p(k, "("):
                k += 1
            if self.tok(k).kw("SELECT", "WITH", "VALUES"):
                self.walk(i + 1, close)  # subquery: its own scope
            else:
                end = self.from_list(i + 1, close)  # parenthesized join
                if end != close:
                    raise UnverifiableStatement("Malformed parenthesized FROM item.")
            return close + 1
        if tk.kw("LATERAL"):
            if self.is_p(i + 1, "("):
                close = self.match(i + 1)
                self.walk(i + 2, close)
                return close + 1
            return self.table_function(i + 1, e)
        if tk.kw("TABLE") and self.is_p(i + 1, "("):
            close = self.match(i + 1)
            parts, j = self.chain(i + 2)
            if not parts or self.tok(i + 2).kind not in ("id", "qid") or not self.is_p(j, "(") or self.match(j) != close - 1:
                raise UnverifiableStatement("TABLE() argument must be a table function call.")
            self.table_function(i + 2, close)
            return close + 1
        if tk.kind == "stage" or (tk.kind == "str" and tk.val.strip().startswith("@")):
            self.stage(tk.val.strip())
            j = i + 1
            if self.is_p(j, "("):  # (FILE_FORMAT => ..., PATTERN => ...)
                close = self.match(j)
                for k in range(j + 1, close):
                    if self.tok(k).kw("FILE_FORMAT") and self.is_p(k + 1, "=>") and self.tok(k + 2).kind == "str":
                        self.literal_name(self.tok(k + 2).val, "schema")
                j = close + 1
            return j
        if tk.kind in ("bind", "str", "num") or tk.kw("IDENTIFIER"):
            raise UnverifiableStatement("FROM item is not a plain object name.")
        parts, j = self.chain(i)
        if not parts:
            raise UnverifiableStatement("FROM item is not a plain object name.")
        if self.is_p(j, "("):
            return self.table_function(i, e)
        if len(parts) == 1 and self.in_cte(parts[0], i):
            return j
        self.add(parts, "object")
        return j

    def table_function(self, i: int, e: int) -> int:
        parts, j = self.chain(i)
        if not parts or not self.is_p(j, "("):
            raise UnverifiableStatement("Table function expected.")
        name = parts[-1]
        if len(parts) == 1 and self.tok(i).kind == "id":
            if name == "RESULT_SCAN":
                raise UnverifiableStatement("RESULT_SCAN is not verifiable.")
            if name in _STAGE_FUNCTIONS:
                self.stage_arg(j)
            elif name not in _TABLE_FUNCTIONS:
                self.add(parts, "routine")  # user UDTF
        else:
            self.function_call(parts, self.tok(i), j)
        close = self.match(j)
        self.walk(j + 1, close)
        return close + 1

    def item_suffix(self, i: int, e: int) -> int:
        aliased = False
        while i < e:
            tk = self.tok(i)
            if tk.kw("AT", "BEFORE", "CHANGES", "END", "MATCH_RECOGNIZE", "PIVOT", "UNPIVOT") and self.is_p(i + 1, "("):
                close = self.match(i + 1)
                self.walk(i + 2, close)
                i = close + 1
                continue
            if tk.kw("UNPIVOT") and self.tok(i + 1).kw("INCLUDE", "EXCLUDE"):
                i += 3
                continue
            if tk.kw("SAMPLE", "TABLESAMPLE"):
                i += 1
                if self.tok(i).kw("BERNOULLI", "ROW", "SYSTEM", "BLOCK"):
                    i += 1
                if not self.is_p(i, "("):
                    raise UnverifiableStatement("Malformed SAMPLE clause.")
                i = self.match(i) + 1
                if self.tok(i).kw("REPEATABLE", "SEED") and self.is_p(i + 1, "("):
                    i = self.match(i + 1) + 1
                continue
            if aliased:
                return i
            if tk.kw("AS"):
                if not self.tok(i + 1).is_name():
                    raise UnverifiableStatement("Malformed alias.")
                i += 2
            elif tk.kind == "qid" or (tk.kind == "id" and tk.val not in _NOT_ALIAS):
                i += 1
            else:
                return i
            aliased = True
            if self.is_p(i, "("):  # alias column list
                i = self.match(i) + 1
        return i

    # ---- statements
    def statement(self, toks: list[_Tok]) -> None:
        self.t, self.ctes, self.skip_from = toks, [], set()
        first = self.tok(0)
        lead = first.val if first.kind == "id" else ("(" if self.is_p(0, "(") else "")
        self.lead = lead
        n = len(toks)
        if lead in ("SELECT", "WITH", "(", "VALUES"):
            self.walk(0, n)
        elif lead == "INSERT":
            self.walk(0, n)
        elif lead in ("UPDATE", "MERGE"):
            self.walk(0, n)
        elif lead == "DELETE":
            if not self.tok(1).kw("FROM"):
                raise UnverifiableStatement("DELETE must be DELETE FROM <table>.")
            parts, j = self.chain(2)
            if not parts:
                raise UnverifiableStatement("DELETE target is not a plain name.")
            self.add(parts, "object", "write")
            j = self.item_suffix(j, n)
            self.walk(j, n)
        elif lead == "COPY":
            if not self.tok(1).kw("INTO"):
                raise UnverifiableStatement("COPY must be COPY INTO.")
            self.walk(1, n)
            self._copy_file_formats()
        elif lead in ("SHOW",):
            self.show()
        elif lead in ("DESCRIBE", "DESC"):
            self.describe()
        elif lead in ("LIST", "LS", "REMOVE", "RM"):
            tk = self.tok(1)
            if tk.kind == "stage" or (tk.kind == "str" and tk.val.strip().startswith("@")):
                self.stage(tk.val.strip(), "read" if lead in ("LIST", "LS") else "write")
            else:
                raise UnverifiableStatement(f"{lead} target must be a named stage.")
            self.walk(2, n)
        elif lead in ("CREATE", "ALTER", "DROP"):
            self.ddl()
        elif lead == "TRUNCATE":
            j = 1
            while self.tok(j).kw("TABLE", "IF", "EXISTS"):
                j += 1
            parts, j = self.chain(j)
            if not parts or j != n:
                raise UnverifiableStatement("TRUNCATE target is not a plain name.")
            self.add(parts, "object", "write")
        elif lead == "EXECUTE" and self.tok(1).kw("TASK"):
            parts, j = self.chain(2)
            if not parts or j != n:
                raise UnverifiableStatement("EXECUTE TASK target is not a plain name.")
            self.add_schema(parts, "write")
        elif lead == "CALL":
            j = self.call(1)
            self.walk(j, n)
        else:
            raise UnverifiableStatement(f"Statement kind {lead or first.val or '?'} is not allowed from flow components.")

    def _copy_file_formats(self) -> None:
        for k in range(len(self.t)):
            if self.tok(k).kw("FORMAT_NAME") and self.is_p(k + 1, "=") and self.tok(k + 2).kind == "str":
                self.literal_name(self.tok(k + 2).val, "schema")

    def show(self) -> None:
        n = len(self.t)
        k = 1
        while k < n and not self.tok(k).kw("IN"):
            if self.is_p(k, "("):
                raise UnverifiableStatement("Malformed SHOW.")
            k += 1
        if k >= n:
            raise UnverifiableStatement("SHOW without IN <database|schema> is outside any flow context.")
        if any(self.tok(x).kw("GRANTS", "USERS", "ROLES", "INTEGRATIONS", "WAREHOUSES", "PARAMETERS") for x in range(1, k)):
            raise UnverifiableStatement("Account-level SHOW is outside any flow context.")
        container = self.tok(k + 1)
        j = k + 2
        if container.kw("ACCOUNT", "APPLICATION", "ORGANIZATION"):
            raise UnverifiableStatement("Account-wide SHOW is outside any flow context.")
        if container.kw("DATABASE"):
            parts, j = self.chain(j)
            if len(parts) != 1:
                raise UnverifiableStatement("SHOW ... IN DATABASE expects a database name.")
            self.refs.append(ObjectRef(parts[0], "", "", "database"))
        elif container.kw("SCHEMA"):
            parts, j = self.chain(j)
            if not parts or len(parts) > 2:
                raise UnverifiableStatement("SHOW ... IN SCHEMA expects a schema name.")
            self.schema_name(parts)
        elif container.kw(*_SHOW_OBJECT_CONTAINERS):
            parts, j = self.chain(j)
            if not parts:
                raise UnverifiableStatement("SHOW ... IN TABLE expects a table name.")
            self.add(parts, "object")
        else:
            parts, j = self.chain(k + 1)
            if len(parts) != 2:
                raise UnverifiableStatement("Ambiguous SHOW ... IN target.")
            self.schema_name(parts)
        while j < n:  # only STARTS WITH '<lit>' / LIMIT <n> [FROM '<lit>'] may follow
            tk = self.tok(j)
            if not (tk.kw("STARTS", "WITH", "LIMIT", "FROM") or tk.kind in ("str", "num")):
                raise UnverifiableStatement("Unexpected SHOW clause.")
            j += 1

    def read_kind(self, i: int, table) -> tuple[tuple[str, ...], str, int] | None:
        for words, kind in table:
            if all(self.tok(i + k).kw(w) for k, w in enumerate(words)):
                return words, kind, i + len(words)
        return None

    def describe(self) -> None:
        n = len(self.t)
        found = self.read_kind(1, _DESCRIBE_KINDS)
        if found is None:
            if self.tok(1).kind == "id" and self.tok(2).is_name():
                raise UnverifiableStatement(f"DESCRIBE {self.tok(1).val} is not allowed from flow components.")
            words, kind, j = ("TABLE",), "object", 1
        else:
            words, kind, j = found
        if self.tok(j).kind == "stage":
            self.stage(self.tok(j).val)
            return
        parts, j = self.chain(j)
        if not parts:
            raise UnverifiableStatement("DESCRIBE target is not a plain name.")
        if kind == "database":
            if len(parts) != 1:
                raise UnverifiableStatement("DESCRIBE DATABASE expects a database name.")
            self.refs.append(ObjectRef(parts[0], "", "", "database"))
        elif kind == "schema":
            if words == ("SCHEMA",):
                if len(parts) > 2:
                    raise UnverifiableStatement("DESCRIBE SCHEMA expects a schema name.")
                self.schema_name(parts)
            else:
                self.add_schema(parts, "read")  # tasks/alerts/... are authorized by their schema
        else:
            self.add(parts, kind)
        if self.is_p(j, "("):  # routine signature
            j = self.match(j) + 1
        if j != n:
            raise UnverifiableStatement("Unexpected DESCRIBE clause.")

    def ddl(self) -> None:
        n = len(self.t)
        lead = self.lead
        i = 1
        if lead == "CREATE":
            if self.tok(i).kw("OR") and self.tok(i + 1).kw("REPLACE"):
                i += 2
            while self.tok(i).kw(*_CREATE_MODIFIERS):
                i += 1
        found = self.read_kind(i, tuple(_DDL_KINDS.items()))
        if found is None:
            raise UnverifiableStatement(f"Object kind {self.tok(i).val or '?'} cannot be managed from flow components.")
        kind_words, auth, i = found
        while self.tok(i).kw("IF", "NOT", "EXISTS"):
            i += 1
        parts, i = self.chain(i)
        if not parts:
            raise UnverifiableStatement(f"{lead} target is not a plain name.")
        target = self.resolve(parts, "routine" if auth == "routine" else ("stage" if auth == "stage" else "object"))
        if lead == "CREATE":
            self.refs.append(ObjectRef(target.database, target.schema, "", "schema", "ddl"))
        elif auth == "schema":
            self.refs.append(ObjectRef(target.database, target.schema, "", "schema", "ddl"))
        else:
            self.refs.append(ObjectRef(target.database, target.schema, target.name, target.kind, "ddl"))
        for k in range(i, n):
            if self.tok(k).kind == "id" and self.tok(k).val in _DDL_FORBIDDEN_WORDS:
                raise UnverifiableStatement(f"{self.tok(k).val} clauses are not allowed from flow components.")
        if lead == "DROP":
            if self.is_p(i, "("):
                i = self.match(i) + 1
            while self.tok(i).kw("CASCADE", "RESTRICT"):
                i += 1
            if i != n:
                raise UnverifiableStatement("Unexpected DROP clause.")
            return
        if kind_words in _ROUTINE_KINDS:
            self.routine_rest(i, n, kind_words)
        elif kind_words == ("TASK",) and lead == "CREATE":
            self.task_rest(i, n)
        elif kind_words == ("SEMANTIC", "VIEW") and lead == "CREATE":
            self.semantic_view_rest(i, n)
        else:
            if lead == "ALTER":
                for k in range(i, n):
                    if self.tok(k).kw("TO", "WITH") and self.tok(k - 1).kw("RENAME", "SWAP"):
                        p2, _ = self.chain(k + 1)
                        if not p2:
                            raise UnverifiableStatement("RENAME/SWAP target is not a plain name.")
                        if self.tok(k - 1).kw("SWAP"):
                            self.add(p2, target.kind, "ddl")
                        else:
                            self.add_schema(p2, "ddl")  # new name must land in an authorized schema
            if lead == "ALTER" and kind_words == ("TASK",) and any(self.tok(k).kw("MODIFY") for k in range(i, n)):
                raise UnverifiableStatement("ALTER TASK ... MODIFY is not verifiable.")
            j = i
            if lead == "CREATE" and self.tok(j).kw("LIKE", "CLONE"):  # CREATE TABLE t LIKE|CLONE src
                p2, j = self.chain(j + 1)
                if not p2:
                    raise UnverifiableStatement("LIKE/CLONE source is not a plain name.")
                self.add(p2, "object")
            if lead == "CREATE" and self.is_p(j, "("):  # column definitions / view column list
                close = self.match(j)
                self.walk(j + 1, close)
                j = close + 1
            self.walk(j, n)
        if lead == "CREATE":
            self.created.add(target.fqn.upper())

    def routine_rest(self, i: int, n: int, kind_words: tuple[str, ...]) -> None:
        if self.is_p(i, "("):  # signature: argument names and types
            i = self.match(i) + 1
        language = "SQL"
        body: _Tok | None = None
        k = i
        while k < n:
            tk = self.tok(k)
            if tk.kw("LANGUAGE") and self.tok(k + 1).kind == "id":
                language = self.tok(k + 1).val
                k += 2
                continue
            if tk.kw("RETURNS") and self.tok(k + 1).kw("TABLE") and self.is_p(k + 2, "("):
                k = self.match(k + 2) + 1
                continue
            if tk.kw("AS") and self.tok(k + 1).kind in ("dollar", "str"):
                body = self.tok(k + 1)
                k += 2
                continue
            if tk.kind == "dollar":
                raise UnverifiableStatement("Routine body must follow AS.")
            if tk.kind == "str" and tk.val.strip().startswith("@"):
                self.stage(tk.val.strip())
            if tk.kind == "stage":
                self.stage(tk.val)
            k += 1
        if language in _NON_SQL_LANGUAGES or language != "SQL":
            raise UnverifiableStatement(f"LANGUAGE {language} routines cannot be created from flow components.")
        if body is None:
            if self.lead == "CREATE":
                raise UnverifiableStatement("Routine without an inspectable SQL body.")
            return
        if kind_words == ("PROCEDURE",):
            raise UnverifiableStatement("SQL procedures are Snowflake Scripting, which is not verifiable.")
        # SQL UDF body: an expression or query, analyzed with the same rules.
        sub = _lex(body.val)
        saved = (self.t, self.ctes, self.skip_from, self.lead)
        try:
            self.t, self.ctes, self.skip_from = sub, [], set()
            self.lead = sub[0].val if sub and sub[0].kind == "id" else ""
            if any(t.kind == "p" and t.val == ";" for t in sub):
                raise UnverifiableStatement("SQL UDF body must be a single expression or query.")
            self.walk(0, len(sub))
        finally:
            self.t, self.ctes, self.skip_from, self.lead = saved

    def task_rest(self, i: int, n: int) -> None:
        k = i
        as_at = -1
        while k < n:
            tk = self.tok(k)
            if tk.kw("AS"):
                as_at = k
                break
            if tk.kw("AFTER"):
                k += 1
                while True:
                    parts, k = self.chain(k)
                    if not parts:
                        raise UnverifiableStatement("AFTER expects task names.")
                    self.add_schema(parts, "read")
                    if not self.is_p(k, ","):
                        break
                    k += 1
                continue
            if tk.kw("WHEN"):
                end = k + 1
                while end < n and not self.tok(end).kw("AS"):
                    if self.is_p(end, "("):
                        end = self.match(end)
                    end += 1
                self.walk(k + 1, end)
                k = end
                continue
            k += 1
        if as_at < 0:
            raise UnverifiableStatement("CREATE TASK without a body.")
        body = self.t[as_at + 1 :]
        saved_t, saved_lead = self.t, self.lead
        try:
            if body and body[0].kw("BEGIN"):
                if not (body[-1].kw("END") or (len(body) >= 2 and body[-2].kw("END") and body[-1].kind == "p" and body[-1].val == ";")):
                    raise UnverifiableStatement("Task block must be BEGIN ... END.")
                inner = body[1 : -1 if body[-1].kw("END") else -2]
                for stmt in _split_tokens(inner):
                    self.statement(stmt)
            else:
                self.statement(body)
        finally:
            self.t, self.lead = saved_t, saved_lead

    def semantic_view_rest(self, i: int, n: int) -> None:
        k = i
        saw_tables = False
        while k < n:
            tk = self.tok(k)
            if tk.kw("TABLES") and self.is_p(k + 1, "("):
                close = self.match(k + 1)
                for entry in _split_commas(self, k + 2, close):
                    s = entry[0]
                    if self.tok(s + 1).kw("AS"):
                        s += 2
                    parts, _ = self.chain(s)
                    if not parts:
                        raise UnverifiableStatement("Semantic view TABLES entry is not a plain name.")
                    self.add(parts, "object")
                saw_tables = True
                k = close + 1
                continue
            if tk.kind == "dollar" or tk.kw("SELECT", "FROM"):
                raise UnverifiableStatement("Unexpected clause in CREATE SEMANTIC VIEW.")
            k += 1
        if not saw_tables:
            raise UnverifiableStatement("CREATE SEMANTIC VIEW without TABLES.")


def _split_commas(an: _Analyzer, s: int, e: int) -> list[tuple[int, int]]:
    out, start, k = [], s, s
    while k < e:
        if an.is_p(k, "("):
            k = an.match(k) + 1
            continue
        if an.is_p(k, ","):
            out.append((start, k))
            start = k + 1
        k += 1
    if start < e:
        out.append((start, e))
    return out


def _split_tokens(toks: list[_Tok]) -> list[list[_Tok]]:
    """Split on top-level ';' (parentheses-aware); empty statements are dropped."""
    out: list[list[_Tok]] = []
    cur: list[_Tok] = []
    depth = 0
    for tk in toks:
        if tk.kind == "p" and tk.val == "(":
            depth += 1
        elif tk.kind == "p" and tk.val == ")":
            depth -= 1
        if tk.kind == "p" and tk.val == ";" and depth == 0:
            if cur:
                out.append(cur)
            cur = []
            continue
        cur.append(tk)
    if cur:
        out.append(cur)
    return out


def _split_batch(toks: list[_Tok]) -> list[list[_Tok]]:
    """Split a batch into statements; a CREATE TASK ... AS BEGIN ... END block stays one statement."""
    out: list[list[_Tok]] = []
    cur: list[_Tok] = []
    depth = 0
    block = 0
    case = 0
    for idx, tk in enumerate(toks):
        if tk.kind == "p" and tk.val == "(":
            depth += 1
        elif tk.kind == "p" and tk.val == ")":
            depth -= 1
        elif tk.kw("BEGIN") and cur and cur[-1].kw("AS") and cur[0].kw("CREATE"):
            block += 1
        elif tk.kw("CASE"):
            case += 1
        elif tk.kw("END") and not (idx + 1 < len(toks) and toks[idx + 1].kind == "p" and toks[idx + 1].val == "("):
            if case:
                case -= 1
            elif block:
                block -= 1
        if tk.kind == "p" and tk.val == ";" and depth == 0 and block == 0:
            if cur:
                out.append(cur)
            cur = []
            continue
        cur.append(tk)
    if depth != 0 or block != 0:
        raise UnverifiableStatement("Unbalanced statement batch.")
    if cur:
        out.append(cur)
    return out


def _resolve(parts: list[str], default_db: str, default_schema: str, kind: str) -> ObjectRef:
    """Resolve an already-split name (used by the bridge for Cortex Analyst semantic sources)."""
    return _Analyzer(default_db, default_schema).resolve([p for p in parts if p], kind)


def _stage_ref(token: str, default_db: str, default_schema: str) -> ObjectRef:
    """Resolve ``@db.sch.stage[/path]`` (raises for user/table stages)."""
    an = _Analyzer(default_db, default_schema)
    an.stage(token)
    return an.refs[0]


def _analyze(statement: str, default_db: str, default_schema: str) -> _Analyzer:
    an = _Analyzer(default_db, default_schema)
    for stmt in _split_batch(_lex(statement or "")):
        an.statement(stmt)
    return an


def extract_object_refs(statement: str, default_db: str, default_schema: str) -> list[ObjectRef]:
    """Every Snowflake object referenced by ``statement``; raises :class:`UnverifiableStatement`.

    ``default_db`` / ``default_schema`` are the flow context's defaults; unqualified names resolve against
    them only (empty -> an unqualified name is unverifiable).
    """
    return _analyze(statement, default_db, default_schema).refs


_OBJECT_CATEGORIES = (
    "tables", "views", "iceberg_tables", "semantic_views", "memories", "cortex_search_services", "streams",
)
_ROUTINE_CATEGORIES = ("procedures", "functions", "cortex_agents")


def _upper_set(values: Any) -> set[str]:
    return {str(v).strip().upper() for v in (values or []) if str(v).strip()}


def is_ref_in_context(ref: ObjectRef, ctx: dict[str, Any]) -> bool:
    """True when ``ref`` is part of the flow's authorized `[ ❄️ Context ]` selection."""
    dbs = _upper_set(ctx.get("databases")) | _upper_set(ctx.get("full_databases"))
    full_dbs = _upper_set(ctx.get("full_databases"))
    full_schemas = _upper_set(ctx.get("full_schemas"))
    schemas_by_db = ctx.get("schemas_by_db") or {}
    db, sch = ref.database.upper(), ref.schema.upper()
    if ref.kind == "database":
        return db in dbs
    if db in full_dbs:
        return True
    key = f"{db}.{sch}"
    schema_selected = key in full_schemas or (
        db in dbs and sch in _upper_set(schemas_by_db.get(db) if isinstance(schemas_by_db, dict) else [])
    )
    if ref.kind == "schema":
        return schema_selected
    if key in full_schemas:
        return True
    selected = (ctx.get("selected_objects") or {}).get(key)
    if selected == "*":
        return True
    if not isinstance(selected, dict):
        return False
    if ref.kind == "stage":
        cats: tuple[str, ...] = ("stages",)
    elif ref.kind == "routine":
        cats = _ROUTINE_CATEGORIES
    else:
        cats = _OBJECT_CATEGORIES
    name = ref.name.upper()
    for cat in cats:
        bucket = selected.get(cat)
        if bucket == "*":
            return True
        if isinstance(bucket, list) and name in _upper_set(bucket):
            return True
    return False


def check_statement(
    statement: str,
    ctx: dict[str, Any],
    default_db: str,
    default_schema: str,
    exempt_fqns: frozenset[str] | set[str] = frozenset(),
    bridge_routine_fqns: frozenset[str] | set[str] = frozenset(),
) -> tuple[bool, str, list[str]]:
    """(allowed, reason, offending_fqns) for ``statement`` against the flow context ``ctx``.

    ``exempt_fqns`` are exact bridge-owned governance FQNs (see ``LiveSnowflakeBridge._flow_guard_exempt_fqns``).
    They are exempt ONLY as append targets (INSERT / MERGE / COPY INTO); reading, updating, deleting,
    truncating or dropping them still requires the flow's context.

    ``bridge_routine_fqns`` are exact FQNs of bridge-owned routines (``bridge_routines.bridge_routine_fqns``):
    they may only be CALLed / invoked; creating, altering, dropping or describing them is never exempt.
    """
    try:
        an = _analyze(statement, default_db, default_schema)
    except UnverifiableStatement as exc:
        return False, str(exc), []
    refs = an.refs
    exempt = {str(f).replace('"', "").strip().upper() for f in exempt_fqns if f}
    routines = {str(f).replace('"', "").strip().upper() for f in bridge_routine_fqns if f and "*" not in str(f)}
    # Bridge-owned routines are never created, replaced, altered or dropped from a flow component.
    managed = {r.fqn.upper() for r in refs if r.access == "ddl" and r.name} | {f.upper() for f in an.created}
    hijack = sorted(managed & routines)
    if hijack:
        return False, "Bridge-owned routines cannot be managed from flow components.", hijack
    offending = sorted(
        {
            r.fqn
            for r in refs
            # Governance exemption: only appending to an exact bridge-owned FQN (INSERT/MERGE/COPY INTO).
            if not (r.access == "append" and r.kind == "object" and r.fqn.upper() in exempt)
            and not (r.access == "read" and r.kind == "routine" and r.fqn.upper() in routines)
            and not is_ref_in_context(r, ctx)
        }
    )
    if offending:
        return False, "Referenced objects are not in this flow's [ ❄️ Context ].", offending
    return True, "", []


def register_component_object(fqn: str, category: str, flow_id: str | None = None) -> bool:
    """Register an object a flow component created/provisioned in that flow's `[ ❄️ Context ]` (PASSO 3).

    Only when the object's schema is already part of the flow context, so this never widens the context
    beyond a schema the user selected; it lets the component use its own object (stream, table) right away.
    Returns True when registered.
    """
    from lfx_snowflake.fastapi_live_bridge import CURRENT_SNOWFLAKE_FLOW_ID, LIVE_BRIDGE  # lazy: import cycle

    parts = [p.strip().strip('"') for p in str(fqn or "").split(".")]
    if len(parts) != 3 or not all(parts):
        return False
    fid = LIVE_BRIDGE._normalize_flow_id(flow_id or CURRENT_SNOWFLAKE_FLOW_ID.get())
    if not fid:
        return False  # no flow: nothing is enforced, nothing to register
    ctx = LIVE_BRIDGE.get_flow_context(fid, create=False) or {}
    db, sch, name = parts[0].upper(), parts[1].upper(), parts[2].upper()
    if not is_ref_in_context(ObjectRef(db, sch, "", "schema"), ctx):
        return False
    LIVE_BRIDGE.register_object_in_context(db, sch, category, name, flow_id=fid)
    return True


def run_in_flow_scope(flow_id: str | None, fn, *args, **kwargs):
    """PASSO 5 hook: run `fn` as component code of `flow_id` (e.g. a trigger firing outside a request).

    Sets `CURRENT_SNOWFLAKE_FLOW_ID` and `FLOW_COMPONENT_SQL`, so every `LIVE_BRIDGE.execute_sql`
    inside `fn` is checked against that flow's `[ ❄️ Context ]`; both are restored afterwards.
    """
    from lfx_snowflake.fastapi_live_bridge import CURRENT_SNOWFLAKE_FLOW_ID  # lazy: avoids an import cycle

    fid_token = CURRENT_SNOWFLAKE_FLOW_ID.set(flow_id)
    scope_token = FLOW_COMPONENT_SQL.set(True)
    try:
        return fn(*args, **kwargs)
    finally:
        FLOW_COMPONENT_SQL.reset(scope_token)
        CURRENT_SNOWFLAKE_FLOW_ID.reset(fid_token)
