"""Build-time installer that wires SnowflakeLangflow components into native built-in categories, patches the React bundle safely, and mounts `/api/v1/snowflake/*`."""

from __future__ import annotations

import copy
import hashlib
import os
import re
import shutil
import sqlite3
import urllib.parse
from pathlib import Path


os.environ.setdefault("UV_CACHE_DIR", "/tmp/uv-cache")
os.environ.setdefault("XDG_CACHE_HOME", "/tmp/.cache")
os.environ.setdefault("SQLITE_TMPDIR", "/tmp")
os.environ.setdefault("LANGFLOW_A2A_ENABLED", "true")

def _detect_site_packages() -> Path:
    """Locate the Langflow site-packages dir instead of hard-coding the Python minor version."""
    env = os.environ.get("SFLF_SITE_PACKAGES")
    if env:
        return Path(env)
    try:
        import importlib.util

        spec = importlib.util.find_spec("langflow")
        if spec and spec.origin:
            return Path(spec.origin).resolve().parent.parent
    except (ImportError, ValueError):
        pass
    candidates = sorted(Path("/app/.venv").glob("lib*/python3*/site-packages"))
    for cand in candidates:
        if (cand / "langflow").is_dir():
            return cand
    raise SystemExit("[install_live_hooks] FAIL-CLOSED: cannot locate the Langflow site-packages directory")


def _detect_langflow_version() -> str:
    import importlib.metadata as _md

    for dist in ("langflow-base", "langflow"):
        try:
            return _md.version(dist)
        except _md.PackageNotFoundError:
            continue
    return ""


def _configure_paths(site_packages: Path | None = None, frontend: Path | None = None, version: str | None = None) -> None:
    global SITE_PACKAGES, LANGFLOW_MAIN, LANGFLOW_FRONTEND, LFX_COMPONENTS_DIR, LFX_INDEX_JSON, LANGFLOW_FAMILY
    SITE_PACKAGES = Path(site_packages) if site_packages else _detect_site_packages()
    LANGFLOW_MAIN = SITE_PACKAGES / "langflow" / "main.py"
    LANGFLOW_FRONTEND = Path(frontend) if frontend else SITE_PACKAGES / "langflow" / "frontend"
    LFX_COMPONENTS_DIR = SITE_PACKAGES / "lfx" / "components"
    LFX_INDEX_JSON = SITE_PACKAGES / "lfx" / "_assets" / "component_index.json"
    ver = version or _detect_langflow_version()
    m = re.match(r"(\d+)\.(\d+)", ver or "")
    if not m or f"{m.group(1)}.{m.group(2)}" not in SUPPORTED_LANGFLOW_FAMILIES:
        raise SystemExit(
            f"[install_live_hooks] FAIL-CLOSED: unsupported or unknown Langflow version {ver!r}; "
            f"supported: {', '.join(SUPPORTED_LANGFLOW_FAMILIES)}"
        )
    LANGFLOW_FAMILY = f"{m.group(1)}.{m.group(2)}"


SUPPORTED_LANGFLOW_FAMILIES = ("1.12", "1.13")
NATIVE_DISPATCHER_GUARD = (
    "    import os as _sflf_os\n"
    "    if _sflf_os.environ.get(\"SFLF_NATIVE_TRIGGER_DISPATCHER\", \"\").strip().lower() != \"enabled\":\n"
    "        logger.info(\"Native trigger dispatcher disabled: Snowflake Langflow triggers_manager owns trigger execution\")\n"
    "        return None\n"
)
SITE_PACKAGES = Path("/app/.venv/lib64/python3.14/site-packages")  # replaced by _configure_paths()
LANGFLOW_MAIN = SITE_PACKAGES / "langflow" / "main.py"
LANGFLOW_FRONTEND = SITE_PACKAGES / "langflow" / "frontend"
LFX_COMPONENTS_DIR = SITE_PACKAGES / "lfx" / "components"
LFX_INDEX_JSON = SITE_PACKAGES / "lfx" / "_assets" / "component_index.json"
COMPONENTS_ROOT = Path("/opt/standard-components")
SOURCE_ROOT = Path("/opt/snowflake-langflow")
PERSIST_SQLITE_DB = Path("/persist/langflow.db")
WRITABLE_CACHE_DIRS = (
    Path("/tmp/uv-cache"),
    Path("/tmp/.cache"),
    Path("/app/data/.cache/uv"),
)


def ensure_container_stability_and_sqlite_wal() -> None:
    """Configure writable UV_CACHE_DIR (0777) and SQLite WAL + busy_timeout=30000 on /persist/langflow.db."""
    # ENV UV_CACHE_DIR=/tmp/uv-cache XDG_CACHE_HOME=/tmp/.cache SQLITE_TMPDIR=/tmp LANGFLOW_A2A_ENABLED=true
    os.environ["UV_CACHE_DIR"] = os.environ.get("UV_CACHE_DIR") or "/tmp/uv-cache"
    os.environ["XDG_CACHE_HOME"] = os.environ.get("XDG_CACHE_HOME") or "/tmp/.cache"
    os.environ["SQLITE_TMPDIR"] = os.environ.get("SQLITE_TMPDIR") or "/tmp"
    os.environ["LANGFLOW_A2A_ENABLED"] = os.environ.get("LANGFLOW_A2A_ENABLED") or "true"

    for cache_dir in WRITABLE_CACHE_DIRS:
        try:
            cache_dir.mkdir(parents=True, exist_ok=True)
            os.chmod(cache_dir, 0o777)
        except OSError:
            pass

    if PERSIST_SQLITE_DB.exists() or PERSIST_SQLITE_DB.parent.exists():
        try:
            PERSIST_SQLITE_DB.parent.mkdir(parents=True, exist_ok=True)
            with sqlite3.connect(str(PERSIST_SQLITE_DB), timeout=30.0) as conn:
                conn.execute("PRAGMA busy_timeout=30000;")
                conn.execute("PRAGMA journal_mode=WAL;")
                conn.execute("PRAGMA synchronous=NORMAL;")
                conn.execute("PRAGMA temp_store=MEMORY;")
        except Exception as exc:
            print(f"[install_live_hooks] Warning configuring WAL on {PERSIST_SQLITE_DB}: {exc}")

    try:
        from sqlalchemy import event
        from sqlalchemy.engine import Engine

        if not getattr(Engine, "_sflf_sqlite_wal_listener_registered", False):

            @event.listens_for(Engine, "connect")
            def _sflf_set_sqlite_pragma(dbapi_connection, _connection_record) -> None:
                if isinstance(dbapi_connection, sqlite3.Connection) or "sqlite" in type(dbapi_connection).__module__.lower():
                    cursor = dbapi_connection.cursor()
                    try:
                        cursor.execute("PRAGMA busy_timeout=30000;")
                        cursor.execute("PRAGMA journal_mode=WAL;")
                        cursor.execute("PRAGMA synchronous=NORMAL;")
                        cursor.execute("PRAGMA temp_store=MEMORY;")
                    except Exception:
                        pass
                    finally:
                        cursor.close()

            setattr(Engine, "_sflf_sqlite_wal_listener_registered", True)
    except Exception:
        pass

OFFICIAL_SNOWFLAKE_PATH = (
    "M9.86 15.298l13.008 7.8a3.72 3.72 0 0 0 4.589-.601 4.01 4.01 0 0 0 1.227-2.908V3.956a3.81 3.81 0 0 0-1.861-3.42 "
    "3.81 3.81 0 0 0-3.893 0 3.81 3.81 0 0 0-1.861 3.42v8.896l-7.387-4.43a3.79 3.79 0 0 0-2.922-.4c-.986.265-1.818.94-2.3 "
    "1.844-1.057 1.9-.44 4.28 1.4 5.422m31.27 7.8l13.008-7.8c1.84-1.143 2.458-3.533 1.4-5.424a3.75 3.75 0 0 0-5.22-1.452l-7.3 "
    "4.37v-8.84a3.81 3.81 0 1 0-7.615 0v15.323a4.08 4.08 0 0 0 .494 2.367c.482.903 1.314 1.57 2.3 1.844a3.71 3.71 0 0 0 "
    "2.922-.4M29.552 31.97c.013-.25.108-.5.272-.68l1.52-1.58a1.06 1.06 0 0 1 .658-.282h.057a1.05 1.05 0 0 1 .656.282l1.52 "
    "1.58a1.12 1.12 0 0 1 .272.681v.06a1.13 1.13 0 0 1-.272.683l-1.52 1.58a1.04 1.04 0 0 1-.656.284h-.057c-.246-.014-.48-.115-.658-.284l-1.52-1.58a1.13 "
    "1.13 0 0 1-.272-.683zm-4.604-.65v1.364a1.54 1.54 0 0 0 .372.93l5.16 5.357a1.42 1.42 0 0 0 .895.386h1.312a1.42 1.42 0 0 0 "
    ".895-.386l5.16-5.357a1.54 1.54 0 0 0 .372-.93V31.32a1.54 1.54 0 0 0-.372-.93l-5.16-5.357a1.42 1.42 0 0 0-.895-.386h-1.312a1.42 "
    "1.42 0 0 0-.895.386L25.32 30.4a1.55 1.55 0 0 0-.372.93M3.13 27.62l7.365 4.417L3.13 36.45a4.06 4.06 0 0 0-1.399 5.424 3.75 "
    "3.75 0 0 0 2.3 1.844c.986.274 2.042.133 2.922-.392l13.008-7.8c1.2-.762 1.9-2.078 1.9-3.492a4.16 4.16 0 0 0-1.9-3.492l-13.008-7.8a3.79 "
    "3.79 0 0 0-2.922-.4c-.986.265-1.818.94-2.3 1.844-1.057 1.9-.44 4.278 1.4 5.422m38.995 4.442a4 4 0 0 0 1.91 3.477l13 7.8c.88.524 "
    "1.934.666 2.92.392s1.817-.94 2.3-1.843a4.05 4.05 0 0 0-1.4-5.424L53.5 32.038l7.365-4.417c1.84-1.143 2.457-3.53 1.4-5.422a3.74 "
    "3.74 0 0 0-2.3-1.844c-.987-.274-2.042-.134-2.92.4l-13 7.8a4 4 0 0 0-1.91 3.507M25.48 40.508a3.7 3.7 0 0 0-2.611.464l-13.008 "
    "7.8c-1.84 1.143-2.456 3.53-1.4 5.422.483.903 1.314 1.57 2.3 1.843a3.75 3.75 0 0 0 2.922-.392l7.387-4.43v8.83a3.81 3.81 0 1 0 "
    "7.614 0V44.4a3.91 3.91 0 0 0-3.205-3.903m28.66 8.276l-13.008-7.8a3.75 3.75 0 0 0-2.922-.392 3.74 3.74 0 0 0-2.3 1.843 4.09 "
    "4.09 0 0 0-.494 2.37v15.25a3.81 3.81 0 1 0 7.614 0V51.28l7.287 4.37a3.79 3.79 0 0 0 2.922.4c.986-.265 1.818-.94 2.3-1.844 "
    "1.057-1.9.44-4.28-1.4-5.422"
)


def _svg_data_uri(fill: str) -> str:
    raw = f'<svg xmlns="http://www.w3.org/2000/svg" width="96" height="96" viewBox="0 0 64 64" fill="{fill}"><path d="{OFFICIAL_SNOWFLAKE_PATH}"/></svg>'
    return "data:image/svg+xml;utf8," + urllib.parse.quote(raw)


# ---------------------------------------------------------------------------
# Fail-closed patch engine (W3-R1 Phase 2).
#
# Every text patch declares how many times its anchor must match on the base
# Langflow version being patched. A patch is OK when either the anchor matches
# exactly the declared count (it is applied), or the already-applied form is
# found that many times (layered release images re-run this installer). Any
# other outcome is recorded as a failure; `install_all` raises once with the
# full list, and `--report` prints counts without writing anything.
# ---------------------------------------------------------------------------

PATCH_LEDGER: list[dict] = []
REPORT_ONLY = False
LANGFLOW_FAMILY = ""  # "1.12" / "1.13", set by _configure_paths()

_JS_KEYWORDS = frozenset(
    "as do if in of for let new try var case else enum eval null this true void with await break catch class "
    "const false super throw while yield delete export import return static switch typeof default extends "
    "finally package private continue debugger function arguments interface protected implements instanceof "
    "async get set NaN URL Set Map".split()
)
# identifiers not preceded by a word char or a property dot; spread (`...x`) is still a binding
_IDENT_RE = re.compile(r"(?:(?<=\.\.\.)|(?<![\w$.]))[A-Za-z_$][\w$]*(?![\w$])")
_DQ_STRING_RE = re.compile(r'"(?:[^"\\]|\\.)*"')


def _is_minified_ident(tok: str, src: str, start: int, end: int) -> bool:
    """True for short bundler-generated identifiers that change on every Vite build."""
    nxt = src[end : end + 1]
    if tok in _JS_KEYWORDS or tok.startswith("_") or len(tok) > 3 or (tok == "$" and nxt == "{"):
        return False
    prev = src[start - 1 : start] if start else ""
    # object-literal keys (`{id:`, `,cx:`) are stable property names, not bindings
    if nxt == ":" and prev in ("{", ","):
        return False
    return True


def _mini_tokens(src: str) -> list[tuple[str, str]]:
    """Split JS source into ('lit', text) and ('id', name) parts; double-quoted strings stay literal."""
    out: list[tuple[str, str]] = []
    pos = 0
    strings = [(m.start(), m.end()) for m in _DQ_STRING_RE.finditer(src)]
    si = 0
    buf = []
    while pos < len(src):
        if si < len(strings) and strings[si][0] == pos:
            buf.append(src[pos : strings[si][1]])
            pos = strings[si][1]
            si += 1
            continue
        nxt_str = strings[si][0] if si < len(strings) else len(src)
        m = _IDENT_RE.search(src, pos, nxt_str)
        if not m:
            buf.append(src[pos:nxt_str])
            pos = nxt_str
            continue
        buf.append(src[pos : m.start()])
        if _is_minified_ident(m.group(0), src, m.start(), m.end()):
            out.append(("lit", "".join(buf)))
            buf = []
            out.append(("id", m.group(0)))
        else:
            buf.append(m.group(0))
        pos = m.end()
    out.append(("lit", "".join(buf)))
    return [p for p in out if p[1]]


def _mini_regex(src: str, prefix: str) -> tuple[str, dict[str, str]]:
    """Compile a literal minified snippet into a regex where minified names are captures/backrefs."""
    names: dict[str, str] = {}
    parts = []
    for kind, val in _mini_tokens(src):
        if kind == "lit":
            parts.append(re.escape(val))
        elif val in names:
            parts.append(f"(?P={names[val]})")
        else:
            names[val] = f"{prefix}{len(names)}"
            parts.append(rf"(?<![\w$])(?P<{names[val]}>[A-Za-z_$][\w$]*)(?![\w$])")
    return "".join(parts), names


def _expected_for(expect) -> tuple[int, int | None]:
    """Normalize an expectation to (min, max). `expect` may be int, (min, max) or {family: int|(min,max)}."""
    if isinstance(expect, dict):
        if LANGFLOW_FAMILY not in expect:
            raise RuntimeError(f"Patch has no declared match count for Langflow {LANGFLOW_FAMILY or '<unknown>'}")
        expect = expect[LANGFLOW_FAMILY]
    if isinstance(expect, tuple):
        return expect
    return int(expect), int(expect)


def _record(target: str, pid: str, status: str, found: int, applied: int, expect: tuple, detail: str = "") -> None:
    PATCH_LEDGER.append(
        {"target": target, "id": pid, "status": status, "found": found, "applied": applied, "expected": expect, "detail": detail}
    )


class BundlePatch:
    """One React-bundle patch.

    mode="mini": `orig`/`new` are literal snippets from a known bundle; short minified identifiers are
    matched by position (captures + backrefs), so the patch survives Vite renames as long as the code
    shape and string literals are unchanged. `symbols` maps a minified name that appears only in `new`
    to a resolver key in BUNDLE_SYMBOL_RESOLVERS (e.g. the Langflow logo component).
    mode="regex": `orig` is a regex, `new` a re.sub template, `applied` a regex for the patched form.
    """

    def __init__(self, pid, orig, new, *, mode="mini", applied=None, expect=1, symbols=None, reason="", hints=None, fresh=()):
        self.pid = pid
        self.origs = list(orig) if isinstance(orig, (list, tuple)) else [orig]
        self.origs_raw = list(self.origs)
        self.new = new
        self.mode = mode
        self.expect = expect
        self.symbols = symbols or {}
        self.reason = reason
        if mode == "literal":
            mode = self.mode = "regex"
            hints = hints or [*self.origs_raw, new]
            self.origs = [re.escape(o) for o in self.origs]
            applied = re.escape(new) if applied is None else applied
            self.new = new.replace("\\", "\\\\")
        if mode == "mini":
            self.patterns = []
            self.name_maps = []
            for i, o in enumerate(self.origs):
                rx, names = _mini_regex(o, f"m{i}_")
                self.patterns.append(re.compile(rx))
                self.name_maps.append(names)
            self.applied_rx = re.compile(_mini_regex(new, "a_")[0]) if applied is None else re.compile(applied)
        else:
            self.patterns = [re.compile(o) for o in self.origs]
            self.name_maps = [{} for _ in self.origs]
            if applied is None:
                raise ValueError(f"regex patch {pid} must declare its applied form")
            self.applied_rx = re.compile(applied)
        # cheap substring prefilters (longest stable literal of each alternative and of the applied form)
        self.hints = (
            [max((p for k, p in _mini_tokens(s) if k == "lit"), key=len, default="") for s in [*self.origs, new]]
            if mode == "mini"
            else list(hints or [])
        )
        # Minified names used only in `new` must be resolved from the bundle (`symbols`) or be fresh
        # locals introduced by the patch (`fresh`); anything else would bake a stale 1.12 name in.
        self.unbound = []
        if mode == "mini":
            for idx, names in enumerate(self.name_maps):
                for kind, val in _mini_tokens(new):
                    if kind == "id" and val not in names and val not in self.symbols and val not in fresh and val not in self.unbound:
                        self.unbound.append(val)

    def _template(self, idx: int, resolved: dict[str, str]):
        if self.mode != "mini":
            return self.new
        names = self.name_maps[idx]
        parts = []
        for kind, val in _mini_tokens(self.new):
            if kind == "lit":
                parts.append(val.replace("\\", "\\\\"))
            elif val in names:
                parts.append(f"\\g<{names[val]}>")
            elif val in resolved:
                parts.append(resolved[val])
            else:
                # identifier introduced by the patch itself (a fresh local such as `ev`)
                parts.append(val)
        return "".join(parts)


def _resolve_symbol(key: str, texts: dict[Path, str]) -> str | None:
    rx = re.compile(BUNDLE_SYMBOL_RESOLVERS[key])
    found = {m.group(1) for t in texts.values() for m in rx.finditer(t)}
    return found.pop() if len(found) == 1 else None


# Resolvers anchored on stable string literals: each must yield exactly one identifier.
BUNDLE_SYMBOL_RESOLVERS = {
    "LOGO": r'\.jsxs?\(([A-Za-z_$][\w$]*),\{"aria-hidden":"true",title:[\w$]+\("common\.langflowLogo"\)',
    # provider-name normalizer declared right after the provider display-alias map
    "PROVIDER_ALIAS": r'\{"IBM watsonx\.ai":"IBM WatsonX"[^}]*\},([A-Za-z_$][\w$]*)=',
}


def apply_bundle_patches(assets_dir: Path, patches: list[BundlePatch], prepend_index: str = "") -> None:
    """Apply `patches` across all JS chunks in `assets_dir` and record one ledger row per patch."""
    texts = {p: p.read_text(encoding="utf-8", errors="ignore") for p in sorted(assets_dir.glob("*.js"))}
    originals = dict(texts)
    for bp in patches:
        target = "frontend/assets"
        try:
            lo, hi = _expected_for(bp.expect)
        except RuntimeError as exc:
            _record(target, bp.pid, "FAIL", 0, 0, (None, None), str(exc))
            continue
        resolved = {}
        missing = []
        for name, key in bp.symbols.items():
            val = _resolve_symbol(key, texts)
            if val is None:
                missing.append(f"{name}->{key}")
            else:
                resolved[name] = val
        if bp.unbound:
            missing += [f"unbound:{n}" for n in bp.unbound]
        found = applied = 0
        plan = []
        for path, txt in texts.items():
            if bp.hints and not any(h and h in txt for h in bp.hints):
                continue
            spans = [m.span() for m in bp.applied_rx.finditer(txt)]
            applied += len(spans)
            for idx, rx in enumerate(bp.patterns):
                hits = [m for m in rx.finditer(txt) if not any(s <= m.start() < e for s, e in spans)]
                if hits:
                    found += len(hits)
                    plan.append((path, idx, spans))
        total = found + applied
        ok = lo <= total and (hi is None or total <= hi)
        if missing and found:
            ok = False
        status = "OK" if ok else "FAIL"
        if ok and found == 0:
            status = "ALREADY" if applied else "ABSENT-DECLARED"
        detail = bp.reason if status == "ABSENT-DECLARED" else ("unresolved symbols: " + ", ".join(missing) if missing and found else "")
        _record(target, bp.pid, status, found, applied, (lo, hi), detail)
        if not ok or REPORT_ONLY:
            continue
        for path, idx, spans in plan:
            tmpl = bp._template(idx, resolved)

            def _sub(m, _spans=spans, _tmpl=tmpl):
                if any(s <= m.start() < e for s, e in _spans):
                    return m.group(0)
                return m.expand(_tmpl)

            texts[path] = bp.patterns[idx].sub(_sub, texts[path])
    if prepend_index:
        for path in texts:
            if path.name.startswith("index-") and "__sflfPatched" not in texts[path] and _is_entry_chunk(path):
                texts[path] = prepend_index + texts[path]
    if REPORT_ONLY:
        return
    for path, txt in texts.items():
        if txt != originals[path]:
            path.write_text(txt, encoding="utf-8")
            print(f"[install_live_hooks] Patched React bundle {path.name}")


def _is_entry_chunk(path: Path) -> bool:
    index_html = path.parent.parent / "index.html"
    try:
        return f"assets/{path.name}" in index_html.read_text(encoding="utf-8")
    except OSError:
        return False


def counted_replace(text: str, target: str, pid: str, old: str, new: str, *, applied: str | None = None, expect=1, reason: str = "") -> str:
    """In-memory exact-string patch with a declared match count; records the outcome in PATCH_LEDGER."""
    try:
        lo, hi = _expected_for(expect)
    except RuntimeError as exc:
        _record(target, pid, "FAIL", 0, 0, (None, None), str(exc))
        return text
    marker = new if applied is None else applied
    n_applied = text.count(marker) if marker and marker not in old else 0
    if n_applied and n_applied >= max(lo, 1):
        _record(target, pid, "ALREADY", 0, n_applied, (lo, hi))
        return text
    found = text.count(old)
    ok = lo <= found and (hi is None or found <= hi)
    status = ("OK" if found else "ABSENT-DECLARED") if ok else "FAIL"
    _record(target, pid, status, found, n_applied, (lo, hi), reason if status == "ABSENT-DECLARED" else "")
    return text.replace(old, new) if ok and found else text


def text_patch(path: Path, pid: str, old: str, new: str, *, applied: str | None = None, expect=1, reason: str = "") -> bool:
    """Exact-string patch on a Python/HTML file with a declared match count. Returns True if written."""
    target = str(path.relative_to(SITE_PACKAGES)) if path.is_relative_to(SITE_PACKAGES) else path.name
    try:
        lo, hi = _expected_for(expect)
    except RuntimeError as exc:
        _record(target, pid, "FAIL", 0, 0, (None, None), str(exc))
        return False
    if not path.exists():
        status = "ABSENT-DECLARED" if hi == 0 else "FAIL"
        _record(target, pid, status, 0, 0, (lo, hi), reason or "file not found")
        return False
    txt = path.read_text(encoding="utf-8")
    patched = counted_replace(txt, target, pid, old, new, applied=applied, expect=expect, reason=reason)
    if patched != txt and not REPORT_ONLY:
        path.write_text(patched, encoding="utf-8")
        return True
    return False


# index.html Snowflake block: also consumes earlier notranslate meta + DOM-guard script copies,
# which older release layers stacked once per build.
# Lazy chunks import the entry as `./index-<hash>.js`; a query string on the <script> URL makes the
# browser evaluate the entry twice (two module records, two React roots on #root). Freshness comes
# from the bridge sending `Cache-Control: no-cache` on /assets/*.js instead.
ENTRY_SCRIPT_RE = r'src="(\./assets/index-[^"?]+\.js)(?:\?[^"]*)?"'
ENTRY_SCRIPT_REPL = r'src="\1"'

# Langflow 1.13 builds with ENABLE_TRIGGERS=false and then drops the whole `triggers` sidebar category,
# which hides the Snowflake triggers too. Keep the upstream per-component filter (components with
# metadata.trigger_kind, i.e. the native 1.13 triggers whose dispatcher is disabled) and stop dropping
# the category. The native category entry is removed so Triggers is listed once, after Input & Output.
TRIGGERS_KEEP_CATEGORY_ORIG = "function kfo(e){let t=!1;const n=[];for(const[i,s]of Object.entries(e)){if(i===Rfo){t=!0;continue}"
TRIGGERS_KEEP_CATEGORY_NEW = "function kfo(e){let t=!1;const n=[];for(const[i,s]of Object.entries(e)){if(!1){t=!0;continue}"
TRIGGERS_NATIVE_ENTRY_ORIG = (
    '{display_name:"sidebar.category.flowControl",name:"flow_controls",icon:"ArrowRightLeft"},'
    '{display_name:"sidebar.category.triggers",name:"triggers",icon:"Clock"},'
    '{display_name:"sidebar.category.utilities"'
)
TRIGGERS_NATIVE_ENTRY_NEW = (
    '{display_name:"sidebar.category.flowControl",name:"flow_controls",icon:"ArrowRightLeft"},'
    '{display_name:"sidebar.category.utilities"'
)

INJECT_BLOCK_REFRESH_RE = (
    r"[ \t]*(?:<meta name=\"google\" content=\"notranslate\" />\s*(?:<script>[\s\S]*?</script>\s*)?)*"
    r"<style(?:\s+id=\"sflf-brand-blue-style\")?>[\s\S]*?</style>\s*(?:<script defer src=\"\./preserved_provisioning_and_memories_concept\.js[^\"]*\"></script>\s*)?<script defer src=\"\./snowflake-ui-integration\.js[^\"]*\"></script>\s*</head>"
)


def _html_sub(text: str, pid: str, pattern: str, repl, applied: str | None, *, expect=1, count: int = 0) -> str:
    """Counted regex substitution on index.html; `applied` short-circuits when already patched."""
    lo, hi = _expected_for(expect)
    if applied and re.search(applied, text):
        _record("frontend/index.html", pid, "ALREADY", 0, len(re.findall(applied, text)), (lo, hi))
        return text
    found = len(re.findall(pattern, text, flags=re.IGNORECASE))
    ok = lo <= found and (hi is None or found <= hi)
    _record("frontend/index.html", pid, "OK" if ok else "FAIL", found, 0, (lo, hi))
    if not ok:
        return text
    return re.sub(pattern, repl, text, count=count, flags=re.IGNORECASE)


def format_patch_report() -> str:
    lines = [f"{'status':<16} {'found':>5} {'appl':>5} {'expect':>9}  target :: id"]
    for r in PATCH_LEDGER:
        lo, hi = r["expected"]
        exp = f"{lo}" if lo == hi else f"{lo}..{'*' if hi is None else hi}"
        line = f"{r['status']:<16} {r['found']:>5} {r['applied']:>5} {exp:>9}  {r['target']} :: {r['id']}"
        if r["detail"]:
            line += f"  ({r['detail']})"
        lines.append(line)
    fails = [r for r in PATCH_LEDGER if r["status"] == "FAIL"]
    lines.append(f"[install_live_hooks] {len(PATCH_LEDGER)} patches, {len(fails)} failed (Langflow {LANGFLOW_FAMILY})")
    return "\n".join(lines)


def assert_patch_ledger_clean() -> None:
    fails = [r for r in PATCH_LEDGER if r["status"] == "FAIL"]
    if fails:
        raise SystemExit(
            "[install_live_hooks] FAIL-CLOSED: patch anchors did not match their declared count:\n"
            + "\n".join(f"  - {r['target']} :: {r['id']} found={r['found']} applied={r['applied']} expected={r['expected']} {r['detail']}" for r in fails)
        )


def _patch_upstream_models_and_agents_files() -> None:
    """Patch upstream `language_model.py`, `agent.py`, `a2a_agent.py`, and `unified_models/__init__.py` (`MA-W2-R6-02..05`)."""
    import importlib
    import sys

    um_init = SITE_PACKAGES / "lfx" / "base" / "models" / "unified_models" / "__init__.py"
    if um_init.exists():
        um_txt = um_init.read_text(encoding="utf-8")
        if "register_snowflake_language_model_providers" not in um_txt:
            um_txt += (
                "\n# Snowflake Langflow provider registration\n"
                "try:\n"
                "    from lfx_snowflake.providers.embeddings import register_snowflake_cortex_embedding_provider as _reg_sf_emb\n"
                "    _reg_sf_emb()\n"
                "except Exception:\n"
                "    pass\n"
                "try:\n"
                "    from lfx_snowflake.providers.language_models import register_snowflake_language_model_providers as _reg_sf_lm\n"
                "    _reg_sf_lm()\n"
                "except Exception:\n"
                "    pass\n"
            )
            _record("lfx/base/models/unified_models/__init__.py", "unified_models.auto_registration", "OK", 1, 0, (1, 1))
            if not REPORT_ONLY:
                um_init.write_text(um_txt, encoding="utf-8")
                print("[install_live_hooks] Patched lfx/base/models/unified_models/__init__.py with auto-registration")
        else:
            _record("lfx/base/models/unified_models/__init__.py", "unified_models.auto_registration", "ALREADY", 0, 1, (1, 1))
    else:
        _record("lfx/base/models/unified_models/__init__.py", "unified_models.auto_registration", "FAIL", 0, 0, (1, 1), "file not found")

    ma_dir = LFX_COMPONENTS_DIR / "models_and_agents"
    if not ma_dir.exists():
        _record("lfx/components/models_and_agents", "models_and_agents.dir", "FAIL", 0, 0, (1, 1), "directory not found")
        return

    # 1. Patch language_model.py on disk so LanguageModelComponent exposes Snowflake providers & reasoning inputs right after model
    lm_file = ma_dir / "language_model.py"
    if lm_file.exists():
        lm_txt = lm_file.read_text(encoding="utf-8")
        lm_orig = lm_txt
        lm_txt = counted_replace(
            lm_txt,
            "lfx/components/models_and_agents/language_model.py",
            "lm.import_providers",
            "from lfx.base.models.unified_models import (\n"
            "    get_language_model_options,\n"
            "    get_llm,\n"
            "    handle_model_input_update,\n"
            ")",
            "from lfx_snowflake.providers.language_models import (\n"
            "    get_snowflake_reasoning_inputs,\n"
            "    register_snowflake_language_model_providers,\n"
            ")\n"
            "register_snowflake_language_model_providers()\n"
            "from lfx.base.models.unified_models import (\n"
            "    get_language_model_options,\n"
            "    get_llm,\n"
            "    handle_model_input_update,\n"
            ")",
            applied="register_snowflake_language_model_providers",
        )
        lm_txt = counted_replace(
            lm_txt,
            "lfx/components/models_and_agents/language_model.py",
            "lm.reasoning_kwargs",
            '            ollama_base_url=getattr(self, "ollama_base_url", None),\n        )',
            '            ollama_base_url=getattr(self, "ollama_base_url", None),\n'
            '            reasoning_effort=getattr(self, "reasoning_effort", None),\n'
            '            extended_thinking=getattr(self, "extended_thinking", "disabled"),\n'
            '            thinking_budget_tokens=int(getattr(self, "thinking_budget_tokens", 2048) or 2048),\n'
            "        )",
            applied='reasoning_effort=getattr(self, "reasoning_effort", None)',
        )
        # Ensure *get_snowflake_reasoning_inputs() is placed right after ModelInput(name="model", ...) and not at the bottom
        lm_txt = counted_replace(
                lm_txt,
                "lfx/components/models_and_agents/language_model.py",
                "lm.legacy_bottom_inputs",
            '            range_spec=RangeSpec(min=0, max=128000, step=1, step_type="int"),\n        ),\n        *get_snowflake_reasoning_inputs(),\n    ]',
            '            range_spec=RangeSpec(min=0, max=128000, step=1, step_type="int"),\n        ),\n    ]',
                expect=(0, 1), reason="legacy bottom placement not present",
            )
        lm_txt = counted_replace(
            lm_txt,
            "lfx/components/models_and_agents/language_model.py",
            "lm.reasoning_inputs_after_model",
            '        ModelInput(\n'
            '            name="model",\n'
            '            display_name="Language Model",\n'
            '            info="Select your model provider",\n'
            '            real_time_refresh=True,\n'
            '            required=True,\n'
            '        ),',
            '        ModelInput(\n'
            '            name="model",\n'
            '            display_name="Language Model",\n'
            '            info="Select your model provider",\n'
            '            real_time_refresh=True,\n'
            '            required=True,\n'
            '        ),\n'
            '        *get_snowflake_reasoning_inputs(),',
            applied="*get_snowflake_reasoning_inputs()",
        )
        if lm_txt != lm_orig and not REPORT_ONLY:
            lm_file.write_text(lm_txt, encoding="utf-8")
            print("[install_live_hooks] Patched lfx/components/models_and_agents/language_model.py")

    # 2. Patch agent.py on disk so AgentComponent exposes Snowflake providers, reasoning inputs right after model, and multi-tool is_list=True
    ag_file = ma_dir / "agent.py"
    if ag_file.exists():
        ag_txt = ag_file.read_text(encoding="utf-8")
        ag_orig = ag_txt
        ag_txt = counted_replace(
            ag_txt,
            "lfx/components/models_and_agents/agent.py",
            "agent.import_providers",
            "from lfx.base.models.unified_models import (\n"
            "    get_language_model_options,\n"
            "    get_llm,\n"
            "    handle_model_input_update,\n"
            ")",
            "from lfx_snowflake.providers.language_models import (\n"
            "    get_snowflake_reasoning_inputs,\n"
            "    register_snowflake_language_model_providers,\n"
            ")\n"
            "register_snowflake_language_model_providers()\n"
            "from lfx.base.models.unified_models import (\n"
            "    get_language_model_options,\n"
            "    get_llm,\n"
            "    handle_model_input_update,\n"
            ")",
            applied="register_snowflake_language_model_providers",
        )
        ag_txt = counted_replace(
            ag_txt,
            "lfx/components/models_and_agents/agent.py",
            "agent.reasoning_kwargs",
            '            overrides=getattr(self, "_model_overrides", None),\n        )',
            '            overrides=getattr(self, "_model_overrides", None),\n'
            '            reasoning_effort=getattr(self, "reasoning_effort", None),\n'
            '            extended_thinking=getattr(self, "extended_thinking", "disabled"),\n'
            '            thinking_budget_tokens=int(getattr(self, "thinking_budget_tokens", 2048) or 2048),\n'
            "        )",
            applied='reasoning_effort=getattr(self, "reasoning_effort", None)',
        )
        # Ensure *get_snowflake_reasoning_inputs() is placed right after ModelInput(name="model", ...) and not at the bottom
        ag_txt = counted_replace(
                ag_txt,
                "lfx/components/models_and_agents/agent.py",
                "agent.legacy_bottom_inputs",
            '            info=(\n'
            '                "If true, adds a zero-config arithmetic calculator tool to the agent "\n'
            '                "(safe: only +, -, *, /, ** operators via AST)."\n'
            "            ),\n"
            "            value=True,\n"
            "        ),\n"
            "        *get_snowflake_reasoning_inputs(),\n"
            "    ]",
            '            info=(\n'
            '                "If true, adds a zero-config arithmetic calculator tool to the agent "\n'
            '                "(safe: only +, -, *, /, ** operators via AST)."\n'
            "            ),\n"
            "            value=True,\n"
            "        ),\n"
            "    ]",
                expect=(0, 1), reason="legacy bottom placement not present",
            )
        ag_txt = counted_replace(
            ag_txt,
            "lfx/components/models_and_agents/agent.py",
            "agent.reasoning_inputs_after_model",
            '            filters={"tool_calling": True},\n        ),',
            '            filters={"tool_calling": True},\n        ),\n        *get_snowflake_reasoning_inputs(),',
            applied="*get_snowflake_reasoning_inputs()",
        )
        if ag_txt != ag_orig and not REPORT_ONLY:
            ag_file.write_text(ag_txt, encoding="utf-8")
            print("[install_live_hooks] Patched lfx/components/models_and_agents/agent.py")

    # 3. Patch a2a_agent.py on disk so A2AAgentComponent supports Internal & External Langflow A2A Flows + Snowflake Cortex Agents
    a2a_file = ma_dir / "a2a_agent.py"
    if a2a_file.exists():
        a2a_txt = a2a_file.read_text(encoding="utf-8")
        if "quick_fill_endpoint" not in a2a_txt or REPORT_ONLY:
            a2a_txt = counted_replace(
                a2a_txt,
                "lfx/components/models_and_agents/a2a_agent.py",
                "a2a.quick_fill_endpoint",
                '        # External: a remote A2A agent by URL.\n'
                '        MessageTextInput(\n'
                '            name="agent_url",',
                '        DropdownInput(\n'
                '            name="quick_fill_endpoint",\n'
                '            display_name="Quick Fill Endpoint (Langflow A2A Flows & Snowflake Cortex Agents)",\n'
                '            info="Select a local Langflow A2A Flow or a [ ❄️ Context ] Snowflake Cortex Agent to auto-fill Agent URL.",\n'
                '            options=[],\n'
                '            value="",\n'
                '            real_time_refresh=True,\n'
                '            refresh_button=True,\n'
                '            combobox=True,\n'
                '            required=False,\n'
                '            show=True,\n'
                '        ),\n'
                '        # External: a remote A2A agent by URL.\n'
                '        MessageTextInput(\n'
                '            name="agent_url",',
                applied="quick_fill_endpoint",
            )
            a2a_patch_block = '''

    async def _populate_internal_agents(self, build_config: dotdict) -> None:
        """Fill the Internal dropdown with both published Langflow A2A flows and [ ❄️ Context ] Snowflake Cortex Agents (`MA-W2-R6-05`)."""
        from lfx_snowflake.fastapi_live_bridge import LIVE_BRIDGE
        from lfx_snowflake.context_helpers import _resolve_flow_id

        flow_id = _resolve_flow_id(self, build_config)
        options: list[str] = []
        metadata: list[dict] = []
        try:
            agents = await self.alist_a2a_agents_by_flow_folder()
            for agent in agents:
                fid = str(agent.data["id"])
                fname = str(agent.data["name"])
                lbl = f"[Langflow Flow] {fname} ({fid})"
                options.append(lbl)
                metadata.append({"id": fid, "kind": "langflow_flow", "updated_at": agent.data.get("updated_at")})
        except Exception:  # noqa: BLE001
            pass

        sf_targets = LIVE_BRIDGE.list_internal_and_external_a2a_targets(flow_id=flow_id)
        for lbl, meta in zip(sf_targets.get("internal_options", []), sf_targets.get("internal_metadata", [])):
            if lbl not in options:
                options.append(lbl)
                metadata.append(meta)

        build_config["agent_name_selected"]["options"] = options
        build_config["agent_name_selected"]["options_metadata"] = metadata
        if options and build_config["agent_name_selected"].get("value") not in options:
            build_config["agent_name_selected"]["value"] = options[0]

    def _populate_quick_fill_endpoints(self, build_config: dotdict) -> dict[str, str]:
        from lfx_snowflake.fastapi_live_bridge import LIVE_BRIDGE
        from lfx_snowflake.context_helpers import _resolve_flow_id

        flow_id = _resolve_flow_id(self, build_config)
        sf_targets = LIVE_BRIDGE.list_internal_and_external_a2a_targets(flow_id=flow_id)
        qf_opts = list(sf_targets.get("quick_fill_options", []))
        qf_map = dict(sf_targets.get("quick_fill_map", {}))
        if "quick_fill_endpoint" in build_config:
            build_config["quick_fill_endpoint"]["options"] = qf_opts
        return qf_map

    async def update_build_config(
        self,
        build_config: dotdict,
        field_value: Any,
        field_name: str | None = None,
    ) -> dotdict:
        qf_map = self._populate_quick_fill_endpoints(build_config)
        if field_name == "mode":
            internal = field_value == "Internal"
            build_config["agent_name_selected"]["show"] = internal
            if "quick_fill_endpoint" in build_config:
                build_config["quick_fill_endpoint"]["show"] = not internal
            build_config["agent_url"]["show"] = not internal
            build_config["agent_url"]["required"] = not internal
            build_config["api_key"]["show"] = not internal
            build_config["timeout"]["show"] = not internal
            if internal:
                build_config["agent_card"]["show"] = False
                await self._populate_internal_agents(build_config)
            else:
                await self._apply_external_card(build_config, build_config["agent_url"].get("value"))
        elif field_name == "agent_name_selected" and (build_config.get("is_refresh") or field_value is None):
            await self._populate_internal_agents(build_config)
        elif field_name == "quick_fill_endpoint" and isinstance(field_value, str) and field_value:
            resolved_url = qf_map.get(field_value) or (field_value.split("->", 1)[1].strip() if "->" in field_value else field_value)
            if "agent_url" in build_config and resolved_url:
                build_config["agent_url"]["value"] = resolved_url
                await self._apply_external_card(build_config, resolved_url)
        elif field_name == "agent_url":
            await self._apply_external_card(build_config, field_value)
        return build_config

    async def _run_internal_agent(self) -> Message:
        import json as _json
        import re as _re
        from lfx_snowflake.fastapi_live_bridge import LIVE_BRIDGE
        from lfx_snowflake.context_helpers import _resolve_flow_id

        agent_name = (self.agent_name_selected or "").strip()
        if not agent_name:
            raise ValueError("Select an A2A agent ([Langflow Flow] or [Cortex Agent]) to call.")

        if agent_name.startswith("[Cortex Agent]") or (not agent_name.startswith("[Langflow Flow]") and agent_name.count(".") == 2):
            fqn = agent_name.replace("[Cortex Agent]", "").strip()
            parts = [p.strip() for p in fqn.split(".") if p.strip()]
            if len(parts) != 3:
                raise ValueError(f"Invalid Cortex Agent FQN: {agent_name!r}")
            flow_id = _resolve_flow_id(self) or "default"
            sess_id = getattr(getattr(self, "graph", None), "session_id", None) or flow_id
            res = LIVE_BRIDGE.invoke_cortex_agent(
                database=parts[0],
                schema=parts[1],
                agent_name=parts[2],
                user_message=str(self.input_value or ""),
                flow_id=flow_id,
                session_id=str(sess_id),
                resume_thread=True,
            )
            if not res.get("ok", True):
                raise ValueError(str(res.get("message") or "Cortex Agent invocation failed"))
            msg = Message(text=str(res.get("text") or _json.dumps(res, indent=2)))
            self.status = res.get("thread_indicator") or msg
            return msg

        from lfx.helpers import get_flow_by_id_or_name

        m_fid = _re.search(r"\\(([^()]+)\\)\\s*$", agent_name)
        explicit_fid = m_fid.group(1).strip() if m_fid else None
        published = await self.alist_a2a_agents_by_flow_folder()
        published_ids = {str(agent.data["id"]) for agent in published}
        clean_name = _re.sub(r"^\\[Langflow Flow\\]\\s*", "", agent_name)
        clean_name = _re.sub(r"\\s*\\([^()]+\\)\\s*$", "", clean_name).strip()
        flow_id = explicit_fid or self._selected_agent_flow_id() or self._agent_id_by_name(published, clean_name)
        if flow_id is None or (published_ids and flow_id not in published_ids and explicit_fid is None):
            raise ValueError(
                f"Agent flow '{agent_name}' is not published as an A2A agent in this project. "
                "Turn on 'Serve as an A2A agent' for it, or select another agent."
            )
        flow = await get_flow_by_id_or_name(user_id=self.user_id, flow_id=flow_id)
        if not flow or not flow.data:
            raise ValueError(f"Agent flow '{agent_name}' could not be found.")

        graph = await self.load_flow(flow_id)
        run_outputs = await self._run_target_isolated(graph, flow_id)
        answer = self._reply_from_run(run_outputs)
        message = Message(text=answer or "No response received from the agent.")
        self.status = message
        return message

    async def _call_external_agent(self) -> Message:
        import json as _json
        import re as _re
        from lfx_snowflake.fastapi_live_bridge import LIVE_BRIDGE
        from lfx_snowflake.context_helpers import _resolve_flow_id

        raw_url = str(self.agent_url or "").strip()
        if "->" in raw_url:
            raw_url = raw_url.split("->", 1)[1].strip()

        m_run = _re.search(
            r"/api/v2/databases/([^/]+)/schemas/([^/]+)/agents/([^/:]+):run",
            raw_url,
            flags=_re.IGNORECASE,
        )
        if m_run:
            flow_id = _resolve_flow_id(self) or "default"
            sess_id = getattr(getattr(self, "graph", None), "session_id", None) or flow_id
            res = LIVE_BRIDGE.invoke_cortex_agent(
                database=m_run.group(1),
                schema=m_run.group(2),
                agent_name=m_run.group(3),
                user_message=str(self.input_value or ""),
                flow_id=flow_id,
                session_id=str(sess_id),
                resume_thread=True,
            )
            if not res.get("ok", True):
                raise ValueError(str(res.get("message") or "Cortex Agent REST :run failed"))
            message = Message(text=str(res.get("text") or _json.dumps(res, indent=2)))
            self.status = res.get("thread_indicator") or message
            return message

        m_bridge = _re.search(
            r"/api/v1/snowflake/a2a/cortex-agents/([^/]+)/([^/]+)/([^/.]+)",
            raw_url,
            flags=_re.IGNORECASE,
        )
        if m_bridge and (not raw_url.startswith("http") or "127.0.0.1" in raw_url or "localhost" in raw_url):
            flow_id = _resolve_flow_id(self) or "default"
            sess_id = getattr(getattr(self, "graph", None), "session_id", None) or flow_id
            rpc = LIVE_BRIDGE.handle_cortex_agent_a2a_jsonrpc(
                m_bridge.group(1),
                m_bridge.group(2),
                m_bridge.group(3),
                {
                    "jsonrpc": "2.0",
                    "id": "a2a-ext-1",
                    "method": "message/send",
                    "params": {
                        "message": {
                            "role": "user",
                            "contextId": str(sess_id),
                            "parts": [{"kind": "text", "text": str(self.input_value or "")}],
                        }
                    },
                },
                flow_id=flow_id,
            )
            if "error" in rpc:
                raise ValueError(str(rpc["error"].get("message") or "A2A Server Bridge error"))
            task_res = rpc.get("result") or {}
            parts = ((task_res.get("status") or {}).get("message") or {}).get("parts") or []
            reply_txt = "\\n".join(str(p.get("text") or "") for p in parts if isinstance(p, dict) and p.get("text"))
            message = Message(text=reply_txt or "No response received from the A2A agent.")
            self.status = message
            return message

        try:
            timeout = float(self.timeout)
        except (TypeError, ValueError):
            timeout = DEFAULT_TIMEOUT

        agent_url = _agent_base_url(raw_url)
        try:
            _validated_url, validated_ips = await asyncio.to_thread(validate_and_resolve_connector_url, agent_url)
        except SSRFProtectionError as e:
            msg = f"SSRF Protection: {e}"
            raise ValueError(msg) from e

        client = build_a2a_client(agent_url, validated_ips, api_key=self.api_key, timeout=timeout)
        try:
            async with client:
                answer = await call_a2a_agent(agent_url, self.input_value, httpx_client=client)
        except SSRFProtectionError as e:
            msg = f"SSRF Protection: {e}"
            raise ValueError(msg) from e
        except Exception as e:
            msg = f"Failed to call A2A agent at {agent_url}: {e}"
            raise ValueError(msg) from e

        message = Message(text=answer or "No response received from the A2A agent.")
        self.status = message
        return message
'''
            if "quick_fill_endpoint" in a2a_txt and "_populate_quick_fill_endpoints" not in a2a_txt and not REPORT_ONLY:
                a2a_txt = a2a_txt.rstrip() + "\n" + a2a_patch_block
                a2a_file.write_text(a2a_txt, encoding="utf-8")
                print("[install_live_hooks] Patched lfx/components/models_and_agents/a2a_agent.py")

    for mod_name in (
        "lfx.components.models_and_agents.language_model",
        "lfx.components.models_and_agents.agent",
        "lfx.components.models_and_agents.a2a_agent",
    ):
        if mod_name in sys.modules:
            try:
                importlib.reload(sys.modules[mod_name])
            except Exception as exc:  # noqa: BLE001
                print(f"[install_live_hooks] Warning reloading {mod_name}: {exc}")


def patch_runtime_sources() -> None:
    """Steps 4-6: patch langflow/main.py, the native trigger dispatcher, the React bundle and index.html."""
    # 4. Patch langflow/main.py to mount /api/v1/snowflake/* routes + SQLite WAL / busy_timeout=30000 + UV_CACHE_DIR + LANGFLOW_A2A_ENABLED
    main_text = LANGFLOW_MAIN.read_text(encoding="utf-8")
    hook_snippet = (
        "    load_plugin_routes(app)\n"
        "    import os as _sflf_os, sqlite3 as _sflf_sqlite3\n"
        "    from pathlib import Path as _SflfPath\n"
        "    _sflf_os.environ.setdefault('UV_CACHE_DIR', '/tmp/uv-cache')\n"
        "    _sflf_os.environ.setdefault('XDG_CACHE_HOME', '/tmp/.cache')\n"
        "    _sflf_os.environ.setdefault('SQLITE_TMPDIR', '/tmp')\n"
        "    _sflf_os.environ.setdefault('LANGFLOW_A2A_ENABLED', 'true')\n"
        "    for _d in ('/tmp/uv-cache', '/tmp/.cache', '/app/data/.cache/uv'):\n"
        "        try:\n"
        "            _SflfPath(_d).mkdir(parents=True, exist_ok=True)\n"
        "            _sflf_os.chmod(_d, 0o777)\n"
        "        except OSError:\n"
        "            pass\n"
        "    try:\n"
        "        from sqlalchemy import event as _sflf_sa_event\n"
        "        from sqlalchemy.engine import Engine as _SflfSaEngine\n"
        "        if not getattr(_SflfSaEngine, '_sflf_sqlite_wal_listener_registered', False):\n"
        "            @_sflf_sa_event.listens_for(_SflfSaEngine, 'connect')\n"
        "            def _sflf_on_sqlite_connect(dbapi_conn, _conn_rec):\n"
        "                if isinstance(dbapi_conn, _sflf_sqlite3.Connection) or 'sqlite' in type(dbapi_conn).__module__.lower():\n"
        "                    _cur = dbapi_conn.cursor()\n"
        "                    try:\n"
        "                        _cur.execute('PRAGMA busy_timeout=30000;')\n"
        "                        _cur.execute('PRAGMA journal_mode=WAL;')\n"
        "                        _cur.execute('PRAGMA synchronous=NORMAL;')\n"
        "                        _cur.execute('PRAGMA temp_store=MEMORY;')\n"
        "                    except Exception:\n"
        "                        pass\n"
        "                    finally:\n"
        "                        _cur.close()\n"
        "            setattr(_SflfSaEngine, '_sflf_sqlite_wal_listener_registered', True)\n"
        "    except Exception:\n"
        "        pass\n"
        "    try:\n"
        "        from lfx_snowflake.providers.embeddings import register_snowflake_cortex_embedding_provider\n"
        "        register_snowflake_cortex_embedding_provider()\n"
        "    except Exception:\n"
        "        pass\n"
        "    try:\n"
        "        from lfx_snowflake.providers.language_models import register_snowflake_language_model_providers\n"
        "        register_snowflake_language_model_providers()\n"
        "    except Exception:\n"
        "        pass\n"
        "    try:\n"
        "        from lfx_snowflake.fastapi_live_bridge import register_snowflake_vector_store_backends\n"
        "        register_snowflake_vector_store_backends()\n"
        "    except Exception:\n"
        "        pass\n"
        "    from lfx_snowflake.fastapi_live_bridge import register_snowflake_fastapi_routes\n"
        "    register_snowflake_fastapi_routes(app)\n"
    )
    if "register_snowflake_fastapi_routes(app)" in main_text and "register_snowflake_language_model_providers" not in main_text:
        # upgrade an older hook block written by a previous release layer
        upgraded, n_up = re.subn(
            r"    load_plugin_routes\(app\)\n[\s\S]*?register_snowflake_fastapi_routes\(app\)\n",
            lambda _m: hook_snippet,
            main_text,
            count=1,
        )
        _record("langflow/main.py", "main.hook_upgrade", "OK" if n_up == 1 else "FAIL", n_up, 0, (1, 1))
        if n_up == 1 and not REPORT_ONLY:
            LANGFLOW_MAIN.write_text(upgraded, encoding="utf-8")
            print("[install_live_hooks] Upgraded langflow/main.py hook with Snowflake Language Model providers, Cortex embeddings & vector store backends")
    elif text_patch(
        LANGFLOW_MAIN,
        "main.load_plugin_routes",
        "    load_plugin_routes(app)\n",
        hook_snippet,
        applied="register_snowflake_fastapi_routes(app)",
    ):
        print("[install_live_hooks] Patched langflow/main.py with SQLite WAL connect listener and register_snowflake_fastapi_routes(app)")

    # 4b. Langflow 1.13 ships a native leased trigger dispatcher that also runs the schedule tick producer.
    # The five Snowflake triggers (schedule, table change, stage file, alert, webhook) are driven by
    # lfx_snowflake.triggers_manager, so the native loop is disabled unless explicitly opted in with
    # SFLF_NATIVE_TRIGGER_DISPATCHER=enabled. This prevents two schedulers firing the same flow, and
    # it keeps native trigger rows (if any) inert instead of half-managed. 1.12 has no native dispatcher.
    text_patch(
        SITE_PACKAGES / "langflow" / "services" / "triggers" / "dispatcher.py",
        "triggers.native_dispatcher_guard",
        "    if not get_settings_service().settings.trigger_dispatcher_enabled:\n        return None\n",
        NATIVE_DISPATCHER_GUARD
        + "    if not get_settings_service().settings.trigger_dispatcher_enabled:\n        return None\n",
        applied="SFLF_NATIVE_TRIGGER_DISPATCHER",
        expect={"1.12": 0, "1.13": 1},
        reason="Langflow 1.12 has no native trigger dispatcher",
    )

    # 4c. Offload synchronous build_custom_component_template in custom_component_update to a worker thread
    # so concurrent component template refreshes never block the Uvicorn event loop (which would starve /health
    # and cause SPCS ingress to return HTTP 503 ERROR_SERVICE_UNAVAILABLE -> "Error while updating the Component").
    text_patch(
        SITE_PACKAGES / "langflow" / "api" / "v1" / "endpoints.py",
        "endpoints.custom_component_update_async_thread_113",
        "        component_node, cc_instance = build_custom_component_template(\n"
        "            component,\n"
        "            user_id=user.id,\n"
        "            class_name=code_request.get_class_name(),\n"
        "        )\n",
        "        component_node, cc_instance = await asyncio.to_thread(\n"
        "            build_custom_component_template,\n"
        "            component,\n"
        "            user_id=user.id,\n"
        "            class_name=code_request.get_class_name(),\n"
        "        )\n",
        applied="await asyncio.to_thread(\n            build_custom_component_template,",
        expect={"1.12": 0, "1.13": (0, 1)},
        reason="1.12 or 1.13.2 custom_component_update signature variant",
    )
    text_patch(
        SITE_PACKAGES / "langflow" / "api" / "v1" / "endpoints.py",
        "endpoints.custom_component_update_async_thread_113_v2",
        "        built_frontend_node, component_instance = build_custom_component_template(\n"
        "            component, user_id=user.id, class_name=raw_code.get_class_name()\n"
        "        )\n",
        "        built_frontend_node, component_instance = await asyncio.to_thread(\n"
        "            build_custom_component_template,\n"
        "            component,\n"
        "            user_id=user.id,\n"
        "            class_name=raw_code.get_class_name(),\n"
        "        )\n",
        applied="built_frontend_node, component_instance = await asyncio.to_thread(",
        expect={"1.12": 0, "1.13": (0, 1)},
        reason="1.13.1 custom_component_update signature variant",
    )
    text_patch(
        SITE_PACKAGES / "langflow" / "api" / "v1" / "endpoints.py",
        "endpoints.custom_component_update_async_thread_112",
        "        built_frontend_node, component_instance = build_custom_component_template(component, user_id=user.id)\n",
        "        built_frontend_node, component_instance = await asyncio.to_thread(build_custom_component_template, component, user_id=user.id)\n",
        applied="await asyncio.to_thread(build_custom_component_template, component, user_id=user.id)",
        expect={"1.12": (0, 1), "1.13": 0},
        reason="Langflow 1.13 passes class_name to build_custom_component_template",
    )

    # 5. Patch compiled React bundle in langflow/frontend/assets/*.js (100% React Virtual-DOM safe!)
    sf_svg_inline_sm = (
        'v.jsx("svg",{xmlns:"http://www.w3.org/2000/svg",width:20,height:20,viewBox:"0 0 64 64",fill:"#29B5E8",'
        'className:"h-5 w-5 shrink-0 text-[#29B5E8]","aria-hidden":"true",'
        'children:v.jsx("path",{d:"' + OFFICIAL_SNOWFLAKE_PATH + '"})})'
    )
    sf_svg_inline_color = (
        'v.jsx("svg",{xmlns:"http://www.w3.org/2000/svg",width:15,height:15,viewBox:"0 0 64 64",fill:"#29B5E8",'
        'className:"h-4 w-4 shrink-0 text-[#29B5E8]","aria-hidden":"true",'
        'children:v.jsx("path",{d:"' + OFFICIAL_SNOWFLAKE_PATH + '"})})'
    )
    sf_svg_inline_lg = (
        'v.jsx("svg",{xmlns:"http://www.w3.org/2000/svg",width:40,height:40,viewBox:"0 0 64 64",fill:"#29B5E8",'
        'className:"h-10 w-10 shrink-0 text-[#29B5E8]","aria-hidden":"true",'
        'children:v.jsx("path",{d:"' + OFFICIAL_SNOWFLAKE_PATH + '"})})'
    )

    sf_svg_inline_xs_white = (
        'v.jsx("svg",{xmlns:"http://www.w3.org/2000/svg",width:14,height:14,viewBox:"0 0 64 64",fill:"#ffffff",'
        'style:{width:14,height:14,fill:"#ffffff",color:"#ffffff",flexShrink:0},"aria-hidden":"true",'
        'children:v.jsx("path",{d:"' + OFFICIAL_SNOWFLAKE_PATH + '"})})'
    )

    s1_orig = (
        'v.jsxs("div",{className:"z-30 flex shrink-0 items-center gap-2","data-testid":"header_left_section_wrapper",'
        'children:[v.jsx(qt,{unstyled:!0,onClick:()=>n("/"),className:"mr-1 flex h-8 w-8 items-center",'
        '"data-testid":"icon-ChevronLeft","aria-label":e("header.home"),'
        'children:v.jsx(c6e,{className:"h-5 w-5","aria-hidden":"true"})}),v.jsx(rki,{})]})'
    )
    s1_new = (
        'v.jsxs("div",{className:"z-30 flex shrink-0 items-center gap-2","data-testid":"header_left_section_wrapper",'
        'children:[v.jsx(qt,{unstyled:!0,onClick:()=>n("/"),className:"mr-1 flex h-8 items-center gap-2 px-1 cursor-pointer",'
        '"data-testid":"icon-ChevronLeft","aria-label":e("header.home"),'
        'children:v.jsxs("span",{className:"inline-flex items-center gap-2 text-[#29B5E8]",style:{color:"#29B5E8"},children:['
        + sf_svg_inline_sm
        + ',v.jsx(c6e,{className:"h-5 w-5 shrink-0 text-[#29B5E8]",style:{color:"#29B5E8",fill:"#29B5E8"},"aria-hidden":"true"}),'
        'v.jsx("span",{className:"text-sm font-semibold tracking-tight text-[#29B5E8] whitespace-nowrap",style:{color:"#29B5E8"},children:"Snowflake Langflow"})]})}),v.jsx(rki,{})]})'
    )

    ttn_orig = (
        'function TTn({remSize:e}){const{t}=dt();return v.jsxs("div",{role:"status",className:"flex flex-col items-center justify-center",'
        'children:[v.jsx(c6e,{"aria-hidden":"true",title:t("common.langflowLogo"),className:"animate-pulse text-primary",style:{width:`${e*.25}rem`,height:`${e*.25}rem`}}),'
        'v.jsx("br",{}),v.jsx("span",{className:"animate-pulse text-lg text-primary",children:t("loading.loading")})]})}'
    )
    ttn_new = (
        'function TTn({remSize:e}){const{t}=dt(),_sz=`${Math.max(2.25,Math.min((e||16)*.18,3.25))}rem`;'
        'return v.jsxs("div",{role:"status","data-testid":"snowflake-langflow-dual-loading-logo",className:"flex flex-col items-center justify-center",style:{color:"#29B5E8"},'
        'children:[v.jsxs("div",{className:"animate-pulse flex items-center justify-center gap-3",style:{display:"inline-flex",alignItems:"center",justifyContent:"center",gap:"14px",color:"#29B5E8"},'
        'children:[v.jsx("svg",{xmlns:"http://www.w3.org/2000/svg",viewBox:"0 0 64 64",fill:"#29B5E8","aria-hidden":"true",style:{width:_sz,height:_sz,color:"#29B5E8",fill:"#29B5E8",flexShrink:0},children:v.jsx("path",{d:"' + OFFICIAL_SNOWFLAKE_PATH + '"})}),'
        'v.jsx(c6e,{"aria-hidden":"true",title:t("common.langflowLogo"),style:{width:_sz,height:_sz,color:"#29B5E8",fill:"#29B5E8",flexShrink:0}})]}),'
        'v.jsx("span",{className:"animate-pulse text-lg font-semibold",style:{color:"#29B5E8",marginTop:"12px"},children:t("loading.loading")})]})}'
    )

    og_orig = (
        'const Og=({size:e=24,...t})=>v.jsx("svg",{xmlns:"http://www.w3.org/2000/svg",width:e,height:e,viewBox:"0 0 24 24",fill:"none",stroke:"currentColor",strokeWidth:2,strokeLinecap:"round",strokeLinejoin:"round",className:"feather feather-circle","aria-hidden":t["aria-label"]?void 0:!0,focusable:t["aria-label"]?void 0:!1,...t,"data-testid":"loading-icon",children:v.jsxs("circle",{cx:12,cy:12,r:10,strokeDasharray:63,strokeDashoffset:21,children:[v.jsx("animateTransform",{attributeName:"transform",type:"rotate",from:"0 12 12",to:"360 12 12",dur:"2s",repeatCount:"indefinite"}),v.jsx("animate",{attributeName:"stroke-dashoffset",dur:"8s",repeatCount:"indefinite",keyTimes:"0; 0.5; 1",values:"-16; -47; -16",calcMode:"spline",keySplines:"0.4 0 0.2 1; 0.4 0 0.2 1"})]})});'
    )
    og_new = (
        'const Og=({size:e=24,...t})=>v.jsxs("span",{"data-testid":"loading-icon","aria-hidden":t["aria-label"]?void 0:!0,className:"animate-pulse inline-flex items-center justify-center gap-1.5",style:{display:"inline-flex",alignItems:"center",gap:"6px",color:"#29B5E8"},children:['
        'v.jsx("svg",{xmlns:"http://www.w3.org/2000/svg",width:e,height:e,viewBox:"0 0 64 64",fill:"#29B5E8",style:{width:e,height:e,color:"#29B5E8",fill:"#29B5E8",flexShrink:0},children:v.jsx("path",{d:"' + OFFICIAL_SNOWFLAKE_PATH + '"})}),'
        'v.jsx(c6e,{"aria-hidden":"true",style:{width:e,height:e,color:"#29B5E8",fill:"#29B5E8",flexShrink:0}})'
        ']});'
    )

    login_logo_orig = 'v.jsx(c6e,{title:h("common.langflowLogo"),className:"h-12 w-12 text-foreground"})'
    login_logo_new = (
        'v.jsxs("span",{className:"inline-flex items-center gap-3",style:{display:"inline-flex",alignItems:"center",gap:"12px",color:"#29B5E8"},children:['
        + sf_svg_inline_lg
        + ',v.jsx(c6e,{title:h("common.langflowLogo"),className:"h-10 w-10",style:{width:40,height:40,color:"#29B5E8",fill:"#29B5E8"}})]})'
    )

    too_fn_orig = (
        'function Too({filteredMemories:e,memoriesSearch:t,setMemoriesSearch:n,fetchNextPage:o,hasNextPage:s,'
        'isFetchingNextPage:a,selectedMemoryId:c,currentFlowId:u,onSelectMemory:h,onCreateMemory:f}){'
    )
    too_fn_prev = (
        'function Too({filteredMemories:_origE,memoriesSearch:t,setMemoriesSearch:n,fetchNextPage:o,hasNextPage:s,'
        'isFetchingNextPage:a,selectedMemoryId:c,currentFlowId:u,onSelectMemory:h,onCreateMemory:f}){'
        'const _sfMems=(typeof window!=="undefined"&&Array.isArray(window.SNOWFLAKE_MEMORIES_LIST)?window.SNOWFLAKE_MEMORIES_LIST:[])'
        '.filter(_m=>{const _q=(t||"").trim().toLowerCase();return!_q||`${_m.name||""} ${_m.description||""}`.toLowerCase().includes(_q);});'
        'const _existingNames=new Set((_origE||[]).map(_x=>(_x.name||"").toUpperCase()));'
        'const e=[..._sfMems.filter(_m=>!_existingNames.has((_m.name||"").toUpperCase())),...(_origE||[])];'
    )
    too_fn_prev2 = (
        'function Too({filteredMemories:_origE,memoriesSearch:t,setMemoriesSearch:n,fetchNextPage:o,hasNextPage:s,'
        'isFetchingNextPage:a,selectedMemoryId:c,currentFlowId:u,onSelectMemory:_origH,onCreateMemory:f}){'
        'const _sfMems=(typeof window!=="undefined"&&Array.isArray(window.SNOWFLAKE_MEMORIES_LIST)?window.SNOWFLAKE_MEMORIES_LIST:[])'
        '.filter(_m=>{const _q=(t||"").trim().toLowerCase();return!_q||`${_m.name||""} ${_m.description||""}`.toLowerCase().includes(_q);});'
        'const _existingNames=new Set((_origE||[]).map(_x=>(_x.name||"").toUpperCase()));'
        'const e=[..._sfMems.filter(_m=>!_existingNames.has((_m.name||"").toUpperCase())),...(_origE||[])];'
        'const h=(_sel)=>{if(typeof _origH==="function")_origH(_sel);if(typeof window!=="undefined"&&typeof window.renderSnowflakeMemoryDetailView==="function")window.renderSnowflakeMemoryDetailView(_sel);};'
    )
    too_fn_new = (
        'function Too({filteredMemories:_origE,memoriesSearch:t,setMemoriesSearch:n,fetchNextPage:o,hasNextPage:s,'
        'isFetchingNextPage:a,selectedMemoryId:c,currentFlowId:u,onSelectMemory:_origH,onCreateMemory:f}){'
        'const _sfMems=(typeof window!=="undefined"&&Array.isArray(window.SNOWFLAKE_MEMORIES_LIST)?window.SNOWFLAKE_MEMORIES_LIST:[])'
        '.filter(_m=>{const _q=(t||"").trim().toLowerCase();return!_q||`${_m.name||""} ${_m.description||""} ${_m.kb_name||""} ${_m.agent_memory_type||""} ${_m.backend_type||""}`.toLowerCase().includes(_q);});'
        'const _existingIds=new Set((_origE||[]).map(_x=>String(_x.id||"").toLowerCase()).filter(Boolean));'
        'const _existingNames=new Set((_origE||[]).map(_x=>(_x.name||"").toUpperCase()).filter(Boolean));'
        'const _mergedE=[...(_origE||[]),..._sfMems.filter(_m=>!_existingIds.has(String(_m.id||"").toLowerCase())&&!_existingNames.has((_m.name||"").toUpperCase()))];'
        'const e=typeof window!=="undefined"&&typeof window.filterSnowflakeMemoriesByContext==="function"?window.filterSnowflakeMemoriesByContext(_mergedE):_mergedE;'
        'const h=(_sel)=>{if(typeof _origH==="function")_origH(_sel);if(typeof window!=="undefined"&&typeof window.renderSnowflakeMemoryDetailView==="function")window.renderSnowflakeMemoryDetailView(_sel);};'
        'if(typeof window!=="undefined"){window.__sflfCurrentMemoryFlowId=u;window.__sflfSelectMemory=h;}'
    )

    ife_orig = (
        'IFe=e=>({...hht,id:e.id,name:e.name,kb_name:e.kb_name,embedding_model:e.embedding_model,user_id:e.user_id,'
        'flow_id:e.flow_id,created_at:e.created_at,is_active:e.auto_capture??hht.is_active,'
        'preprocessing_enabled:e.preprocessing??hht.preprocessing_enabled,preprocessing_model:e.preproc_model,'
        'preproc_instructions:e.preproc_instructions,batch_size:Math.max(1,Math.trunc(e.threshold??1)),'
        'backend_type:e.backend_type,backend_config:e.backend_config})'
    )
    ife_new = (
        'IFe=e=>({...hht,id:e.id,name:e.name,description:e.description??hht.description,kb_name:e.kb_name,'
        'embedding_model:e.embedding_model,embedding_provider:e.embedding_provider??hht.embedding_provider,'
        'user_id:e.user_id,flow_id:e.flow_id,created_at:e.created_at,'
        'last_generated_at:e.last_generated_at??e.created_at,'
        'total_messages_processed:e.total_messages_processed??hht.total_messages_processed,'
        'pending_messages_count:e.pending_messages_count??hht.pending_messages_count,'
        'sessions_count:e.sessions_count??hht.sessions_count,'
        'is_active:e.auto_capture??e.is_active??hht.is_active,'
        'preprocessing_enabled:e.preprocessing??e.preprocessing_enabled??hht.preprocessing_enabled,'
        'preprocessing_model:e.preproc_model??e.preprocessing_model,'
        'preproc_instructions:e.preproc_instructions,'
        'batch_size:Math.max(1,Math.trunc(e.threshold??e.batch_size??1)),'
        'backend_type:e.backend_type,backend_config:e.backend_config})'
    )

    nso_t_orig = (
        'T=N.useMemo(()=>{const He=y?.pages??[];if(He.length===0)return gzn;'
        'const et=He.flatMap(Te=>Te?.items??[]);return et.length?et:gzn},[y]);'
    )
    nso_t_new = (
        'T=N.useMemo(()=>{const He=y?.pages??[],et=He.flatMap(Te=>Te?.items??[]),'
        '_sf=typeof window!=="undefined"&&Array.isArray(window.SNOWFLAKE_MEMORIES_LIST)?window.SNOWFLAKE_MEMORIES_LIST:[],'
        '_ids=new Set(et.map(Te=>String(Te.id||"").toLowerCase()).filter(Boolean)),'
        '_nms=new Set(et.map(Te=>String(Te.name||"").toUpperCase()).filter(Boolean)),'
        '_m=[...et,..._sf.filter(Te=>!_ids.has(String(Te.id||"").toLowerCase())&&!_nms.has(String(Te.name||"").toUpperCase()))],'
        '_f=typeof window!=="undefined"&&typeof window.filterSnowflakeMemoriesByContext==="function"?window.filterSnowflakeMemoriesByContext(_m):_m;'
        'return _f.length?_f:gzn},[y]);'
    )

    boo_cards_orig = (
        'v.jsx(fht,{label:z("memory.processedMessages"),value:T?"—":e.total_messages_processed,icon:"MessageSquare"}),'
        'v.jsx(fht,{label:k,value:T?"—":D,icon:"Timer"}),'
        'v.jsx(fht,{label:z("memory.lastGenerated"),value:T?"—":Doo(e.last_generated_at),icon:"Clock"})'
    )
    boo_cards_new = (
        'v.jsx(fht,{label:z("memory.processedMessages"),value:e.total_messages_processed??(T?"—":0),icon:"MessageSquare"}),'
        'v.jsx(fht,{label:k,value:D??(T?"—":0),icon:"Timer"}),'
        'v.jsx(fht,{label:z("memory.lastGenerated"),value:e.last_generated_at?Doo(e.last_generated_at):T?"—":Doo(e.created_at),icon:"Clock"})'
    )

    too_orig = (
        'v.jsxs("div",{className:"border-b border-border px-4 py-3",children:[v.jsxs("div",{className:"flex items-center gap-2",children:[v.jsx(qe,{name:"BrainCog",className:"h-4 w-4 text-muted-foreground"}),v.jsx("h2",{className:"text-sm font-semibold",children:p("memory.sidebarTitle")})]}),'
        'v.jsxs(qt,{size:"sm",variant:"outline",onClick:f,disabled:!u,className:"mt-3 rounded-[10px]",children:[v.jsx(qe,{name:"Plus",className:"h-3.5 w-3.5"}),p("memory.createButton")]})]})'
    )
    too_new = (
        'v.jsxs("div",{className:"border-b border-border px-4 py-3",children:[v.jsxs("div",{className:"flex items-center gap-2",children:[v.jsx(qe,{name:"BrainCog",className:"h-4 w-4 text-muted-foreground"}),v.jsx("h2",{className:"text-sm font-semibold",children:p("memory.sidebarTitle")})]}),'
        'v.jsxs("div",{className:"mt-3 flex items-center gap-2 flex-wrap",children:['
        'v.jsxs(qt,{size:"sm",variant:"outline",onClick:f,disabled:!u,className:"rounded-[10px]",children:[v.jsx(qe,{name:"Plus",className:"h-3.5 w-3.5"}),p("memory.createButton")]}),'
        'v.jsxs(qt,{size:"sm",variant:"outline","data-testid":"memories-plus-snowflake-btn",onClick:(ev)=>window.openSnowflakeMemoryTypeDropdown?window.openSnowflakeMemoryTypeDropdown(u,ev):window.openCreateSnowflakeMemoryModal&&window.openCreateSnowflakeMemoryModal(u),style:{backgroundColor:"#29B5E8",borderColor:"#29B5E8",color:"#ffffff"},className:"rounded-[10px] font-semibold flex items-center gap-1.5",children:['
        'v.jsx(qe,{name:"Plus",className:"h-3.5 w-3.5"}),'
        + sf_svg_inline_xs_white
        + ',"Snowflake"]})'
        ']})]})'
    )
    too_prev = (
        'v.jsxs("div",{className:"border-b border-border px-4 py-3",children:[v.jsxs("div",{className:"flex items-center gap-2",children:[v.jsx(qe,{name:"BrainCog",className:"h-4 w-4 text-muted-foreground"}),v.jsx("h2",{className:"text-sm font-semibold",children:p("memory.sidebarTitle")})]}),'
        'v.jsxs("div",{className:"mt-3 flex items-center gap-2 flex-wrap",children:['
        'v.jsxs(qt,{size:"sm",variant:"outline",onClick:f,disabled:!u,className:"rounded-[10px]",children:[v.jsx(qe,{name:"Plus",className:"h-3.5 w-3.5"}),p("memory.createButton")]}),'
        'v.jsxs(qt,{size:"sm",variant:"outline","data-testid":"memories-plus-snowflake-btn",onClick:()=>window.openCreateSnowflakeMemoryModal&&window.openCreateSnowflakeMemoryModal(u),style:{backgroundColor:"#29B5E8",borderColor:"#29B5E8",color:"#ffffff"},className:"rounded-[10px] font-semibold flex items-center gap-1.5",children:['
        + sf_svg_inline_xs_white
        + ',v.jsx(qe,{name:"Plus",className:"h-3.5 w-3.5"}),"Snowflake"]})'
        ']})]})'
    )
    too_prev_plus_text = (
        'v.jsxs("div",{className:"border-b border-border px-4 py-3",children:[v.jsxs("div",{className:"flex items-center gap-2",children:[v.jsx(qe,{name:"BrainCog",className:"h-4 w-4 text-muted-foreground"}),v.jsx("h2",{className:"text-sm font-semibold",children:p("memory.sidebarTitle")})]}),'
        'v.jsxs("div",{className:"mt-3 flex items-center gap-2 flex-wrap",children:['
        'v.jsxs(qt,{size:"sm",variant:"outline",onClick:f,disabled:!u,className:"rounded-[10px]",children:[v.jsx(qe,{name:"Plus",className:"h-3.5 w-3.5"}),p("memory.createButton")]}),'
        'v.jsxs(qt,{size:"sm",variant:"outline","data-testid":"memories-plus-snowflake-btn",onClick:()=>window.openCreateSnowflakeMemoryModal&&window.openCreateSnowflakeMemoryModal(u),style:{backgroundColor:"#29B5E8",borderColor:"#29B5E8",color:"#ffffff"},className:"rounded-[10px] font-semibold flex items-center gap-1.5",children:['
        + sf_svg_inline_xs_white
        + ',"\x2b Snowflake"]})'
        ']})]})'
    )
    too_btn_canonical = (
        'v.jsxs(qt,{size:"sm",variant:"outline","data-testid":"memories-plus-snowflake-btn",'
        'onClick:(ev)=>window.openSnowflakeMemoryTypeDropdown?window.openSnowflakeMemoryTypeDropdown(u,ev):window.openCreateSnowflakeMemoryModal&&window.openCreateSnowflakeMemoryModal(u),'
        'style:{backgroundColor:"#29B5E8",borderColor:"#29B5E8",color:"#ffffff"},'
        'className:"rounded-[10px] font-semibold flex items-center gap-1.5",children:['
        'v.jsx(qe,{name:"Plus",className:"h-3.5 w-3.5"}),'
        + sf_svg_inline_xs_white
        + ',"Snowflake"]})]})]})'
    )

    s2_orig = (
        'v.jsx(cs,{content:e("header.goToDiscord"),side:"bottom",styleClasses:"z-10",ariaDescribedBy:void 0,'
        'children:v.jsxs(qt,{unstyled:!0,onClick:()=>window.open(N6e,"_blank"),'
        'className:"hit-area-hover flex items-center gap-2 rounded-md p-1 text-muted-foreground",'
        'children:[v.jsx("span",{className:"sr-only",children:e("header.goToDiscord")}),'
        'v.jsxs("div",{className:"relative items-center rounded-md px-2 py-1 flex",'
        'children:[v.jsx(Ise,{"aria-hidden":"true",className:"h-4 w-4"}),'
        'v.jsx(hk,{condition:!!s&&s!=="0",children:v.jsx("span",{className:"text-xs font-semibold pl-2","aria-hidden":"true",children:s})})]})]})})'
    )
    s2_new = (
        'v.jsx(cs,{content:"Snowflake Docs",side:"bottom",styleClasses:"z-10",ariaDescribedBy:void 0,'
        'children:v.jsxs(qt,{unstyled:!0,onClick:()=>window.open("https://docs.snowflake.com/","_blank"),'
        'className:"hit-area-hover flex items-center gap-1.5 rounded-md p-1 text-muted-foreground",'
        'children:[v.jsx("span",{className:"sr-only",children:"Snowflake Docs"}),'
        'v.jsxs("div",{className:"relative items-center rounded-md px-2 py-1 flex gap-1.5",'
        'children:[' + sf_svg_inline_color + ',v.jsx("span",{className:"text-xs font-semibold text-[#29B5E8]",children:"Docs"})]})]})})'
    )

    s3_orig = (
        'v.jsx(QTi,{position:"right",classNameSize:"w-[272px]",children:v.jsxs("div",{className:"divide-y divide-foreground/10",'
        'children:[v.jsx(HRi,{}),v.jsx("div",{children:v.jsx("div",{className:"h-[44px] items-center px-4 pt-3",'
        'children:v.jsxs("div",{className:"flex items-center justify-between",'
        'children:[v.jsx("span",{"data-testid":"menu_version_button",id:"menu_version_button",className:"text-sm",children:e("account.version")}),'
        'v.jsxs("div",{className:Ve("float-right text-xs",h&&"text-accent-emerald-foreground",!h&&"text-accent-amber-foreground"),'
        'children:[t," ",e(h?"account.latest":"account.updateAvailable")]})]})})}),'
        'v.jsxs("div",{children:[v.jsx(DAn,{onClick:()=>{o("/settings")},children:v.jsx("span",{"data-testid":"menu_settings_button",id:"menu_settings_button",children:e("account.settings")})}),'
        'v.jsx($Ri,{onNavigate:f=>o(f)}),'
        'v.jsx(Tke,{newPage:!0,href:xNn,children:v.jsx("span",{"data-testid":"menu_docs_button",id:"menu_docs_button",children:e("account.docs")})})]}),'
        'v.jsxs("div",{children:[v.jsx(Tke,{newPage:!0,href:D6e,children:v.jsxs("span",{"data-testid":"menu_github_button",id:"menu_github_button",className:"flex items-center gap-2",children:[v.jsx(Lse,{className:"h-4 w-4","aria-hidden":"true"}),e("account.github")]})}),'
        'v.jsx(Tke,{newPage:!0,href:N6e,children:v.jsxs("span",{"data-testid":"menu_discord_button",id:"menu_discord_button",className:"flex items-center gap-2",children:[v.jsx(Ise,{className:"h-4 w-4 text-[#5865F2]","aria-hidden":"true"}),e("account.discord")]})}),'
        'v.jsx(Tke,{newPage:!0,href:D7r,children:v.jsxs("span",{"data-testid":"menu_twitter_button",id:"menu_twitter_button",className:"flex items-center gap-2",children:[v.jsx(qe,{strokeWidth:2,name:"TwitterX",className:"h-4 w-4"}),e("account.twitter")]})})]})'
    )
    s3_new = (
        'v.jsx(QTi,{position:"right",classNameSize:"w-[208px] text-xs",children:v.jsxs("div",{className:"divide-y divide-foreground/10",'
        'children:[v.jsx(HRi,{}),v.jsx("div",{children:v.jsx("div",{className:"h-[32px] items-center px-3 pt-1.5",'
        'children:v.jsxs("div",{className:"flex items-center justify-between",'
        'children:[v.jsx("span",{"data-testid":"menu_version_button",id:"menu_version_button",className:"text-xs",children:e("account.version")}),'
        'v.jsxs("div",{className:Ve("float-right text-[11px]",h&&"text-accent-emerald-foreground",!h&&"text-accent-amber-foreground"),'
        'children:[t," ",e(h?"account.latest":"account.updateAvailable")]})]})})}),'
        'v.jsxs("div",{children:[v.jsx(DAn,{onClick:()=>{o("/settings")},children:v.jsx("span",{"data-testid":"menu_settings_button",id:"menu_settings_button",className:"text-xs",children:e("account.settings")})}),'
        'v.jsx($Ri,{onNavigate:f=>o(f)}),'
        'v.jsx(Tke,{newPage:!0,href:xNn,children:v.jsxs("span",{"data-testid":"menu_docs_button",id:"menu_docs_button",className:"flex items-center gap-2 text-xs",children:[' + sf_svg_inline_color + ',e("account.docs")]})})]}),'
        'v.jsxs("div",{children:[v.jsx(Tke,{newPage:!0,href:D6e,children:v.jsxs("span",{"data-testid":"menu_github_button",id:"menu_github_button",className:"flex items-center gap-2 text-xs",children:[v.jsx(Lse,{className:"h-3.5 w-3.5","aria-hidden":"true"}),e("account.github")]})})]})'
    )

    s4_orig = (
        'v.jsx("div",{className:"z-50 dark:hidden",children:v.jsx("img",{src:Uvo,alt:t("common.langflowLogoLight"),"data-testid":"empty_page_logo_light",className:"relative top-8 h-40 pointer-events-none select-none"})}),'
        'v.jsx("div",{className:"z-50 hidden dark:block",children:v.jsx("img",{src:Vvo,alt:t("common.langflowLogoDark"),"data-testid":"empty_page_logo_dark",className:"relative top-8 h-40 pointer-events-none select-none"})})'
    )
    s4_new = (
        'v.jsxs("div",{className:"z-50 mb-3 flex items-center justify-center gap-3 text-[#29B5E8] pointer-events-none select-none","data-testid":"empty_page_logo_light",'
        'children:[' + sf_svg_inline_lg + ',v.jsx(c6e,{className:"h-9 w-9 shrink-0 text-[#29B5E8]","aria-hidden":"true"})]})'
    )
    s4_loading_orig = 'v.jsx(c6e,{className:"h-10 w-10"})'
    s4_loading_new = (
        'v.jsx("span",{className:"inline-flex items-center justify-center text-[#29B5E8]","data-testid":"snowflake-langflow-dual-loading-logo",'
        'children:' + sf_svg_inline_lg + '})'
    )

    s5_orig = (
        ',v.jsx(qt,{unstyled:!0,className:"group mx-3 min-h-[84px] sm:mx-0",onClick:()=>{p("discord_clicked")(),window.open(N6e,"_blank","noopener,noreferrer")},"data-testid":"empty_page_discord_button",'
        'children:v.jsxs("div",{className:"relative flex flex-col rounded-lg border-[1px] bg-background p-4 transition-all duration-300 hover:border-discord-color",'
        'children:[v.jsxs("div",{className:"grid w-full items-center justify-between gap-2",'
        'children:[v.jsxs("div",{className:"flex gap-3",children:[v.jsx(Ise,{className:"h-6 w-6 text-discord-color","aria-hidden":"true"}),'
        'v.jsxs("div",{children:[v.jsx("span",{className:"font-semibold",children:"Discord"}),v.jsx("span",{className:"ml-2 font-mono text-muted-foreground",children:K5e(c)})]})]}),'
        'v.jsx("div",{children:v.jsx("span",{className:"line-clamp-2 text-left text-base text-secondary-foreground",children:t("page.discordDescription")})})]}),v.jsx(V5e,{className:hRn})]})})'
    )

    dom_guard_banner = (
        'if(typeof Node==="function"&&Node.prototype&&!Node.prototype.__sflfPatched){'
        'var _oRC=Node.prototype.removeChild;'
        'Node.prototype.removeChild=function(c){if(c&&c.parentNode!==this){return c.parentNode?_oRC.call(c.parentNode,c):c;}return _oRC.apply(this,arguments);};'
        'var _oIB=Node.prototype.insertBefore;'
        'Node.prototype.insertBefore=function(n,r){if(r&&r.parentNode!==this){return this.appendChild(n);}return _oIB.apply(this,arguments);};'
        'Node.prototype.__sflfPatched=true;}'
        'if(typeof window!=="undefined"&&typeof window.fetch==="function"&&!window.fetch.__sflfHeadersGuarded){'
        'var _pF=window.fetch;'
        'var _gF=function(u,o){if(typeof u==="string"||(typeof URL==="function"&&u instanceof URL)){'
        'var s=o&&typeof o==="object"?Object.assign({},o):{};if(!s.headers){s.headers={};}return _pF.call(this,u,s);}'
        'return _pF.apply(this,arguments);};'
        '_gF.__sflfHeadersGuarded=true;window.fetch=_gF;}\n'
    )

    fetch_intercept_orig = (
        'const y=hKr.register({request:(z,T)=>{if(!x(z)){for(const[k,D]of Object.entries(h))T.headers[k]=D;'
        'T.headers["x-langflow-client"]="playground"}return[z,T]}})'
    )
    fetch_intercept_new = (
        'const y=hKr.register({request:(z,T)=>{try{if(!x(z)){'
        'if(z&&typeof z=="object"&&z.headers&&typeof z.headers.set=="function"&&!T){'
        'for(const[k,D]of Object.entries(h||{}))z.headers.set(k,D);'
        'z.headers.set("x-langflow-client","playground");return[z]}'
        'T=T&&typeof T=="object"?{...T}:{};'
        'if(T.headers&&typeof T.headers.set=="function"){'
        'for(const[k,D]of Object.entries(h||{}))T.headers.set(k,D);'
        'T.headers.set("x-langflow-client","playground")'
        '}else{T.headers={...(T.headers||{})};'
        'for(const[k,D]of Object.entries(h||{}))T.headers[k]=D;'
        'T.headers["x-langflow-client"]="playground"}}'
        '}catch(_e){}return T===void 0?[z]:[z,T]}})'
    )

    axios_req_intercept_orig = (
        'if(new URL(z?.url,k).origin===k){for(const[j,q]of Object.entries(h))z.headers[j]=q;'
        'z.headers["x-langflow-client"]="playground"}'
    )
    axios_req_intercept_new = (
        'if(new URL(z?.url,k).origin===k){z.headers=z.headers||{};for(const[j,q]of Object.entries(h||{}))z.headers[j]=q;'
        'z.headers["x-langflow-client"]="playground"}'
    )

    agui_hzr_orig = 'const hZr=(e,t)=>JLn(()=>zLe(fetch(e,t))).pipe($Qr(n=>{if(!n.ok){let a=n.headers.get("content-type")||"";'
    agui_hzr_new = 'const hZr=(e,t)=>JLn(()=>zLe(fetch(e,t))).pipe($Qr(n=>{if(!n?.ok){let a=n?.headers?.get?.("content-type")||"";'

    agui_gzr_orig = 'if(s.next(c),c.type===ZY.HEADERS&&!a){a=!0;let u=c.headers.get("content-type");'
    agui_gzr_new = 'if(s.next(c),c?.type===ZY.HEADERS&&!a){a=!0;let u=c?.headers?.get?.("content-type")||"";'

    # S4: the empty-page logo is written in its final form (40px Langflow mark) instead of a chained h-9 -> h-10 rewrite.
    s4_new = s4_new.replace(
        'v.jsx(c6e,{className:"h-9 w-9 shrink-0 text-[#29B5E8]","aria-hidden":"true"})',
        'v.jsx(c6e,{className:"h-10 w-10 shrink-0 text-[#29B5E8]",style:{width:40,height:40,color:"#29B5E8",fill:"#29B5E8"},"aria-hidden":"true"})',
    )
    bucket_svg_inline = (
                'v.jsxs("svg",{xmlns:"http://www.w3.org/2000/svg",width:20,height:20,viewBox:"0 0 24 24",fill:"none",'
                'stroke:"currentColor",strokeWidth:2,strokeLinecap:"round",strokeLinejoin:"round",className:"h-5 w-5",'
                'children:[v.jsx("ellipse",{cx:12,cy:7,rx:8,ry:3}),'
                'v.jsx("path",{d:"M4 7l1.8 11.2A2 2 0 0 0 7.78 20h8.44a2 2 0 0 0 1.98-1.8L20 7"}),'
                'v.jsx("path",{d:"M7 7a5 5 0 0 1 10 0"})]})'
            )
    sf_cq_entries = (
                '{id:"snowflake_table",label:"Snowflake Standard Table (VECTOR)",description:"Governed Snowflake Standard Table with VECTOR(FLOAT, 768|1024) and SNOWFLAKE.CORTEX.AI_EMBED in active [ ❄️ Context ].",icon:"Database",status:"available",defaultEnabled:!0,configFields:[]},'
                '{id:"snowflake_hybrid",label:"Snowflake Hybrid Table (VECTOR)",description:"Low-latency Snowflake Hybrid Table with Primary Key + VECTOR(FLOAT, 768|1024) in active [ ❄️ Context ].",icon:"Database",status:"available",defaultEnabled:!0,configFields:[]},'
                '{id:"snowflake_dynamic",label:"Snowflake Dynamic Table (VECTOR)",description:"Incremental Snowflake Dynamic Table with TARGET_LAG and VECTOR(FLOAT, 768|1024) in active [ ❄️ Context ].",icon:"Database",status:"available",defaultEnabled:!0,configFields:[]},'
                '{id:"snowflake_interactive",label:"Snowflake Interactive Table (VECTOR)",description:"Sub-second Snowflake Interactive Table with VECTOR(FLOAT, 768|1024) in active [ ❄️ Context ].",icon:"Database",status:"available",defaultEnabled:!0,configFields:[]},'
                '{id:"snowflake_iceberg",label:"Snowflake Iceberg Table (VECTOR)",description:"Open Apache Iceberg Table managed by Snowflake with VECTOR(FLOAT, 768|1024) in active [ ❄️ Context ].",icon:"Database",status:"available",defaultEnabled:!0,configFields:[]},'
                '{id:"snowflake_postgres",label:"Snowflake Postgres (:5432 pgvector)",description:"Managed Snowflake Postgres (:5432) with pgvector HNSW index in active [ ❄️ Context ].",icon:"Postgres",status:"available",defaultEnabled:!0,configFields:[]},'
                )
    s_i_orig = (
                'return u.filter(E=>x(E.provider)).map(E=>{const z=E?.models?.filter(T=>e==="all"?!0:T?.metadata?.model_type===e)||[];'
                'return{provider:E.provider,icon:E.icon,is_enabled:E.is_enabled,is_configured:E.is_configured,model_count:z.length,models:z,api_docs_url:E.api_docs_url,live_discovery:E.live_discovery}})'
            )
    s_i_new = (
                'return u.filter(E=>x(E.provider)).map(E=>{const z=E?.models?.filter(T=>e==="all"?!0:T?.metadata?.model_type===e)||[];'
                'return{provider:E.provider,icon:E.icon,is_enabled:E.is_enabled,is_configured:E.is_configured,model_count:z.length,models:z,api_docs_url:E.api_docs_url,live_discovery:E.live_discovery}})'
                '.filter(E=>!(e&&e!=="all"&&typeof E.provider==="string"&&E.provider.startsWith("Snowflake")&&E.model_count===0))'
            )
    gqn_orig = (
                'function gQn(e){if(!e||typeof e.name!="string"||!uoo(e.name))return e;let t;try{t=JSON.parse(e.name)}catch{return e}'
                'const n=Array.isArray(t)?t[0]:t;if(!n||typeof n!="object"||typeof n.name!="string")return e;const o=n;'
                'return{...e,name:o.name,provider:o.provider??e.provider,icon:o.icon??e.icon,metadata:o.metadata??e.metadata}}'
            )
    gqn_new = (
                'const _sfMMap={"glm-5.3":"Snowflake Proxy - Chat (/chat/v1)","grok-4.6":"Snowflake Proxy - Chat (/chat/v1)","grok-4.7":"Snowflake Proxy - Chat (/chat/v1)","kimi-k3":"Snowflake Proxy - Chat (/chat/v1)","openai-gpt-6-luna":"Snowflake Proxy - Chat (/chat/v1)","openai-gpt-6-sol":"Snowflake Proxy - Chat (/chat/v1)","openai-gpt-6-astra":"Snowflake Proxy - Responses (/responses/v1)","openai-gpt-6.1-sol":"Snowflake Proxy - Responses (/responses/v1)","claude-haiku-5-5":"Snowflake Proxy - Messages (/messages)","claude-opus-5-5":"Snowflake Proxy - Messages (/messages)","claude-sonnet-5-5":"Snowflake Proxy - Messages (/messages)","llama3.1:8b":"Snowflake Private - Ollama"};'
                'function _normSfModel(r){if(!r||typeof r.name!="string")return r;const p=ej(r.provider||""),sp=_sfMMap[r.name];if(sp&&(!p||!p.startsWith("Snowflake "))){return{...r,provider:sp,icon:"Snowflake"}}if(p&&p!==r.provider){return{...r,provider:p,icon:p.startsWith("Snowflake ")?"Snowflake":r.icon}}return r}'
                'function gQn(e){if(typeof e=="string"&&e.trim())e={name:e.trim()};if(!e||typeof e.name!="string")return e;if(!uoo(e.name))return _normSfModel(e);let t;try{t=JSON.parse(e.name)}catch{return _normSfModel(e)}'
                'const n=Array.isArray(t)?t[0]:t;if(!n||typeof n!="object"||typeof n.name!="string")return _normSfModel(e);const o=n;'
                'return _normSfModel({...e,name:o.name,provider:o.provider??e.provider,icon:o.icon??e.icon,metadata:o.metadata??e.metadata})}'
            )
    # Langflow 1.13 variants. Written with the 1.12 minified names on purpose: the mini matcher binds
    # names by position, and the shared Snowflake snippets (sf_svg_inline_*) use the same names.
    s1_orig_113 = (
        'v.jsxs("div",{className:"z-30 flex shrink-0 items-center gap-2","data-testid":"header_left_section_wrapper",'
        'children:[v.jsx(qt,{unstyled:!0,onClick:()=>n("/"),className:"mr-1 flex h-8 w-8 items-center justify-center",'
        '"data-testid":"icon-ChevronLeft","aria-label":e("header.home"),'
        'children:v.jsx(c6e,{className:"h-5 w-5"})}),v.jsx(rki,{})]})'
    )
    agui_runhttp_orig = 'const hZr=e=>JLn(()=>zLe(e())).pipe($Qr(n=>{if(!n.ok){let a=n.headers.get("content-type")||"";'
    agui_runhttp_new = 'const hZr=e=>JLn(()=>zLe(e())).pipe($Qr(n=>{if(!n?.ok){let a=n?.headers?.get?.("content-type")||"";'
    _ife_storage = "storage_state:e.storage_state,storage_kb_id:e.storage_kb_id,active_migration_id:e.active_migration_id,"
    ife_113_orig = ife_orig.replace("backend_type:e.backend_type,", _ife_storage + "backend_type:e.backend_type,")
    ife_113_new = ife_new.replace("backend_type:e.backend_type,", _ife_storage + "backend_type:e.backend_type,")
    s3_113_orig = (
        'v.jsx(QTi,{position:"right",classNameSize:"w-[272px]",children:v.jsxs("div",{className:"divide-y divide-foreground/10",'
        'children:[v.jsx(HRi,{}),v.jsx("div",{children:v.jsx("div",{className:"h-[44px] items-center px-4 pt-3",'
        'children:v.jsxs("div",{className:"flex items-center justify-between",'
        'children:[v.jsx("span",{"data-testid":"menu_version_button",id:"menu_version_button",className:"text-sm",children:e("account.version")}),'
        'v.jsxs("div",{className:Ve("float-right text-xs",h&&"text-accent-emerald-foreground",!h&&"text-accent-amber-foreground"),'
        'children:[t," ",e(h?"account.latest":"account.updateAvailable")]})]})})}),'
        'v.jsxs("div",{children:[v.jsx($Ri,{onNavigate:f=>o(f)}),'
        'v.jsx(Tke,{newPage:!0,href:xNn,children:v.jsx("span",{"data-testid":"menu_docs_button",id:"menu_docs_button",children:e("account.docs")})})]})'
    )
    s3_113_new = (
        'v.jsx(QTi,{position:"right",classNameSize:"w-[208px] text-xs",children:v.jsxs("div",{className:"divide-y divide-foreground/10",'
        'children:[v.jsx(HRi,{}),v.jsx("div",{children:v.jsx("div",{className:"h-[32px] items-center px-3 pt-1.5",'
        'children:v.jsxs("div",{className:"flex items-center justify-between",'
        'children:[v.jsx("span",{"data-testid":"menu_version_button",id:"menu_version_button",className:"text-xs",children:e("account.version")}),'
        'v.jsxs("div",{className:Ve("float-right text-[11px]",h&&"text-accent-emerald-foreground",!h&&"text-accent-amber-foreground"),'
        'children:[t," ",e(h?"account.latest":"account.updateAvailable")]})]})})}),'
        'v.jsxs("div",{children:[v.jsx($Ri,{onNavigate:f=>o(f)}),'
        'v.jsx(Tke,{newPage:!0,href:xNn,children:v.jsxs("span",{"data-testid":"menu_docs_button",id:"menu_docs_button",className:"flex items-center gap-2 text-xs",children:['
        + sf_svg_inline_color
        + ',e("account.docs")]})})]})'
    )
    loo_aliases_new = (
        '{"IBM watsonx.ai":"IBM WatsonX","Proxy - Chat":"Snowflake Proxy - Chat (/chat/v1)","Snowflake Proxy - Chat":"Snowflake Proxy - Chat (/chat/v1)",'
        '"Proxy - Responses":"Snowflake Proxy - Responses (/responses/v1)","Snowflake Proxy - Responses":"Snowflake Proxy - Responses (/responses/v1)",'
        '"Proxy - Messages":"Snowflake Proxy - Messages (/messages)","Snowflake Proxy - Messages":"Snowflake Proxy - Messages (/messages)",'
        '"Private - Ollama":"Snowflake Private - Ollama"}'
    )
    bundle_patches = [
        BundlePatch("playground.fetch_intercept", fetch_intercept_orig, fetch_intercept_new),
        BundlePatch("playground.axios_intercept", axios_req_intercept_orig, axios_req_intercept_new),
        BundlePatch("agui.fetch_response_null_safe", agui_hzr_orig, agui_hzr_new, expect={"1.12": (0, 1), "1.13": 0}, reason="1.12.5+ AG-UI client no longer wraps fetch(e,t) here"),
        BundlePatch("agui.fetch_response_null_safe_v2", agui_runhttp_orig, agui_runhttp_new, expect={"1.12": (0, 1), "1.13": 1}, reason="pre-1.12.5 AG-UI client"),
        BundlePatch("agui.headers_null_safe", agui_gzr_orig, agui_gzr_new),
        BundlePatch("react_dom.remove_child_guard", 'if(Lp)try{(Xd.nodeType===9?Xd.body:Xd.nodeName==="HTML"?Xd.ownerDocument.body:Xd).removeChild(Q.stateNode)}catch(Oe){gd(Q,W,Oe)}else try{Xd.removeChild(Q.stateNode)}catch(Oe){gd(Q,W,Oe)}', 'if(Lp)try{(Xd.nodeType===9?Xd.body:Xd.nodeName==="HTML"?Xd.ownerDocument.body:Xd).removeChild(Q.stateNode)}catch(Oe){}else try{Xd.removeChild(Q.stateNode)}catch(Oe){}'),
        BundlePatch("react_dom.insert_notfound_guard", 'case 3:case 4:var On=Q.stateNode.containerInfo,Sr=Tq(I);kq(I,Sr,On);break;default:throw Error(o(161))}}catch(zi){gd(I,I.return,zi)}', 'case 3:case 4:var On=Q.stateNode.containerInfo,Sr=Tq(I);kq(I,Sr,On);break;default:throw Error(o(161))}}catch(zi){zi&&zi.name==="NotFoundError"||gd(I,I.return,zi)}'),
        BundlePatch("header.brand_left", [s1_orig, s1_orig_113], s1_new),
        BundlePatch("brand.loading_logo", ttn_orig, ttn_new),
        BundlePatch("brand.loading_icon", og_orig, og_new, symbols={"c6e": "LOGO"}),
        # the login logo is rendered by two lazily-loaded chunks (login and admin login)
        BundlePatch("brand.login_logo", login_logo_orig, login_logo_new, expect=2),
        BundlePatch("memories.sidebar_merge", [too_fn_orig, too_fn_prev, too_fn_prev2], too_fn_new, fresh=("_m", "_q", "_x", "_sel")),
        BundlePatch("memories.snowflake_button", [too_orig, too_prev, too_prev_plus_text], too_new, fresh=("ev",)),
        BundlePatch("memories.adapter_fields", ife_orig, ife_new, expect={"1.12": 1, "1.13": 0}, reason="1.13 adapter adds storage_* fields"),
        BundlePatch("memories.adapter_fields_113", ife_113_orig, ife_113_new, expect={"1.12": 0, "1.13": 1}, reason="1.12 adapter has no storage_* fields"),
        BundlePatch("memories.list_merge", nso_t_orig, nso_t_new),
        BundlePatch("memories.detail_cards", boo_cards_orig, boo_cards_new),
        BundlePatch(
            "memories.refetch_hook",
            # the memories hook return object is the only one that also exposes updateMemoryMutation
            r"(updateMemoryMutation:[\w$]+,handleToggleActive:[\w$]+,onRefresh:)([\w$]+)(?=,fetchNextSessionsPage:)",
            r'\1(typeof window!=="undefined"&&(window.__sflfRefetchMemories=\2),\2)',
            mode="regex",
            applied=r"onRefresh:\(typeof window!==\"undefined\"&&\(window\.__sflfRefetchMemories=",
            hints=["updateMemoryMutation:", "__sflfRefetchMemories"],
        ),
        BundlePatch("header.docs_button", s2_orig, s2_new),
        BundlePatch("account_menu.compact", s3_orig, s3_new, expect={"1.12": 1, "1.13": 0}, reason="1.13 account menu no longer has settings/github/discord/twitter items"),
        BundlePatch("account_menu.compact_113", s3_113_orig, s3_113_new, expect={"1.12": 0, "1.13": 1}, reason="1.12 account menu layout"),
        BundlePatch("brand.empty_page_logo", s4_orig, s4_new, symbols={"c6e": "LOGO"}),
        BundlePatch("brand.loading_logo_10", s4_loading_orig, s4_loading_new, expect=0, reason="anchor not present in Langflow 1.12.2+ bundles"),
        BundlePatch(
            "header.remove_discord_card",
            s5_orig,
            "",
            applied=r"(?!)",
            expect={"1.12": (0, 1), "1.13": (0, 1)},
            reason="removal patch: 0 means the card is already gone",
        ),
        BundlePatch("brand.loading_icon_color", '"data-testid":"loading_icon",className:"', '"data-testid":"loading_icon",style:{color:"#29B5E8",fill:"#29B5E8",stroke:"#29B5E8"},className:"text-[#29B5E8] ', expect=0, reason="anchor not present in Langflow 1.12.2+ bundles"),
        BundlePatch("brand.loading_component_color", 'className:"loading-component', 'style:{color:"#29B5E8"},className:"loading-component text-[#29B5E8]', expect=0, reason="anchor not present in Langflow 1.12.2+ bundles"),
        BundlePatch("brand.new_project_btn", '"data-testid":"new-project-btn"', '"data-testid":"new-project-btn",style:{backgroundColor:"#29B5E8",borderColor:"#29B5E8",color:"#ffffff"}'),
        BundlePatch("brand.first_flow_btn", '"data-testid":"empty_page_create_first_flow_button"', '"data-testid":"empty_page_create_first_flow_button",style:{backgroundColor:"#29B5E8",borderColor:"#29B5E8",color:"#ffffff"}', expect=0, reason="anchor not present in Langflow 1.12.2+ bundles"),
        BundlePatch("account_menu.link_item_compact", 'Tke=({href:e="#",children:t,newPage:n=!1,icon:o="external-link"})=>v.jsx(au,{className:"cursor-pointer rounded-none p-3 px-4"', 'Tke=({href:e="#",children:t,newPage:n=!1,icon:o="external-link"})=>v.jsx(au,{className:"cursor-pointer rounded-none py-1.5 px-3 text-xs"'),
        BundlePatch("account_menu.action_item_compact", 'DAn=({icon:e="",onClick:t,children:n})=>v.jsxs(au,{className:"group flex cursor-pointer items-center justify-between p-3 px-4"', 'DAn=({icon:e="",onClick:t,children:n})=>v.jsxs(au,{className:"group flex cursor-pointer items-center justify-between py-1.5 px-3 text-xs"'),
        BundlePatch("account_menu.theme_row_compact", 'v.jsxs("div",{className:"flex items-center justify-between px-4 py-[6.5px] text-sm",children:[v.jsx("span",{className:"",children:e("account.theme")})', 'v.jsxs("div",{className:"flex items-center justify-between px-3 py-1.5 text-xs",children:[v.jsx("span",{className:"",children:e("account.theme")})'),
        BundlePatch("brand.github_repo_url", "https://github.com/langflow-ai/langflow", "https://github.com/fbattaglieri-snowflake/Native-Snowflake-Langflow", mode="literal", expect=(1, None)),
        BundlePatch("brand.docs_url", 'xNn="https://docs.langflow.org"', 'xNn="https://docs.snowflake.com/"'),
        BundlePatch("i18n.account_docs", '"account.docs":"Docs"', '"account.docs":"Snowflake Docs"'),
        BundlePatch("i18n.github_description", '"page.githubDescription":"Follow development, star the repo, and shape the future."', '"page.githubDescription":"Unofficial personal fork by Francesco Battaglieri (fbattaglieri-snowflake/Native-Snowflake-Langflow) — Under Active Testing."'),
        BundlePatch("i18n.welcome_title", '"page.welcomeTitle":"Welcome to Langflow"', '"page.welcomeTitle":"Welcome to Snowflake Langflow"'),
        BundlePatch("i18n.flow_builder_configure_models", '"flowBuilderWelcome.configureModelProviders":"Configure Model Providers"', '"flowBuilderWelcome.configureModelProviders":"Configure Other Model Provider"'),
        BundlePatch("i18n.assistant_configure_models", '"assistant.configureModels":"Configure Model Providers"', '"assistant.configureModels":"Configure Other Model Provider"'),
        BundlePatch("i18n.model_input_manage_providers", '"modelInput.manageProviders":"Manage Model Providers"', '"modelInput.manageProviders":"Configure Other Model Provider"'),
        BundlePatch("i18n.settings_nav_model_providers", '"settings.nav.modelProviders":"Model Providers"', '"settings.nav.modelProviders":"Additional Model Providers"'),
        BundlePatch("i18n.model_providers_title", '"modelProviders.pageTitle":"Model Providers"', '"modelProviders.pageTitle":"Additional Model Providers"'),
        BundlePatch("i18n.model_providers_description", '"modelProviders.pageDescription":"Configure AI model providers and manage their API keys."', '"modelProviders.pageDescription":"Snowflake Proxy (Chat /chat/v1, Responses /responses/v1, Messages /messages) and Snowflake-hosted Ollama are pre-configured in Models & Agents. Configure any additional external model providers below."'),
        BundlePatch("i18n.settings_nav_db_providers", '"settings.nav.dbProviders":"DB Providers"', '"settings.nav.dbProviders":"Additional DB Providers"'),
        BundlePatch("i18n.db_providers_title", '"settings.dbProviders.title":"DB Providers"', '"settings.dbProviders.title":"Additional DB Providers"'),
        BundlePatch("i18n.db_providers_description", '"settings.dbProviders.description":"Configure vector-store providers for Knowledge Bases."', '"settings.dbProviders.description":"Snowflake Vector Stores (Standard Table VECTOR, Hybrid Table VECTOR, Cortex Search, Snowflake Postgres pgvector) are pre-configured in Files & Knowledge. Configure any additional DB providers below."'),
        BundlePatch("triggers.sidebar_category", '{display_name:"sidebar.category.inputOutput",name:"input_output",icon:"Cable"},{display_name:"sidebar.category.dataSources",name:"data_source",icon:"Database"}', '{display_name:"sidebar.category.inputOutput",name:"input_output",icon:"Cable"},{display_name:"sidebar.category.triggers",name:"triggers",icon:"Zap"},{display_name:"sidebar.category.dataSources",name:"data_source",icon:"Database"}'),
        BundlePatch("triggers.sidebar_category_i18n", '"sidebar.category.inputOutput":"Input & Output",', '"sidebar.category.inputOutput":"Input & Output","sidebar.category.triggers":"Triggers",'),
        BundlePatch("triggers.category_icon_map", 'kei={saved_components:"Save",input_output:"Cable",', 'kei={saved_components:"Save",input_output:"Cable",triggers:"Zap",'),
        BundlePatch("triggers.category_icon_map_2", 'F5n={input_output:"Cable",', 'F5n={input_output:"Cable",triggers:"Zap",'),
        BundlePatch("triggers.category_color", 'Ex={inputs:"#10B981",', 'Ex={triggers:"#29B5E8",inputs:"#10B981",'),
        BundlePatch("triggers.keep_sidebar_category", TRIGGERS_KEEP_CATEGORY_ORIG, TRIGGERS_KEEP_CATEGORY_NEW, expect={"1.12": 0, "1.13": 1}, reason="1.12 has no ENABLE_TRIGGERS sidebar filter"),
        BundlePatch("triggers.drop_native_sidebar_entry", TRIGGERS_NATIVE_ENTRY_ORIG, TRIGGERS_NATIVE_ENTRY_NEW, expect={"1.12": (0, 1), "1.13": 1}, reason="1.12 has no native triggers category (its layout already matches)"),
        BundlePatch("stages.rail_item", '{id:"memories",icon:"BrainCog",label:"memory.sidebarTitle",tooltip:"memory.sidebarTitle"},{id:"traces",icon:"Activity",label:"sidebar.nav.traces",tooltip:"sidebar.nav.traces"}],dzn=new Set(["agent","memories","traces"])', '{id:"memories",icon:"BrainCog",label:"memory.sidebarTitle",tooltip:"memory.sidebarTitle"},{id:"stages",icon:"Database",label:"Stages & Volumes",tooltip:"Stages & Volumes"},{id:"traces",icon:"Activity",label:"sidebar.nav.traces",tooltip:"sidebar.nav.traces"}],dzn=new Set(["agent","memories","stages","traces"])'),
        BundlePatch("stages.rail_icon", 'children:[v.jsx(qe,{name:m.icon,className:"h-5 w-5"}),v.jsx("span",{className:"sr-only",children:t(m.label)})]', 'children:[m.id==="stages"?' + bucket_svg_inline + ':v.jsx(qe,{name:m.icon,className:"h-5 w-5"}),v.jsx("span",{className:"sr-only",children:t(m.label)})]'),
        BundlePatch("stages.rail_panel_flag", 'br=p==="traces"||p==="memories"||p==="agent"', 'br=p==="traces"||p==="memories"||p==="stages"||p==="agent"'),
        BundlePatch("stages.rail_panel_host", 'return n==="agent"?v.jsx(cio,{}):o?v.jsx("div",{className:"flex h-full w-full flex-col overflow-hidden","data-testid":"flow-insights-embedded",children:v.jsx(kmo,{flowId:e,refreshOnMount:!0,showFlowActivityHeader:!0})}):s?v.jsx(rso,{}):v.jsx(Jgo,{setIsLoading:t})', 'return n==="agent"?v.jsx(cio,{}):o?v.jsx("div",{className:"flex h-full w-full flex-col overflow-hidden","data-testid":"flow-insights-embedded",children:v.jsx(kmo,{flowId:e,refreshOnMount:!0,showFlowActivityHeader:!0})}):s?v.jsx(rso,{}):n==="stages"?v.jsx("div",{id:"sflf-stages-react-host","data-testid":"snowflake-stages-react-host",className:"flex h-full w-full flex-col overflow-hidden"}):v.jsx(Jgo,{setIsLoading:t})'),
        BundlePatch("auth.health_503_no_block", 'throw s===null&&u?.response?.status===503?o("timeout"):s===null&&o("serverDown"),u', 'return o(null),{status:"ok",chat:"ok",db:"ok"}'),
        BundlePatch("auth.autologin_401_retry", 'if(E.name!=="CanceledError"){a(!1);const z=QKr(E.response?.data);!u&&!z&&await b()}', 'if(E.name!=="CanceledError"){const z=QKr(E.response?.data);if(z){a(!1)}else{await b()}}'),
        BundlePatch(
            "auth.redirect_guard",
            r'(sessionStorage\.getItem\("testMockAutoLogin"\),[\w$]+=)![\w$]+&&([\w$]+)!==void 0&&\(!\2\|\|![\w$]+\);',
            r"\1!1;",
            mode="regex",
            applied=r'sessionStorage\.getItem\("testMockAutoLogin"\),[\w$]+=!1;',
            hints=['"testMockAutoLogin"'],
        ),
        BundlePatch("auth.login_guard", 'const Xct=({children:e})=>{const t=Kl(o=>o.autoLogin),n=Kl(o=>o.isAuthenticated);if(t===!0||n){', 'const Xct=({children:e})=>{const t=Kl(o=>o.autoLogin),n=Kl(o=>o.isAuthenticated);if(!0){'),
        # playgroundAuthGate: the else-branch is followed by setSessionProcessed(true)
        BundlePatch("auth.playground_session_probe", 'else h&&!h.authenticated&&kBn()&&o(!1);', 'else h&&!h.authenticated&&o(!0);'),
        # AppInitPage: same canSessionProbeClearAuth() probe, ternary form in 1.12 and else-if form in 1.13
        BundlePatch(
            "auth.app_init_session_probe",
            r"([\w$]+)&&!\1\.authenticated&&[\w$]+\(\)&&([\w$]+)\(!1\)(?=\},\[\1\]\))",
            r"\1&&!\1.authenticated&&\2(!0)",
            mode="regex",
            applied=r"([\w$]+)&&!\1\.authenticated&&[\w$]+\(!0\)\},\[\1\]\)",
            hints=[".authenticated&&"],
        ),
        BundlePatch("auth.logout_reset", 'e({isAdmin:!1,userData:null,accessToken:null,isAuthenticated:!1,autoLogin:!1,apiKey:null})', 'e({isAdmin:!0,userData:null,accessToken:"auto",isAuthenticated:!0,autoLogin:!0,apiKey:null})'),
        BundlePatch(
            "vector_stores.snowflake_entries",
            r'([\w$]+=\[)(\{id:"(?:chroma",label:"Chroma Local|sqlite",label:"SQLite Local)")',
            r"\1" + sf_cq_entries + r"\2",
            mode="regex",
            applied=r'=\[\{id:"snowflake_table"',
            hints=['label:"Chroma Local"', 'label:"SQLite Local"', 'id:"snowflake_table"'],
        ),
        BundlePatch(
            "vector_stores.snowflake_configured",
            r'(function [\w$]+\(e,t,n=!0(?:,i,s=\[\])?\)\{)(return e==="chroma")',
            r'\1if(typeof e==="string"&&e.startsWith("snowflake_"))return!0;\2',
            mode="regex",
            applied=r'\(e,t,n=!0(?:,i,s=\[\])?\)\{if\(typeof e==="string"&&e\.startsWith\("snowflake_"\)\)return!0;',
            hints=['==="chroma"'],
        ),
        BundlePatch(
            "vector_stores.snowflake_backend_passthrough",
            r'(function [\w$]+\(e,t\)\{)(return (?:e==="sqlite"\?"sqlite":)?e==="opensearch"\?"opensearch":)',
            r'\1if(typeof e==="string"&&e.startsWith("snowflake_"))return e;\2',
            mode="regex",
            applied=r'\(e,t\)\{if\(typeof e==="string"&&e\.startsWith\("snowflake_"\)\)return e;return',
            hints=['e==="opensearch"?"opensearch":'],
        ),
        BundlePatch(
            "model_providers.snowflake_enabled_models",
            r"(function [\w$]+\(e,t,n,[\w$]+\)\{)(?=const s=e\.enabled_models_by_type\?\.\[t\];)",
            r'\1if(t==="Snowflake Cortex"||typeof t==="string"&&(t.startsWith("Snowflake Proxy")||t==="Snowflake Private - Ollama"))return!0;',
            mode="regex",
            applied=r'\(e,t,n,[\w$]+\)\{if\(t==="Snowflake Cortex"\|\|',
            hints=["enabled_models_by_type?.[t]"],
        ),
        BundlePatch("model_providers.title_suffix", '`${e.provider||T("modelProviders.unknownProvider")} ${E&&` ${T("modelProviders.configuration")}`}`', '`${e.provider||T("modelProviders.unknownProvider")}${E?` ${T("modelProviders.configuration")}`:""}`'),
        BundlePatch("model_providers.icon_snowflake", 'm_i=e=>({OpenAI:"OpenAI",', 'm_i=e=>typeof e==="string"&&e.startsWith("Snowflake")?"Snowflake":({OpenAI:"OpenAI",'),
        BundlePatch("model_providers.hide_empty_snowflake", s_i_orig, s_i_new),
        BundlePatch(
            "model_providers.display_aliases",
            r'([\w$]+=)\{"IBM watsonx\.ai":"IBM WatsonX"\}(?=,[\w$]+=([\w$]+)=>[\w$]+\[\2\]\?\?\2)',
            r"\1" + loo_aliases_new,
            mode="regex",
            applied=r'=\{"IBM watsonx\.ai":"IBM WatsonX","Proxy - Chat":',
            hints=['"IBM watsonx.ai":"IBM WatsonX"'],
        ),
        BundlePatch("model_providers.snowflake_default_enabled", 'if(t){const z=t[x]??t[E];if(z&&z[O.name]!==!0)continue}', 'if(t){const z=t[x]??t[E],_isSf=typeof x==="string"&&x.startsWith("Snowflake ");if(z&&(_isSf?z[O.name]===!1:z[O.name]!==!0))continue}'),
        BundlePatch("model_providers.snowflake_default_enabled_2", 'if(t&&n)for(const O of n){const x=O.provider,E=ej(x);if(p.has(E))continue;const z=t[x]??t[E];if(z)for(const T of O.models??[]){const k=T.model_name;if(z[k]!==!0)continue;', 'if(t&&n)for(const O of n){const x=O.provider,E=ej(x),_isSf=typeof x==="string"&&x.startsWith("Snowflake ");if(p.has(E))continue;const z=t[x]??t[E];if(z||_isSf)for(const T of O.models??[]){const k=T.model_name;if(_isSf?z?.[k]===!1:z?.[k]!==!0)continue;'),
        BundlePatch("model_providers.normalize_snowflake_models", gqn_orig, gqn_new, symbols={"ej": "PROVIDER_ALIAS"}, fresh=("r", "p", "sp")),
        BundlePatch(
            "nodes.skip_snowflake_mount_apikey_prefill",
            'i==="model"&&e.template?.api_key!=null&&!e.template?.api_key?.value',
            'i==="model"&&e.template?.api_key!=null&&e.template?.api_key?.show!==!1&&!e.template?.api_key?.value',
            expect={"1.12": (0, 1), "1.13": 1},
            reason="1.12 may not have needApiKeyPrefill",
        ),
    ]
    apply_bundle_patches(LANGFLOW_FRONTEND / "assets", bundle_patches, prepend_index=dom_guard_banner)
    if not REPORT_ONLY:
        for js_asset in (LANGFLOW_FRONTEND / "assets").glob("index-*.js"):
            txt = js_asset.read_text(encoding="utf-8", errors="ignore")
            if "memories-plus-snowflake-btn" in txt:
                assert '"\x2b Snowflake"' not in txt, "Legacy '+ Snowflake' label found in patched React bundle!"
                assert "openSnowflakeMemoryTypeDropdown" in txt, "Missing openSnowflakeMemoryTypeDropdown in patched React bundle!"

    # 6. Copy snowflake-ui-integration.js & preserved_provisioning_and_memories_concept.js into Langflow static frontend and link in index.html + cache-busting query param
    if not REPORT_ONLY:
        ui_js_src = SOURCE_ROOT / "frontend_patches" / "snowflake-ui-integration.js"
        shutil.copyfile(ui_js_src, LANGFLOW_FRONTEND / "snowflake-ui-integration.js")
        preserved_js_src = SOURCE_ROOT / "frontend_patches" / "preserved_provisioning_and_memories_concept.js"
        if preserved_js_src.exists():
            shutil.copyfile(preserved_js_src, LANGFLOW_FRONTEND / "preserved_provisioning_and_memories_concept.js")

    favicon_data_uri = _svg_data_uri("#29B5E8")
    initial_loader_svg = (
        f'<svg xmlns="http://www.w3.org/2000/svg" width="40" height="40" viewBox="0 0 64 64" '
        f'fill="#29B5E8" class="h-10 w-10 shrink-0 text-[#29B5E8]" data-testid="index-initial-snowflake-loader">'
        f'<path d="{OFFICIAL_SNOWFLAKE_PATH}"/></svg>'
    )
    index_html = LANGFLOW_FRONTEND / "index.html"
    html_text = index_html.read_text(encoding="utf-8")
    html_text = _html_sub(html_text, "html.notranslate", r'<html lang="en">', '<html lang="en" translate="no" class="notranslate">', r'translate="no"')
    html_text = _html_sub(html_text, "html.title", r"<title>Langflow</title>", "<title>Snowflake Langflow</title>", r"<title>Snowflake Langflow</title>")
    html_text = _html_sub(
        html_text,
        "html.favicon",
        r'<link[^>]+rel=["\'](?:shortcut )?icon["\'][^>]*>',
        f'<link rel="icon" type="image/svg+xml" href="{favicon_data_uri}" />',
        None,
        expect=(1, None),
    )
    html_text = _html_sub(
        html_text,
        "html.initial_loader",
        r"(<body[\s\S]*?)(<svg[\s\S]*?</svg>)",
        lambda m: m.group(1) + initial_loader_svg,
        r'data-testid="index-initial-snowflake-loader"',
        expect=(0, 1),
        count=1,
    )
    # Exactly one module entry script is expected; it must keep the plain URL (see ENTRY_SCRIPT_RE).
    html_text = _html_sub(html_text, "html.entry_single_module_url", ENTRY_SCRIPT_RE, ENTRY_SCRIPT_REPL, None)
    inject_block = (
        '    <meta name="google" content="notranslate" />\n'
        f"    <script>{dom_guard_banner.strip()}</script>\n"
        '    <style id="sflf-brand-blue-style">\n'
        '      [data-testid="empty_page_discord_button"],\n'
        '      [data-testid="menu_discord_button"],\n'
        '      [data-testid="menu_twitter_button"],\n'
        '      [data-testid="header_right_section_wrapper"] [data-testid="icon-Discord"] { display: none !important; }\n'
        '      [data-testid="header_left_section_wrapper"],\n'
        '      [data-testid="header_left_section_wrapper"] span,\n'
        '      [data-testid="header_left_section_wrapper"] svg,\n'
        '      [data-testid="empty_page_logo_light"] svg,\n'
        '      [data-testid="snowflake-langflow-dual-loading-logo"],\n'
        '      [data-testid="snowflake-langflow-dual-loading-logo"] *,\n'
        '      [data-testid="snowflake-langflow-dual-loading-logo"] svg,\n'
        '      [data-testid="index-initial-snowflake-loader"],\n'
        '      [data-testid="loading-icon"],\n'
        '      [data-testid="loading-icon"] svg,\n'
        '      [data-testid="loading_icon"],\n'
        '      [data-testid*="loading"],\n'
        '      [data-testid*="loading"] *,\n'
        '      [data-testid*="loading"] svg,\n'
        '      #root .loading-component,\n'
        '      #root .loading-component *,\n'
        '      #root .loading-component svg { color: #29B5E8 !important; fill: #29B5E8 !important; }\n'
        '      [data-testid="mainpage_title"],\n'
        '      [data-testid="welcome-to-snowflake-langflow-title"] { color: #29B5E8 !important; }\n'
        '      [data-testid="new-project-btn"],\n'
        '      [data-testid="empty_page_create_first_flow_button"] {\n'
        '        background-color: #29B5E8 !important;\n'
        '        border-color: #29B5E8 !important;\n'
        '        color: #ffffff !important;\n'
        "      }\n"
        '      [data-testid="new-project-btn"]:hover,\n'
        '      [data-testid="empty_page_create_first_flow_button"]:hover {\n'
        '        background-color: #0284c7 !important;\n'
        '        border-color: #0284c7 !important;\n'
        '        color: #ffffff !important;\n'
        "      }\n"
        "    </style>\n"
        '    <script defer src="./preserved_provisioning_and_memories_concept.js?v=sflf048"></script>\n'
        '    <script defer src="./snowflake-ui-integration.js?v=sflf048"></script>\n'
        "  </head>"
    )
    if "snowflake-ui-integration.js" not in html_text:
        html_text = _html_sub(html_text, "html.inject_block", r"  </head>", lambda _m: inject_block, None)
    else:
        html_text = _html_sub(
            html_text,
            "html.inject_block_refresh",
            INJECT_BLOCK_REFRESH_RE,
            lambda _m: inject_block,
            None,
        )
    if REPORT_ONLY:
        return
    index_html.write_text(html_text, encoding="utf-8")
    print("[install_live_hooks] Updated index.html with Snowflake SVG favicon, title, and snowflake-ui-integration.js?v=sflf048")



def install_all() -> None:
    import orjson
    if not LANGFLOW_FAMILY:
        _configure_paths()
    # 0a. Configure writable UV_CACHE_DIR (0777) and SQLite WAL + busy_timeout=30000 on /persist/langflow.db
    ensure_container_stability_and_sqlite_wal()

    # 0. Capture any existing non-placeholder proxy/ollama origin before overwriting site-packages
    prev_text = ""
    for existing_std in (
        SITE_PACKAGES / "standard_langflow.py",
        Path("/app/standard_langflow.py"),
        SITE_PACKAGES / "lfx_snowflake" / "standard_langflow.py",
    ):
        if existing_std.exists():
            txt = existing_std.read_text(encoding="utf-8", errors="ignore")
            if "<PROXY_SERVICE>" not in txt and "ORIGIN" in txt:
                prev_text = txt
                break

    # Install lfx_snowflake package into site-packages so all runtime/live bridge imports resolve
    pkg_src = SOURCE_ROOT / "src" / "lfx_snowflake"
    pkg_dst = SITE_PACKAGES / "lfx_snowflake"
    if pkg_src.exists():
        if pkg_dst.exists():
            shutil.rmtree(pkg_dst)
        shutil.copytree(pkg_src, pkg_dst)
        print(f"[install_live_hooks] Installed lfx_snowflake package into {pkg_dst}")

    # 0b. Copy or vendor updated standard_langflow.py if present so W2-R2-07 & proxy origin resolution are active in all 3 locations
    for std_candidate in (
        SOURCE_ROOT / "src" / "lfx_snowflake" / "standard_langflow.py",
        Path("/tmp/standard_langflow.py"),
        Path("/opt/llm-proxy-standard/langflow/standard_langflow.py"),
        SOURCE_ROOT / "standard_langflow.py",
    ):
        if std_candidate.exists():
            cand_text = std_candidate.read_text(encoding="utf-8")
            if prev_text and "<PROXY_SERVICE>" in cand_text:
                m_orig = re.search(r"ORIGIN\s*=\s*['\"](http[^'\"]+)['\"]", prev_text)
                m_ollama = re.search(r"['\"]ollama['\"]\s*:\s*['\"](http[^'\"]+)['\"]", prev_text)
                if m_orig and "<PROXY_SERVICE>" not in m_orig.group(1):
                    cand_text = cand_text.replace(
                        "http://<PROXY_SERVICE>.<DNS_NAMESPACE>:8080",
                        m_orig.group(1),
                    )
                if m_ollama and "<OLLAMA_SERVICE>" not in m_ollama.group(1):
                    cand_text = cand_text.replace(
                        "http://<OLLAMA_SERVICE>.<DNS_NAMESPACE>:11434/v1",
                        m_ollama.group(1),
                    )
            for std_target in (
                SITE_PACKAGES / "lfx_snowflake" / "standard_langflow.py",
                SITE_PACKAGES / "standard_langflow.py",
                Path("/app/standard_langflow.py"),
            ):
                if std_target.parent.exists():
                    std_target.write_text(cand_text, encoding="utf-8")
                    print(f"[install_live_hooks] Installed standard_langflow.py into {std_target}")
            break

    # 1. Empty /opt/standard-components so NO pink-cube external bundle tabs appear at the bottom of the sidebar
    if COMPONENTS_ROOT.exists():
        for child in COMPONENTS_ROOT.iterdir():
            if child.is_dir():
                shutil.rmtree(child)
            else:
                child.unlink()
    print("[install_live_hooks] Cleared /opt/standard-components to eliminate separate bottom bundle tabs.")

    # 2. Remove stale / relocated component files and copy all Round 5 component modules into lfx/components/<native_category>/
    stale_component_files = (
        ("data_source", "snowflake_object_provisioner.py"),
        ("data_source", "snowflake_data_source.py"),
        ("llm_operations", "snowflake_object_provisioner.py"),
        ("llm_operations", "snowflake_stored_procedure.py"),
        ("llm_operations", "snowflake_function_udf.py"),
        ("llm_operations", "snowflake_cortex_search.py"),
        ("llm_operations", "snowflake_cortex_analyst_llm.py"),
        ("llm_operations", "snowflake_smart_router.py"),
        ("files_and_knowledge", "snowflake_knowledge_memory.py"),
        ("files_and_knowledge", "snowflake_cortex_analyst_disabled.py"),
        ("files_and_knowledge", "snowflake_cortex_search_parse_document.py"),
        ("files_and_knowledge", "snowflake_cortex_search_split_text.py"),
        ("files_and_knowledge", "snowflake_cortex_search_service_creator.py"),
        ("files_and_knowledge", "snowflake_cortex_search_stream_task.py"),
        ("files_and_knowledge", "snowflake_object_provisioner_fk.py"),
        ("utilities", "snowflake_python_pypi.py"),
    )
    for stale_cat, stale_fname in stale_component_files:
        stale_path = LFX_COMPONENTS_DIR / stale_cat / stale_fname
        if stale_path.exists():
            stale_path.unlink()
            print(f"[install_live_hooks] Removed stale {stale_cat}/{stale_fname}")

    # Register Snowflake Cortex embedding provider, language model providers, & vector store backends before component indexing
    try:
        from lfx_snowflake.providers.embeddings import register_snowflake_cortex_embedding_provider

        register_snowflake_cortex_embedding_provider()
        print("[install_live_hooks] Registered Snowflake Cortex (AI_EMBED) embedding provider (6 models).")
    except Exception as exc:
        print(f"[install_live_hooks] Warning registering Snowflake Cortex embedding provider: {exc}")

    try:
        from lfx_snowflake.providers.language_models import register_snowflake_language_model_providers

        register_snowflake_language_model_providers()
        print("[install_live_hooks] Registered Snowflake Language Model providers (Chat, Responses, Messages, Ollama).")
    except Exception as exc:
        print(f"[install_live_hooks] Warning registering Snowflake Language Model providers: {exc}")

    try:
        from lfx_snowflake.fastapi_live_bridge import register_snowflake_vector_store_backends

        register_snowflake_vector_store_backends()
        print("[install_live_hooks] Registered Snowflake Vector Store backends.")
    except Exception as exc:
        print(f"[install_live_hooks] Warning registering Snowflake Vector Store backends: {exc}")

    _patch_upstream_models_and_agents_files()

    try:
        from lfx_snowflake.providers.language_models import register_snowflake_language_model_providers

        register_snowflake_language_model_providers()
    except Exception:
        pass

    skipped_fk_component_files = {
        "snowflake_cortex_search_parse_document.py",
        "snowflake_cortex_search_split_text.py",
        "snowflake_cortex_search_service_creator.py",
        "snowflake_cortex_search_stream_task.py",
        "snowflake_object_provisioner_fk.py",
    }
    non_indexed_fk_helper_files = {
        "snowflake_stage_files.py",
        "snowflake_canonical_fk.py",
        "snowflake_memory_base.py",
    }
    non_indexed_ma_helper_files = {
        "snowflake_standard_chat.py",
        "snowflake_standard_responses.py",
        "snowflake_standard_messages.py",
        "snowflake_standard_ollama.py",
    }

    native_src = SOURCE_ROOT / "src" / "lfx_snowflake" / "native_category_components"
    installed_modules: list[tuple[str, str]] = []
    for cat_dir in sorted(native_src.iterdir()):
        if not cat_dir.is_dir() or cat_dir.name.startswith("_"):
            continue
        target_cat_dir = LFX_COMPONENTS_DIR / cat_dir.name
        target_cat_dir.mkdir(parents=True, exist_ok=True)
        (target_cat_dir / "__init__.py").touch(exist_ok=True)
        if cat_dir.name == "triggers" and (cat_dir / "__init__.py").exists():
            shutil.copyfile(cat_dir / "__init__.py", target_cat_dir / "__init__.py")
        for py_file in sorted(cat_dir.glob("*.py")):
            if py_file.name == "__init__.py":
                continue
            if cat_dir.name == "files_and_knowledge" and py_file.name in skipped_fk_component_files:
                continue
            dest_file = target_cat_dir / py_file.name
            shutil.copyfile(py_file, dest_file)
            if cat_dir.name == "files_and_knowledge" and py_file.name in non_indexed_fk_helper_files:
                # Keep helper modules on disk for Python imports, while preserving the 5 original Langflow Files & Knowledge tabs
                continue
            if cat_dir.name == "models_and_agents" and py_file.name in non_indexed_ma_helper_files:
                # Keep standalone proxy modules on disk for backward-compatible Python imports, while unifying sidebar into Language Model & Agent (MA-W2-R6-04)
                continue
            mod_fqn = f"lfx.components.{cat_dir.name}.{py_file.stem}"
            installed_modules.append((cat_dir.name, mod_fqn))
            print(f"[install_live_hooks] Installed native component module: {mod_fqn}")

    # 3. Rebuild lfx/_assets/component_index.json with all Snowflake components merged into native categories
    from lfx.interface.components import _process_single_module

    blob = orjson.loads(LFX_INDEX_JSON.read_bytes())
    entries_map: dict[str, dict] = {cat: dict(comps) for cat, comps in blob["entries"]}
    stale_index_entries = (
        ("data_source", "SnowflakeObjectProvisioner"),
        ("data_source", "SnowflakeDataSource"),
        ("llm_operations", "SnowflakeObjectProvisioner"),
        ("llm_operations", "SnowflakeStoredProcedure"),
        ("llm_operations", "SnowflakeFunctionUDF"),
        ("llm_operations", "SnowflakeCortexSearch"),
        ("llm_operations", "SnowflakeCortexAnalystLLM"),
        ("llm_operations", "SnowflakeSmartRouter"),
        ("files_and_knowledge", "SnowflakeKnowledgeMemory"),
        ("files_and_knowledge", "SnowflakeCortexAnalystDisabledCreator"),
        ("files_and_knowledge", "SnowflakeCortexSearchParseDocument"),
        ("files_and_knowledge", "SnowflakeCortexSearchSplitText"),
        ("files_and_knowledge", "SnowflakeCortexSearchServiceCreator"),
        ("files_and_knowledge", "SnowflakeCortexSearchStreamTask"),
        ("files_and_knowledge", "SnowflakeObjectProvisionerFK"),
        ("files_and_knowledge", "SnowflakeStageFileSystem"),
        ("files_and_knowledge", "SnowflakeMemoryBase"),
        ("models_and_agents", "StandardChat"),
        ("models_and_agents", "StandardResponses"),
        ("models_and_agents", "StandardMessages"),
        ("models_and_agents", "PrivateOllama"),
        ("models_and_agents", "SnowflakeCortexAgentA2A"),
        ("models_and_agents", "Memory"),
        ("models_and_agents", "SnowflakeMessageHistory"),
        ("models_and_agents", "MCPTools"),
        ("utilities", "SnowflakePythonInterpreterPyPI"),
    )
    for stale_cat, stale_comp in stale_index_entries:
        if stale_cat in entries_map:
            entries_map[stale_cat].pop(stale_comp, None)

    for cat_name, mod_fqn in installed_modules:
        res = _process_single_module(mod_fqn)
        if not res or not res[1]:
            raise RuntimeError(f"Failed to template native component module: {mod_fqn}")
        top_level, comp_templates = res
        assert top_level == cat_name
        entries_map.setdefault(cat_name, {}).update(comp_templates)
        print(f"[install_live_hooks] Merged {list(comp_templates.keys())} into native category '{cat_name}'")

    # TRG-W2-R8-01: Expose native Webhook in 'triggers' category while keeping 'input_output' 100% invariant
    if "input_output" in entries_map and "Webhook" in entries_map["input_output"]:
        entries_map.setdefault("triggers", {})["Webhook"] = copy.deepcopy(
            entries_map["input_output"]["Webhook"]
        )

    # Re-template patched upstream Models & Agents modules so component_index.json has the updated code & inputs
    for ma_mod in ("language_model", "agent", "a2a_agent", "embedding_model", "prompt"):
        ma_res = _process_single_module(f"lfx.components.models_and_agents.{ma_mod}")
        if ma_res and ma_res[1]:
            entries_map.setdefault("models_and_agents", {}).update(ma_res[1])

    # Populate Snowflake LLM & Embedding options, default selection, and field_order in component_index.json
    ma_entries = entries_map.setdefault("models_and_agents", {})
    try:
        from lfx_snowflake.providers.language_models import (
            get_snowflake_live_language_model_catalog,
            update_snowflake_language_model_build_config,
        )

        sf_lm_opts = get_snowflake_live_language_model_catalog(force_refresh=False)
        default_luna = next(
            (opt for opt in sf_lm_opts if opt.get("name") == "openai-gpt-6-luna"),
            sf_lm_opts[0] if sf_lm_opts else None,
        )
        for lm_key in ("LanguageModelComponent", "Agent"):
            if lm_key in ma_entries and isinstance(ma_entries[lm_key], dict):
                comp_dict = ma_entries[lm_key]
                tmpl = comp_dict.setdefault("template", {})
                if "model" in tmpl and isinstance(tmpl["model"], dict):
                    tmpl["model"]["options"] = list(sf_lm_opts)
                    tmpl["model"]["_connection_mode"] = False
                    if default_luna is not None:
                        tmpl["model"]["value"] = [dict(default_luna)]
                update_snowflake_language_model_build_config(tmpl, field_name="model", force_refresh=False)
                f_order = [
                    f
                    for f in list(comp_dict.get("field_order") or [])
                    if f not in ("reasoning_effort", "extended_thinking", "thinking_budget_tokens")
                ]
                m_idx = f_order.index("model") if "model" in f_order else 0
                comp_dict["field_order"] = (
                    f_order[: m_idx + 1]
                    + ["reasoning_effort", "extended_thinking", "thinking_budget_tokens"]
                    + f_order[m_idx + 1 :]
                )
    except Exception as exc:
        print(f"[install_live_hooks] Warning populating LanguageModelComponent/Agent template options: {exc}")

    try:
        from lfx_snowflake.providers.embeddings import get_snowflake_cortex_embedding_catalog

        sf_emb_cat = get_snowflake_cortex_embedding_catalog()
        sf_emb_opts = [
            {
                "name": item["model_name"],
                "model_name": item["model_name"],
                "provider": "Snowflake Cortex",
                "category": "Snowflake Cortex",
                "icon": "Snowflake",
                "default": True,
                "metadata": dict(item["metadata"]),
            }
            for item in sf_emb_cat
        ]
        if "EmbeddingModel" in ma_entries and isinstance(ma_entries["EmbeddingModel"], dict):
            emb_tmpl = ma_entries["EmbeddingModel"].setdefault("template", {})
            if "model" in emb_tmpl and isinstance(emb_tmpl["model"], dict):
                emb_tmpl["model"]["options"] = list(sf_emb_opts)
                emb_tmpl["model"]["_connection_mode"] = False
                if sf_emb_opts and not emb_tmpl["model"].get("value"):
                    emb_tmpl["model"]["value"] = [dict(sf_emb_opts[0])]
            if "api_key" in emb_tmpl and isinstance(emb_tmpl["api_key"], dict):
                emb_tmpl["api_key"]["show"] = False
                emb_tmpl["api_key"]["required"] = False
    except Exception as exc:
        print(f"[install_live_hooks] Warning populating EmbeddingModel template options: {exc}")

    # Map SnowflakeMessageHistoryNativeComponent under canonical key "MessageHistory" and pop duplicate keys (MA-W2-R6-04)
    if "SnowflakeMessageHistory" in ma_entries:
        mh_entry = dict(ma_entries.pop("SnowflakeMessageHistory"))
        mh_entry["display_name"] = "Message History"
        mh_entry["icon"] = "Snowflake"
        mh_entry["legacy"] = False
        ma_entries["MessageHistory"] = mh_entry
    for dup_ma in (
        "StandardChat",
        "StandardResponses",
        "StandardMessages",
        "PrivateOllama",
        "SnowflakeCortexAgentA2A",
        "Memory",
        "SnowflakeMessageHistory",
        "MCPTools",
        "PoliciesComponent",
    ):
        ma_entries.pop(dup_ma, None)

    canonical_ma_keys = {
        "A2AAgent",
        "Agent",
        "SnowflakeCortexAgent",
        "EmbeddingModel",
        "LanguageModelComponent",
        "MessageHistory",
        "Prompt Template",
    }
    for extra_ma_key in list(ma_entries.keys()):
        if extra_ma_key not in canonical_ma_keys:
            if isinstance(ma_entries.get(extra_ma_key), dict) and not ma_entries[extra_ma_key].get("legacy"):
                ma_entries.pop(extra_ma_key, None)

    # Restore and guarantee the 5 original Langflow components in files_and_knowledge (each from its own 1-class module)
    for orig_fk_mod in ("filesystem", "knowledge", "memory_retrieval", "file", "save_file"):
        fk_res = _process_single_module(f"lfx.components.files_and_knowledge.{orig_fk_mod}")
        if fk_res and fk_res[1]:
            entries_map.setdefault("files_and_knowledge", {}).update(fk_res[1])

    # Final cleanup of files_and_knowledge to guarantee strictly the 5 canonical non-legacy components
    for dup_fk in (
        "SnowflakeCortexSearchParseDocument",
        "SnowflakeCortexSearchSplitText",
        "SnowflakeCortexSearchServiceCreator",
        "SnowflakeCortexSearchStreamTask",
        "SnowflakeObjectProvisionerFK",
        "SnowflakeStageFileSystem",
        "SnowflakeMemoryBase",
        "SnowflakeKnowledgeMemory",
        "SnowflakeCortexAnalystDisabledCreator",
    ):
        entries_map.get("files_and_knowledge", {}).pop(dup_fk, None)

    blob["entries"] = [[cat, comps] for cat, comps in entries_map.items()]
    blob["metadata"]["num_modules"] = len(blob["entries"])
    blob["metadata"]["num_components"] = sum(len(comps) for _, comps in blob["entries"])
    blob.pop("sha256", None)
    digest = hashlib.sha256(orjson.dumps(blob, option=orjson.OPT_SORT_KEYS)).hexdigest()
    blob["sha256"] = digest
    LFX_INDEX_JSON.write_bytes(orjson.dumps(blob, option=orjson.OPT_SORT_KEYS))
    print(f"[install_live_hooks] Updated component_index.json (sha256={digest[:16]}...)")

    patch_runtime_sources()
    assert_patch_ledger_clean()
    print(format_patch_report())

    # 7. Verify component_index.json integrity, unified_models catalog, and FastAPI routes
    from langflow.main import create_app
    from langflow.plugin_routes import _get_route_keys
    from lfx.base.models import unified_models as _um_check
    from lfx.interface.components import _read_component_index

    verified_idx = _read_component_index()
    assert verified_idx is not None, "component_index.json failed SHA256 or structure verification!"
    idx_cats = dict(verified_idx["entries"])

    for io_comp in ("ChatInput", "ChatOutput", "TextInput", "TextOutput", "Webhook"):
        assert io_comp in idx_cats["input_output"], f"Missing invariant {io_comp} in input_output"
    assert "triggers" in idx_cats, "Missing triggers category in component_index.json"
    for trg_comp in (
        "SnowflakeScheduleTrigger",
        "SnowflakeTableChangeTrigger",
        "SnowflakeStageFileTrigger",
        "SnowflakeAlertTrigger",
        "Webhook",
    ):
        assert trg_comp in idx_cats["triggers"], f"Missing {trg_comp} in triggers"

    for ds_comp in (
        "SnowflakeTables",
        "SnowflakeViews",
        "SnowflakeIcebergTables",
        "SnowflakePostgres",
        "SnowflakeCortexSearch",
        "SnowflakeCortexAnalyst",
    ):
        assert ds_comp in idx_cats["data_source"], f"Missing {ds_comp} in data_source"
    assert "SnowflakeDataSource" not in idx_cats["data_source"]
    assert "SnowflakeObjectProvisioner" not in idx_cats["data_source"]

    for ma_comp in (
        "A2AAgent",
        "Agent",
        "SnowflakeCortexAgent",
        "EmbeddingModel",
        "LanguageModelComponent",
        "MessageHistory",
        "Prompt Template",
    ):
        assert ma_comp in idx_cats["models_and_agents"], f"Missing {ma_comp} in models_and_agents"
    non_legacy_ma = sorted(
        k for k, v in idx_cats["models_and_agents"].items() if not (isinstance(v, dict) and v.get("legacy"))
    )
    assert non_legacy_ma == sorted(
        [
            "A2AAgent",
            "Agent",
            "SnowflakeCortexAgent",
            "EmbeddingModel",
            "LanguageModelComponent",
            "MessageHistory",
            "Prompt Template",
        ]
    ), f"Expected strictly 7 canonical non-legacy components in models_and_agents, got: {non_legacy_ma}"
    for ma_removed in (
        "StandardChat",
        "StandardResponses",
        "StandardMessages",
        "PrivateOllama",
        "SnowflakeCortexAgentA2A",
        "Memory",
        "SnowflakeMessageHistory",
        "MCPTools",
    ):
        assert ma_removed not in idx_cats["models_and_agents"], (
            f"Removed duplicate {ma_removed} still in models_and_agents"
        )

    for lm_comp_key in ("LanguageModelComponent", "Agent"):
        lm_comp_entry = idx_cats["models_and_agents"][lm_comp_key]
        assert lm_comp_entry["field_order"][:4] == [
            "model",
            "reasoning_effort",
            "extended_thinking",
            "thinking_budget_tokens",
        ], f"Unexpected field_order on {lm_comp_key}: {lm_comp_entry['field_order'][:6]}"
        lm_model_tmpl = lm_comp_entry["template"]["model"]
        assert len(lm_model_tmpl.get("options") or []) >= 12, (
            f"Expected >= 12 Snowflake LLM options in {lm_comp_key}.template.model.options, got {len(lm_model_tmpl.get('options') or [])}"
        )
        assert lm_model_tmpl["value"][0]["name"] == "openai-gpt-6-luna", (
            f"Expected default model openai-gpt-6-luna on {lm_comp_key}, got {lm_model_tmpl.get('value')}"
        )

    llm_detailed = {
        p["provider"]: p
        for p in _um_check.get_unified_models_detailed(model_type="llm")
    }
    expected_sf_llm_counts = {
        "Snowflake Proxy - Chat (/chat/v1)": 6,
        "Snowflake Proxy - Responses (/responses/v1)": 2,
        "Snowflake Proxy - Messages (/messages)": 3,
        "Snowflake Private - Ollama": 1,
    }
    for exp_prov, exp_min_count in expected_sf_llm_counts.items():
        assert exp_prov in llm_detailed, f"Missing {exp_prov} in get_unified_models_detailed(model_type='llm')!"
        prov_models = llm_detailed[exp_prov]["models"]
        assert len(prov_models) >= exp_min_count, (
            f"Expected >= {exp_min_count} models for {exp_prov}, got {len(prov_models)}"
        )
        assert all(m["metadata"].get("default") is True for m in prov_models), (
            f"Expected all models in {exp_prov} to have default=True, got {[m['metadata'].get('default') for m in prov_models]}"
        )
    emb_detailed = {
        p["provider"]: p
        for p in _um_check.get_unified_models_detailed(model_type="embeddings")
    }
    assert "Snowflake Cortex" in emb_detailed, "Missing Snowflake Cortex in get_unified_models_detailed(model_type='embeddings')!"
    assert len(emb_detailed["Snowflake Cortex"]["models"]) == 6
    assert all(m["metadata"].get("default") is True for m in emb_detailed["Snowflake Cortex"]["models"])

    for llm_comp in (
        "SnowflakeCustomTool",
        "SnowflakeAIComplete",
        "SnowflakeAIFunction",
        "SnowflakeAIRedact",
        "SnowflakeSQLStatement",
        "SnowflakeLLMNode",
        "SnowflakeDecision",
    ):
        assert llm_comp in idx_cats["llm_operations"], f"Missing {llm_comp} in llm_operations"
    assert "SnowflakeObjectProvisioner" not in idx_cats["llm_operations"]
    assert "SnowflakeStoredProcedure" not in idx_cats["llm_operations"]
    assert "SnowflakeFunctionUDF" not in idx_cats["llm_operations"]
    assert "SnowflakeCortexSearch" not in idx_cats["llm_operations"]
    assert "SnowflakeCortexAnalystLLM" not in idx_cats["llm_operations"]
    assert "SnowflakeSmartRouter" not in idx_cats["llm_operations"]

    expected_fk_display_and_icons = {
        "FileSystemTool": ("File System", "folder"),
        "Knowledge": ("Knowledge", "database"),
        "MemoryBase": ("Memory Base", "brain"),
        "File": ("Read File", "file-text"),
        "SaveToFile": ("Write File", "file-text"),
    }
    for fk_canonical, (exp_disp, exp_icon) in expected_fk_display_and_icons.items():
        assert fk_canonical in idx_cats["files_and_knowledge"], f"Missing canonical {fk_canonical} in files_and_knowledge"
        actual_entry = idx_cats["files_and_knowledge"][fk_canonical]
        assert actual_entry.get("display_name") == exp_disp, (
            f"Expected {fk_canonical} display_name={exp_disp!r}, got {actual_entry.get('display_name')!r}"
        )
        assert actual_entry.get("icon") == exp_icon, (
            f"Expected {fk_canonical} icon={exp_icon!r}, got {actual_entry.get('icon')!r}"
        )
    non_legacy_fk = sorted(
        k for k, v in idx_cats["files_and_knowledge"].items() if not (isinstance(v, dict) and v.get("legacy"))
    )
    assert non_legacy_fk == sorted(["FileSystemTool", "Knowledge", "MemoryBase", "File", "SaveToFile"]), (
        f"Expected strictly 5 canonical non-legacy components in files_and_knowledge, got: {non_legacy_fk}"
    )
    for fk_removed in (
        "SnowflakeObjectProvisionerFK",
        "SnowflakeStageFileSystem",
        "SnowflakeCortexSearchParseDocument",
        "SnowflakeCortexSearchSplitText",
        "SnowflakeCortexSearchServiceCreator",
        "SnowflakeCortexSearchStreamTask",
        "SnowflakeMemoryBase",
        "SnowflakeKnowledgeMemory",
    ):
        assert fk_removed not in idx_cats["files_and_knowledge"], f"Removed duplicate {fk_removed} still in files_and_knowledge"

    assert "SplitText" in idx_cats["processing"], "Missing SplitText in processing"
    assert "SnowflakeToolGuardPolicies" in idx_cats["utilities"]
    assert "SnowflakePythonInterpreterPyPI" not in idx_cats["utilities"]

    from lfx.custom.eval import eval_custom_component_code

    for proxy_fname in (
        "snowflake_standard_chat.py",
        "snowflake_standard_messages.py",
        "snowflake_standard_responses.py",
        "snowflake_standard_ollama.py",
    ):
        proxy_path = LFX_COMPONENTS_DIR / "models_and_agents" / proxy_fname
        compiled_cls = eval_custom_component_code(proxy_path.read_text(encoding="utf-8"))
        assert "standard_langflow" in compiled_cls.build_model.__globals__, (
            f"Missing standard_langflow in {proxy_fname} build_model globals"
        )
        assert "standard_langflow" in compiled_cls.update_build_config.__globals__, (
            f"Missing standard_langflow in {proxy_fname} update_build_config globals"
        )

    app = create_app()
    sf_paths = sorted({path for path, _m in _get_route_keys(app) if "/api/v1/snowflake" in path})
    for req_route in (
        "/api/v1/snowflake/context",
        "/api/v1/snowflake/catalog",
        "/api/v1/snowflake/provision",
        "/api/v1/snowflake/memories",
        "/api/v1/snowflake/memories/recall",
        "/api/v1/snowflake/memories/showcase/full-memory-enterprise-agent",
        "/api/v1/snowflake/stages",
        "/api/v1/snowflake/external-volumes",
        "/api/v1/snowflake/embedding-models",
        "/api/v1/snowflake/language-models",
        "/api/v1/snowflake/cortex/threads",
        "/api/v1/snowflake/a2a/targets",
        "/api/v1/snowflake/a2a/cortex-agents/{database}/{schema}/{agent_name}/.well-known/agent-card.json",
        "/api/v1/snowflake/a2a/cortex-agents/{database}/{schema}/{agent_name}/jsonrpc",
        "/api/v1/snowflake/traces",
    ):
        assert req_route in sf_paths, f"Missing route {req_route} in {sf_paths}"
    print("[install_live_hooks] Verification passed: 28 components merged into 5 native categories + 0 bottom bundles.")


def main(argv: list[str] | None = None) -> int:
    import argparse

    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--report", action="store_true", help="dry run: print per-patch match counts, write nothing")
    ap.add_argument("--site-packages", type=Path, help="site-packages root (default: auto-detected)")
    ap.add_argument("--frontend", type=Path, help="built Langflow frontend dir (default: <site-packages>/langflow/frontend)")
    ap.add_argument("--langflow-version", help="Langflow version of the base (default: installed langflow-base)")
    args = ap.parse_args(argv)
    global REPORT_ONLY
    _configure_paths(args.site_packages, args.frontend, args.langflow_version)
    if args.report:
        REPORT_ONLY = True
        _patch_upstream_models_and_agents_files()
        patch_runtime_sources()
        print(format_patch_report())
        return 1 if any(r["status"] == "FAIL" for r in PATCH_LEDGER) else 0
    install_all()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
