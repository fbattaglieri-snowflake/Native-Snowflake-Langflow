from __future__ import annotations

from typing import Any

import lfx_snowflake._lfx_compat  # noqa: F401
from lfx.custom import Component
from lfx.io import BoolInput, DropdownInput, HandleInput, IntInput, MessageTextInput, Output, StrInput
from lfx.schema import Data, DataFrame
from lfx.schema.message import Message

from lfx_snowflake.fastapi_live_bridge import LIVE_BRIDGE


def _unescape_sep(separator: str) -> str:
    if separator in ("/n", "\\n"):
        return "\n"
    if separator in ("/t", "\\t"):
        return "\t"
    try:
        from lfx.utils.util import unescape_string

        return str(unescape_string(separator))
    except Exception:  # noqa: BLE001
        return separator.replace("\\n", "\n").replace("\\t", "\t")


def _recursive_split_fallback(
    text: str,
    *,
    chunk_size: int = 1000,
    chunk_overlap: int = 200,
    split_format: str = "markdown",
    custom_separators: list[str] | None = None,
) -> list[str]:
    cleaned = str(text or "")
    if not cleaned:
        return []
    max_len = max(1, int(chunk_size or 1000))
    overlap = max(0, min(int(chunk_overlap or 0), max_len - 1 if max_len > 1 else 0))
    if len(cleaned) <= max_len:
        return [cleaned]

    if custom_separators:
        seps = [s for s in custom_separators if s]
    elif split_format == "markdown":
        seps = ["\n## ", "\n### ", "\n\n", "\n", " "]
    else:
        seps = ["\n\n", "\n", ". ", " "]

    try:
        from langchain_text_splitters import RecursiveCharacterTextSplitter

        splitter = RecursiveCharacterTextSplitter(
            separators=seps + [""],
            chunk_size=max_len,
            chunk_overlap=overlap,
        )
        return splitter.split_text(cleaned)
    except Exception:  # noqa: BLE001
        pass

    chunks: list[str] = []
    step = max(1, max_len - overlap)
    idx = 0
    while idx < len(cleaned):
        chunks.append(cleaned[idx : idx + max_len])
        if idx + max_len >= len(cleaned):
            break
        idx += step
    return chunks


class SplitTextComponent(Component):
    display_name: str = "Split Text"
    description: str = (
        "Split text into chunks based on specified criteria using Snowflake Cortex "
        "(SNOWFLAKE.CORTEX.SPLIT_TEXT_RECURSIVE_CHARACTER) or Standard CharacterTextSplitter."
    )
    documentation: str = "https://docs.langflow.org/split-text"
    icon = "scissors-line-dashed"
    name = "SplitText"

    inputs = [
        HandleInput(
            name="data_inputs",
            display_name="Input",
            info="The data with texts to split in chunks.",
            input_types=["Data", "JSON", "DataFrame", "Table", "Message"],
            required=True,
        ),
        DropdownInput(
            name="split_engine",
            display_name="Split Engine",
            info="Select Snowflake Cortex SPLIT_TEXT_RECURSIVE_CHARACTER or Standard CharacterTextSplitter.",
            options=[
                "Snowflake Cortex (SPLIT_TEXT_RECURSIVE_CHARACTER)",
                "Standard (CharacterTextSplitter)",
            ],
            value="Snowflake Cortex (SPLIT_TEXT_RECURSIVE_CHARACTER)",
            real_time_refresh=True,
        ),
        DropdownInput(
            name="split_format",
            display_name="Document Format (SPLIT_TEXT_RECURSIVE_CHARACTER)",
            info="Format parameter for SNOWFLAKE.CORTEX.SPLIT_TEXT_RECURSIVE_CHARACTER ('none' or 'markdown').",
            options=["none", "markdown"],
            value="markdown",
        ),
        IntInput(
            name="chunk_overlap",
            display_name="Chunk Overlap",
            info="Number of characters to overlap between chunks.",
            value=200,
        ),
        IntInput(
            name="chunk_size",
            display_name="Chunk Size",
            info=(
                "The maximum length of each chunk. Text is first split by separator, "
                "then chunks are merged up to this size."
            ),
            value=1000,
        ),
        MessageTextInput(
            name="separator",
            display_name="Separator",
            info=(
                "The character to split on for Standard CharacterTextSplitter. Use \\n for newline. "
                "Examples: \\n\\n for paragraphs, \\n for lines, . for sentences"
            ),
            value="\n",
        ),
        StrInput(
            name="separators_csv",
            display_name="Custom Separators (optional CSV)",
            info="Optional comma-separated custom separators for SNOWFLAKE.CORTEX.SPLIT_TEXT_RECURSIVE_CHARACTER.",
            value="",
            advanced=True,
        ),
        MessageTextInput(
            name="text_key",
            display_name="Text Key",
            info="The key to use for the text column.",
            value="text",
            advanced=True,
        ),
        DropdownInput(
            name="keep_separator",
            display_name="Keep Separator",
            info="Whether to keep the separator in the output chunks and where to place it.",
            options=["False", "True", "Start", "End"],
            value="False",
            advanced=True,
        ),
        BoolInput(
            name="clean_output",
            display_name="Clean Output",
            info="When enabled, only the text column is included in the output. Metadata columns are removed.",
            value=False,
            advanced=True,
        ),
    ]

    outputs = [
        Output(display_name="Chunks", name="dataframe", method="split_text"),
    ]

    def _extract_input_items(self) -> list[tuple[str, dict[str, Any]]]:
        raw = getattr(self, "data_inputs", None)
        text_key = str(getattr(self, "text_key", "text") or "text")
        if raw is None:
            raise TypeError("No data inputs provided")
        if isinstance(raw, str):
            return [(raw, {})] if raw else []
        if isinstance(raw, Message):
            txt = str(getattr(raw, "text", "") or "")
            meta = dict(getattr(raw, "data", {}) or {})
            meta.pop("text", None)
            return [(txt, meta)]
        if isinstance(raw, Data):
            d = dict(getattr(raw, "data", {}) or {})
            txt = str(d.get(text_key) or d.get("text") or getattr(raw, "text", "") or "")
            meta = {k: v for k, v in d.items() if k != text_key}
            return [(txt, meta)]
        if isinstance(raw, DataFrame):
            if hasattr(raw, "to_lc_documents"):
                docs = raw.to_lc_documents()
                return [(str(doc.page_content), dict(doc.metadata or {})) for doc in docs]
            rows = getattr(raw, "rows", None) or list(raw)
            if not rows:
                raise TypeError("DataFrame is empty")
            items: list[tuple[str, dict[str, Any]]] = []
            for row in rows:
                if isinstance(row, Data):
                    d = dict(getattr(row, "data", {}) or {})
                    txt = str(d.get(text_key) or d.get("text") or getattr(row, "text", "") or "")
                    meta = {k: v for k, v in d.items() if k != text_key}
                    items.append((txt, meta))
                elif isinstance(row, dict):
                    txt = str(row.get(text_key) or row.get("text") or "")
                    meta = {k: v for k, v in row.items() if k != text_key}
                    items.append((txt, meta))
                else:
                    items.append((str(row), {}))
            return items
        if isinstance(raw, dict):
            txt = str(raw.get(text_key) or raw.get("text") or "")
            meta = {k: v for k, v in raw.items() if k != text_key}
            return [(txt, meta)]
        if isinstance(raw, (list, tuple)):
            items = []
            for entry in raw:
                if isinstance(entry, Data):
                    d = dict(getattr(entry, "data", {}) or {})
                    txt = str(d.get(text_key) or d.get("text") or getattr(entry, "text", "") or "")
                    meta = {k: v for k, v in d.items() if k != text_key}
                    items.append((txt, meta))
                elif isinstance(entry, Message):
                    items.append((str(getattr(entry, "text", "") or ""), {}))
                elif isinstance(entry, dict):
                    txt = str(entry.get(text_key) or entry.get("text") or "")
                    meta = {k: v for k, v in entry.items() if k != text_key}
                    items.append((txt, meta))
                elif isinstance(entry, str):
                    items.append((entry, {}))
            if not items:
                raise TypeError(f"No valid Data inputs found in {type(raw)}")
            return items
        raise TypeError(f"Unsupported input type: {type(raw)}")

    def build_cortex_split_sql(self, text: str) -> str:
        safe_text = str(text or "").replace("'", "''")
        fmt = str(getattr(self, "split_format", "markdown") or "markdown").strip().lower()
        if fmt not in ("none", "markdown"):
            fmt = "markdown"
        chunk_size = max(1, int(getattr(self, "chunk_size", 1000) or 1000))
        overlap = max(0, int(getattr(self, "chunk_overlap", 200) or 0))
        seps_raw = str(getattr(self, "separators_csv", "") or "").strip()
        if seps_raw:
            parts = [_unescape_sep(p.strip()).replace("'", "''") for p in seps_raw.split(",") if p.strip()]
            if parts:
                arr_sql = "ARRAY_CONSTRUCT(" + ", ".join(f"'{p}'" for p in parts) + ")"
                return (
                    "SELECT c.value::STRING AS CHUNK, c.index AS CHUNK_INDEX "
                    "FROM LATERAL FLATTEN("
                    "input => SNOWFLAKE.CORTEX.SPLIT_TEXT_RECURSIVE_CHARACTER("
                    f"'{safe_text}', '{fmt}', {chunk_size}, {overlap}, {arr_sql}"
                    ")) c"
                )
        return (
            "SELECT c.value::STRING AS CHUNK, c.index AS CHUNK_INDEX "
            "FROM LATERAL FLATTEN("
            "input => SNOWFLAKE.CORTEX.SPLIT_TEXT_RECURSIVE_CHARACTER("
            f"'{safe_text}', '{fmt}', {chunk_size}, {overlap}"
            ")) c"
        )

    def _split_with_snowflake_cortex(self, items: list[tuple[str, dict[str, Any]]]) -> list[Data]:
        fmt = str(getattr(self, "split_format", "markdown") or "markdown").strip().lower()
        if fmt not in ("none", "markdown"):
            fmt = "markdown"
        chunk_size = max(1, int(getattr(self, "chunk_size", 1000) or 1000))
        overlap = max(0, int(getattr(self, "chunk_overlap", 200) or 0))
        seps_raw = str(getattr(self, "separators_csv", "") or "").strip()
        custom_seps = [_unescape_sep(p.strip()) for p in seps_raw.split(",") if p.strip()] if seps_raw else None
        clean = bool(getattr(self, "clean_output", False))

        out_data: list[Data] = []
        for doc_idx, (text, base_meta) in enumerate(items):
            sql_stmt = self.build_cortex_split_sql(text)
            self.last_sql = sql_stmt
            res = LIVE_BRIDGE.execute_sql(sql_stmt)
            live_chunks: list[str] = []
            if isinstance(res, dict) and res.get("mode") != "offline" and isinstance(res.get("rows"), list):
                for r in res["rows"]:
                    if isinstance(r, dict):
                        val = r.get("CHUNK") or r.get("chunk") or r.get("VALUE") or r.get("value")
                        if val is not None:
                            live_chunks.append(str(val))
            chunks = (
                live_chunks
                if live_chunks
                else _recursive_split_fallback(
                    text,
                    chunk_size=chunk_size,
                    chunk_overlap=overlap,
                    split_format=fmt,
                    custom_separators=custom_seps,
                )
            )
            for chunk_idx, chunk_text in enumerate(chunks):
                if clean:
                    out_data.append(Data(data={"text": chunk_text}, text=chunk_text))
                else:
                    meta = dict(base_meta)
                    meta.update(
                        {
                            "text": chunk_text,
                            "chunk_index": chunk_idx,
                            "doc_index": doc_idx,
                            "split_engine": "Snowflake Cortex (SPLIT_TEXT_RECURSIVE_CHARACTER)",
                            "split_format": fmt,
                            "chunk_size": chunk_size,
                            "chunk_overlap": overlap,
                            "sql": sql_stmt,
                        }
                    )
                    out_data.append(Data(data=meta, text=chunk_text))
        return out_data

    def _split_with_standard(self, items: list[tuple[str, dict[str, Any]]]) -> list[Data]:
        separator = _unescape_sep(str(getattr(self, "separator", "\n") or "\n"))
        chunk_size = max(1, int(getattr(self, "chunk_size", 1000) or 1000))
        overlap = max(0, int(getattr(self, "chunk_overlap", 200) or 0))
        clean = bool(getattr(self, "clean_output", False))

        out_data: list[Data] = []
        for doc_idx, (text, base_meta) in enumerate(items):
            chunks: list[str]
            try:
                from langchain_text_splitters import CharacterTextSplitter

                keep_sep: Any = getattr(self, "keep_separator", "False")
                if isinstance(keep_sep, str):
                    if keep_sep.lower() == "false":
                        keep_sep = False
                    elif keep_sep.lower() == "true":
                        keep_sep = True
                splitter = CharacterTextSplitter(
                    chunk_overlap=overlap,
                    chunk_size=chunk_size,
                    separator=separator,
                    keep_separator=keep_sep,
                )
                chunks = splitter.split_text(text)
            except Exception:  # noqa: BLE001
                chunks = _recursive_split_fallback(
                    text,
                    chunk_size=chunk_size,
                    chunk_overlap=overlap,
                    split_format="none",
                    custom_separators=[separator] if separator else None,
                )
            for chunk_idx, chunk_text in enumerate(chunks):
                if clean:
                    out_data.append(Data(data={"text": chunk_text}, text=chunk_text))
                else:
                    meta = dict(base_meta)
                    meta.update(
                        {
                            "text": chunk_text,
                            "chunk_index": chunk_idx,
                            "doc_index": doc_idx,
                            "split_engine": "Standard (CharacterTextSplitter)",
                            "chunk_size": chunk_size,
                            "chunk_overlap": overlap,
                        }
                    )
                    out_data.append(Data(data=meta, text=chunk_text))
        return out_data

    def split_text(self) -> DataFrame:
        items = self._extract_input_items()
        engine = str(
            getattr(self, "split_engine", "Snowflake Cortex (SPLIT_TEXT_RECURSIVE_CHARACTER)")
            or "Snowflake Cortex (SPLIT_TEXT_RECURSIVE_CHARACTER)"
        )
        if "Snowflake Cortex" in engine or "SPLIT_TEXT_RECURSIVE_CHARACTER" in engine:
            data_rows = self._split_with_snowflake_cortex(items)
        else:
            data_rows = self._split_with_standard(items)
        try:
            df = DataFrame(data_rows)
            if not getattr(self, "clean_output", False) and hasattr(df, "smart_column_order"):
                return df.smart_column_order()
            return df
        except Exception:  # noqa: BLE001
            return DataFrame([d.data if isinstance(d, Data) else {"text": str(d)} for d in data_rows])
