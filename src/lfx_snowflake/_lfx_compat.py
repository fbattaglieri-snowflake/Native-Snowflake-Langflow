"""Lightweight fallback definitions for `lfx.*` when running outside the Langflow container."""

from __future__ import annotations

from typing import Any


class _BaseInput(dict):
    """Dict-like and attribute-accessible input descriptor compatible with Langflow build_config."""

    input_type: str = "StrInput"

    def __init__(self, **kwargs: Any) -> None:
        defaults: dict[str, Any] = {
            "name": kwargs.get("name", ""),
            "display_name": kwargs.get("display_name", kwargs.get("name", "")),
            "value": kwargs.get("value", ""),
            "options": list(kwargs.get("options", [])),
            "show": kwargs.get("show", True),
            "advanced": kwargs.get("advanced", False),
            "real_time_refresh": kwargs.get("real_time_refresh", False),
            "refresh_button": kwargs.get("refresh_button", False),
            "combobox": kwargs.get("combobox", False),
            "tool_mode": kwargs.get("tool_mode", False),
            "required": kwargs.get("required", False),
            "is_list": kwargs.get("is_list", False),
            "input_types": list(kwargs.get("input_types", [])),
            "info": kwargs.get("info", ""),
            "_input_type": self.input_type,
        }
        defaults.update(kwargs)
        super().__init__(defaults)
        for k, v in defaults.items():
            setattr(self, k, v)

    def __setitem__(self, key: str, value: Any) -> None:
        super().__setitem__(key, value)
        setattr(self, key, value)

    def to_dict(self) -> dict[str, Any]:
        return dict(self)


class StrInput(_BaseInput):
    input_type = "StrInput"


class MultilineInput(_BaseInput):
    input_type = "MultilineInput"


class MessageTextInput(_BaseInput):
    input_type = "MessageTextInput"


class DropdownInput(_BaseInput):
    input_type = "DropdownInput"


class IntInput(_BaseInput):
    input_type = "IntInput"


class FloatInput(_BaseInput):
    input_type = "FloatInput"


class BoolInput(_BaseInput):
    input_type = "BoolInput"


class DataInput(_BaseInput):
    input_type = "DataInput"


class HandleInput(_BaseInput):
    input_type = "HandleInput"


class Output(dict):
    def __init__(
        self,
        *,
        display_name: str = "",
        name: str = "",
        method: str = "",
        types: list[str] | None = None,
        **kwargs: Any,
    ) -> None:
        payload = {
            "display_name": display_name,
            "name": name,
            "method": method,
            "types": list(types or []),
            **kwargs,
        }
        super().__init__(payload)
        for k, v in payload.items():
            setattr(self, k, v)


class Data:
    def __init__(self, data: dict[str, Any] | None = None, **kwargs: Any) -> None:
        self.data: dict[str, Any] = dict(data or {})
        if kwargs:
            self.data.update(kwargs)


class DataFrame(list):
    def __init__(self, rows: list[dict[str, Any]] | None = None) -> None:
        super().__init__(rows or [])
        self.rows = list(rows or [])


class Message:
    def __init__(self, text: str = "", **kwargs: Any) -> None:
        self.text = text
        self.data = {"text": text, **kwargs}


class Tool:
    """Lightweight LangChain/Langflow Tool representation for offline tests."""

    def __init__(
        self,
        *,
        name: str,
        description: str,
        func: Any = None,
        coroutine: Any = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        self.name = name
        self.description = description
        self.func = func
        self.coroutine = coroutine
        self.metadata = dict(metadata or {})

    def invoke(self, tool_input: Any = None, **kwargs: Any) -> Any:
        if callable(self.func):
            return self.func(tool_input, **kwargs) if kwargs else self.func(tool_input)
        return {"tool": self.name, "input": tool_input}

    def run(self, tool_input: Any = None, **kwargs: Any) -> Any:
        return self.invoke(tool_input, **kwargs)


LanguageModel = Any


class Component:
    display_name: str = ""
    description: str = ""
    icon: str = "Snowflake"
    name: str = ""
    inputs: list[Any] = []
    outputs: list[Any] = []

    def __init__(self, **kwargs: Any) -> None:
        for inp in getattr(self, "inputs", []):
            inp_name = getattr(inp, "name", None) or (inp.get("name") if isinstance(inp, dict) else None)
            inp_val = getattr(inp, "value", None) if not isinstance(inp, dict) else inp.get("value")
            if inp_name and not hasattr(self, inp_name):
                setattr(self, inp_name, inp_val)
        for k, v in kwargs.items():
            setattr(self, k, v)

    def to_build_config(self) -> dict[str, dict[str, Any]]:
        cfg: dict[str, dict[str, Any]] = {}
        for inp in getattr(self, "inputs", []):
            inp_name = getattr(inp, "name", None) or (inp.get("name") if isinstance(inp, dict) else None)
            if inp_name:
                if isinstance(inp, dict):
                    cfg[inp_name] = dict(inp)
                elif hasattr(inp, "to_dict"):
                    cfg[inp_name] = inp.to_dict()
                else:
                    cfg[inp_name] = {"name": inp_name, "value": getattr(inp, "value", None), "show": True}
        return cfg


class LCModelComponent(Component):
    @staticmethod
    def get_base_inputs() -> list[Any]:
        return [
            MessageTextInput(name="input_value", display_name="Input", value=""),
            StrInput(name="system_message", display_name="System Message", value="", advanced=True),
            BoolInput(name="stream", display_name="Stream", value=False, advanced=True),
        ]


def ensure_lfx_shim_installed() -> None:
    """Install lightweight `lfx.*` module shims only when running outside the Langflow container."""
    import importlib.util
    import sys
    import types

    if "lfx" in sys.modules or importlib.util.find_spec("lfx") is not None:
        return

    lfx_mod = types.ModuleType("lfx")
    custom_mod = types.ModuleType("lfx.custom")
    custom_mod.Component = Component  # type: ignore[attr-defined]

    io_mod = types.ModuleType("lfx.io")
    for cls in (
        BoolInput,
        DataInput,
        DropdownInput,
        FloatInput,
        HandleInput,
        IntInput,
        MessageTextInput,
        MultilineInput,
        Output,
        StrInput,
    ):
        setattr(io_mod, cls.__name__, cls)

    schema_mod = types.ModuleType("lfx.schema")
    schema_mod.Data = Data  # type: ignore[attr-defined]
    schema_mod.DataFrame = DataFrame  # type: ignore[attr-defined]
    schema_mod.Message = Message  # type: ignore[attr-defined]

    msg_mod = types.ModuleType("lfx.schema.message")
    msg_mod.Message = Message  # type: ignore[attr-defined]

    base_mod = types.ModuleType("lfx.base")
    base_models_mod = types.ModuleType("lfx.base.models")
    base_model_mod = types.ModuleType("lfx.base.models.model")
    base_model_mod.LCModelComponent = LCModelComponent  # type: ignore[attr-defined]

    ft_mod = types.ModuleType("lfx.field_typing")
    ft_mod.LanguageModel = LanguageModel  # type: ignore[attr-defined]
    ft_mod.Tool = Tool  # type: ignore[attr-defined]

    sys.modules["lfx"] = lfx_mod
    sys.modules["lfx.custom"] = custom_mod
    sys.modules["lfx.io"] = io_mod
    sys.modules["lfx.schema"] = schema_mod
    sys.modules["lfx.schema.message"] = msg_mod
    sys.modules["lfx.base"] = base_mod
    sys.modules["lfx.base.models"] = base_models_mod
    sys.modules["lfx.base.models.model"] = base_model_mod
    sys.modules["lfx.field_typing"] = ft_mod


ensure_lfx_shim_installed()


def ensure_standard_langflow_bound() -> None:
    """Bind `standard_langflow` in `sys.modules` and `builtins` so canvas-serialized nodes never fail with NameError."""
    import builtins
    import sys

    try:
        from lfx_snowflake import standard_langflow as _std_lf

        sys.modules["standard_langflow"] = _std_lf
        setattr(builtins, "standard_langflow", _std_lf)
    except Exception:  # noqa: BLE001
        pass


ensure_standard_langflow_bound()


