import lfx_snowflake._lfx_compat  # noqa: F401
from lfx.base.models.model import LCModelComponent
from lfx.io import DropdownInput, IntInput
from lfx.field_typing import LanguageModel

try:
    import standard_langflow
except ImportError:  # pragma: no cover
    from lfx_snowflake import standard_langflow


class StandardChat(LCModelComponent):
    display_name = "Proxy - Chat"
    name = "StandardChat"
    icon = "Snowflake"
    description = "Snowflake Standard Proxy Chat (/chat/v1). Live discovery, retry=0, no fallback, model-aware reasoning_effort."
    inputs = [
        *LCModelComponent.get_base_inputs(),
        DropdownInput(
            name="model_id",
            display_name="Published model",
            options=["openai-gpt-6-luna", "openai-gpt-6-sol", "grok-4.7", "grok-4.6", "kimi-k3", "glm-5.3"],
            value="openai-gpt-6-luna",
            refresh_button=True,
            real_time_refresh=True,
        ),
        DropdownInput(
            name="reasoning_effort",
            display_name="Reasoning Effort",
            options=["unspecified", "none", "low", "medium", "high"],
            value="none",
            advanced=False,
        ),
        IntInput(name="max_tokens", display_name="Output tokens", value=2048, advanced=True),
    ]

    def _remediate_model_error(self, *args):
        return False

    def update_build_config(self, build_config, field_value, field_name=None):
        return standard_langflow.refresh(build_config, "chat")

    def build_model(self) -> LanguageModel:
        return standard_langflow.build(
            "chat",
            self.model_id,
            self.stream,
            self.reasoning_effort,
            self.max_tokens,
            reasoning_effort=self.reasoning_effort,
        )
