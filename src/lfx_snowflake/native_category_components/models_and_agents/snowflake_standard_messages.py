import lfx_snowflake._lfx_compat  # noqa: F401
from lfx.base.models.model import LCModelComponent
from lfx.io import DropdownInput, IntInput
from lfx.field_typing import LanguageModel

try:
    import standard_langflow
except ImportError:  # pragma: no cover
    from lfx_snowflake import standard_langflow


class StandardMessages(LCModelComponent):
    display_name = "Proxy - Messages"
    name = "StandardMessages"
    icon = "Snowflake"
    description = "Snowflake Standard Proxy Anthropic Messages (/messages/v1/messages) with Extended Thinking budget_tokens and thinking.signature preserved."
    inputs = [
        *LCModelComponent.get_base_inputs(),
        DropdownInput(
            name="model_id",
            display_name="Published model",
            options=["claude-sonnet-5-5", "claude-opus-5-5", "claude-haiku-5-5"],
            value="claude-sonnet-5-5",
            refresh_button=True,
            real_time_refresh=True,
        ),
        DropdownInput(
            name="extended_thinking",
            display_name="Extended Thinking",
            options=["disabled", "enabled"],
            value="disabled",
            real_time_refresh=True,
            advanced=False,
        ),
        IntInput(
            name="thinking_budget_tokens",
            display_name="Thinking Budget Tokens (1024..32768)",
            value=2048,
            advanced=False,
        ),
        IntInput(name="max_tokens", display_name="Output tokens", value=4096, advanced=True),
    ]

    def _remediate_model_error(self, *args):
        return False

    def update_build_config(self, build_config, field_value, field_name=None):
        return standard_langflow.refresh(build_config, "messages")

    def build_model(self) -> LanguageModel:
        return standard_langflow.build(
            "messages",
            self.model_id,
            self.stream,
            max_tokens=self.max_tokens,
            extended_thinking=getattr(self, "extended_thinking", "disabled") or "disabled",
            thinking_budget_tokens=int(getattr(self, "thinking_budget_tokens", 2048) or 2048),
        )
