try:
    from lfx.base.models.model import LCModelComponent
    from lfx.io import DropdownInput, IntInput
    from lfx.field_typing import LanguageModel
except ImportError:  # pragma: no cover
    from lfx_snowflake._lfx_compat import DropdownInput, IntInput, LCModelComponent, LanguageModel

try:
    import standard_langflow
except ImportError:  # pragma: no cover
    from lfx_snowflake import standard_langflow


class StandardResponses(LCModelComponent):
    display_name = "Proxy - Responses"
    name = "StandardResponses"
    icon = "Snowflake"
    description = "Snowflake Standard Proxy Responses (/responses/v1), store=false, reasoning_effort and encrypted reasoning preserved."
    inputs = [
        *LCModelComponent.get_base_inputs(),
        DropdownInput(
            name="model_id",
            display_name="Published model",
            options=["openai-gpt-6-astra", "openai-gpt-6.1-sol", "openai-gpt-6-luna", "openai-gpt-6-sol"],
            value="openai-gpt-6-astra",
            refresh_button=True,
            real_time_refresh=True,
        ),
        DropdownInput(
            name="reasoning_effort",
            display_name="Reasoning Effort",
            options=["low", "medium", "high"],
            value="medium",
            advanced=False,
        ),
        IntInput(name="max_tokens", display_name="Output tokens", value=2048, advanced=True),
    ]

    def _remediate_model_error(self, *args):
        return False

    def update_build_config(self, build_config, field_value, field_name=None):
        return standard_langflow.refresh(build_config, "responses")

    def build_model(self) -> LanguageModel:
        return standard_langflow.build(
            "responses",
            self.model_id,
            self.stream,
            max_tokens=self.max_tokens,
            reasoning_effort=getattr(self, "reasoning_effort", "medium") or "medium",
        )
