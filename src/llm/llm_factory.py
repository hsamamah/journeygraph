# src/llm/llm_factory.py
"""
llm_factory.py — LLM provider abstraction for the JourneyGraph query pipeline.

Builds a neo4j-graphrag LLMInterface instance from LLMConfig. All pipeline
components that need an LLM receive an LLMInterface — they never reference
a provider-specific class directly.

Adding a new provider:
    1. Install the relevant neo4j-graphrag extra e.g. neo4j-graphrag[openai]
    2. Add the provider import and branch below
    3. Add the provider's API key to LLMConfig and get_llm_config()
    4. Update the .env documentation in README or CONVENTIONS.md

Current supported providers:
    anthropic — AnthropicLLM via neo4j-graphrag[anthropic]
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from neo4j_graphrag.llm import AnthropicLLM, LLMInterface

from src.common.logger import get_logger

if TYPE_CHECKING:
    from src.common.config import LLMConfig

log = get_logger(__name__)


def build_llm(config: LLMConfig) -> LLMInterface:
    """
    Build and return an LLMInterface from LLMConfig.

    The returned instance is safe to reuse across multiple invoke() calls.
    Callers should instantiate once (e.g. in Planner.__init__) rather than
    calling build_llm() per query.

    Args:
        config: Validated LLMConfig from get_llm_config().

    Returns:
        LLMInterface implementation for the configured provider.

    Raises:
        ValueError: if config.llm_provider is not a supported provider.
    """
    provider = config.llm_provider.lower()

    if provider == "anthropic":
        log.debug(
            "Building AnthropicLLM — model=%s, max_tokens=%d",
            config.llm_model,
            config.llm_max_tokens,
        )
        return AnthropicLLM(
            model_name=config.llm_model,
            model_params={"max_tokens": config.llm_max_tokens},
            # api_key is read automatically from ANTHROPIC_API_KEY env var
            # by the anthropic SDK — consistent with how neo4j driver reads
            # NEO4J_URI. Explicit passing would duplicate what LLMConfig holds.
        )

    raise ValueError(
        f"Unsupported LLM provider: '{config.llm_provider}'. "
        f"Supported providers: ['anthropic']. "
        "To add a new provider, see the module docstring in llm_factory.py."
    )
