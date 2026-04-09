"""
config.py — single source of truth for all environment variables.

All other modules import from here instead of calling os.getenv() directly.
Add new env vars here as the project grows.

ETL pipeline config:  get_config()     → Config
LLM pipeline config:  get_llm_config() → LLMConfig

The two configs are intentionally separate. The ETL pipeline has no dependency
on LLM credentials, so a missing ANTHROPIC_API_KEY never surfaces during
a normal pipeline run. get_llm_config() is only called by src/llm/ code.
"""

from dataclasses import dataclass
import os

from dotenv import load_dotenv

load_dotenv()


def _require(key: str) -> str:
    val = os.getenv(key)
    if not val:
        raise OSError(
            f"Required environment variable '{key}' is not set. Check your .env file."
        )
    return val


@dataclass(frozen=True)
class Config:
    neo4j_uri: str
    neo4j_user: str
    neo4j_password: str
    wmata_api_key: str
    gtfs_feed_url: str


def get_config() -> Config:
    """
    Build and validate config from environment variables.
    Raises EnvironmentError if any required variable is missing.
    Call this at runtime (in pipeline.py), not at module import time.
    """
    return Config(
        neo4j_uri=_require("NEO4J_URI"),
        neo4j_user=_require("NEO4J_USER"),
        neo4j_password=_require("NEO4J_PASSWORD"),
        wmata_api_key=_require("WMATA_API_KEY"),
        gtfs_feed_url=os.getenv(
            "GTFS_FEED_URL",
            "https://api.wmata.com/gtfs/rail-bus-gtfs-static.zip",
        ),
    )


# ── LLM pipeline config ───────────────────────────────────────────────────────


@dataclass(frozen=True)
class LLMConfig:
    """
    Configuration for the LLM query pipeline.

    Separate from Config so the ETL pipeline has no dependency on LLM
    credentials. Only imported and called by src/llm/ code.

    Environment variables:
        ANTHROPIC_API_KEY          Required. API key for the Anthropic client.
        LLM_PROVIDER               Optional. Defaults to 'anthropic'.
                                   Used by llm_factory.py to select the LLM class.
        LLM_MODEL                  Optional. Defaults to 'claude-haiku-4-5-20251001'.
                                   Pinned snapshot string — not an alias — for
                                   stable, reproducible behaviour across releases.
        LLM_MAX_TOKENS             Optional. Defaults to 512.
                                   Controls Stage 2 Planner call token budget.
                                   Increase if Planner responses are being truncated.
        LLM_NARRATION_MAX_TOKENS   Optional. Defaults to 1024.
                                   Controls NarrationAgent call token budget.
                                   Kept separate from LLM_MAX_TOKENS because the
                                   Narration Agent produces the final user-facing
                                   answer and needs more headroom than the
                                   lightweight Planner JSON call.
    """

    anthropic_api_key: str
    llm_provider: str
    llm_model: str
    llm_max_tokens: int
    llm_narration_max_tokens: int


def get_llm_config() -> LLMConfig:
    """
    Build and validate LLM config from environment variables.
    Raises OSError if ANTHROPIC_API_KEY is not set.
    Call this at LLM pipeline startup, not at module import time.
    """
    return LLMConfig(
        anthropic_api_key=_require("ANTHROPIC_API_KEY"),
        llm_provider=os.getenv("LLM_PROVIDER", "anthropic"),
        llm_model=os.getenv("LLM_MODEL", "claude-haiku-4-5-20251001"),
        llm_max_tokens=int(os.getenv("LLM_MAX_TOKENS", "512")),
        llm_narration_max_tokens=int(os.getenv("LLM_NARRATION_MAX_TOKENS", "1024")),
    )
