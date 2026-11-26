"""Gateway configuration, read from environment variables."""
import os
from dataclasses import dataclass


def _int_env(name: str, default: int, minimum: int, maximum: int) -> int:
    raw = os.getenv(name, str(default))
    try:
        value = int(raw)
    except ValueError as exc:
        raise RuntimeError(f"{name} must be an integer, got {raw!r}") from exc
    if not minimum <= value <= maximum:
        raise RuntimeError(f"{name} must be between {minimum} and {maximum}, got {value}")
    return value


@dataclass(frozen=True)
class Settings:
    legacy_url: str
    candidate_url: str
    max_inflight: int
    shadow_timeout_ms: int
    legacy_timeout_ms: int
    comparison_limit: int
    default_delay_ms: int

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            legacy_url=os.getenv("LEGACY_SEARCH_URL", "http://localhost:8101").rstrip("/"),
            candidate_url=os.getenv(
                "CANDIDATE_SEARCH_URL", "http://localhost:8102"
            ).rstrip("/"),
            # Hard cap: a round may never have more than four shadow requests in
            # flight at the same time.  Kept configurable only so tests can
            # exercise the limit with a smaller value.
            max_inflight=_int_env("MAX_INFLIGHT_SHADOWS", 4, 1, 64),
            shadow_timeout_ms=_int_env("SHADOW_TIMEOUT_MS", 8000, 100, 120_000),
            legacy_timeout_ms=_int_env("LEGACY_TIMEOUT_MS", 5000, 100, 60_000),
            comparison_limit=_int_env("COMPARISON_LIMIT", 10, 1, 50),
            default_delay_ms=_int_env("SHADOW_DEFAULT_DELAY_MS", 1500, 0, 60_000),
        )
