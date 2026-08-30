"""INFRA-186: Model tier characterization from config; exact aliases here, not prose."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import yaml

_VALID_EFFORTS = frozenset({"low", "medium", "high"})
_REQUIRED_TIERS = frozenset({"haiku", "sonnet", "fable"})


@dataclass(frozen=True, slots=True)
class ModelTier:
    """Characterized model tier with Agent-tool model alias and default effort."""

    name: str
    model: str
    default_effort: str
    policy: str


def load_model_tiers(path: Path) -> dict[str, ModelTier]:
    """Load and validate model tier configuration from YAML.

    Raises ValueError if:
    - Document is missing or not a mapping
    - Missing 'tiers' key
    - Tier set is not exactly {haiku, sonnet, fable}
    - Any tier has empty/non-string model or effort
    - Any effort is not in {low, medium, high}
    - Any policy is empty/non-string
    """
    with path.open(encoding="utf-8") as stream:
        document = yaml.safe_load(stream)

    if not isinstance(document, dict):
        raise ValueError("model tier configuration must be a mapping")

    if "tiers" not in document:
        raise ValueError("model tier configuration must contain 'tiers' key")

    raw_tiers = document["tiers"]
    if not isinstance(raw_tiers, dict):
        raise ValueError("'tiers' must be a mapping")

    tier_names = set(raw_tiers.keys())
    if tier_names != _REQUIRED_TIERS:
        required = sorted(_REQUIRED_TIERS)
        raise ValueError(
            f"model tier configuration must define exactly three tiers: {required}"
        )

    tiers: dict[str, ModelTier] = {}
    for tier_name in sorted(_REQUIRED_TIERS):
        raw_tier = raw_tiers[tier_name]
        if not isinstance(raw_tier, dict):
            raise ValueError(f"tier '{tier_name}' must be a mapping")

        # Validate and extract model
        model = raw_tier.get("model")
        if not isinstance(model, str) or not model:
            raise ValueError(
                f"tier '{tier_name}' model must be a non-empty string"
            )

        # Validate and extract default_effort
        effort = raw_tier.get("default_effort")
        if not isinstance(effort, str) or not effort:
            raise ValueError(
                f"tier '{tier_name}' default_effort must be a non-empty string"
            )
        if effort not in _VALID_EFFORTS:
            valid = sorted(_VALID_EFFORTS)
            raise ValueError(
                f"tier '{tier_name}' effort must be one of {valid}, got '{effort}'"
            )

        # Validate and extract policy
        policy = raw_tier.get("policy")
        if not isinstance(policy, str) or not policy:
            raise ValueError(
                f"tier '{tier_name}' policy must be a non-empty string"
            )

        tiers[tier_name] = ModelTier(
            name=tier_name,
            model=model,
            default_effort=effort,
            policy=policy,
        )

    return tiers
