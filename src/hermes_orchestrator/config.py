"""Validated, non-secret configuration for the orchestrator."""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import Any, Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field

_SECRET_KEY_PARTS = ("token", "secret", "password", "api_key", "email", "phone")


class ProjectConfig(BaseModel):
    """Routing and repository metadata for one registered project."""

    model_config = ConfigDict(extra="forbid")

    linear_team: str = Field(min_length=1)
    repo_path: Path
    integration_branch: str = Field(min_length=1)
    github_repo: str = Field(pattern=r"^[^/\s]+/[^/\s]+$")


class ResourcePolicy(BaseModel):
    """Resource thresholds; null values keep admission observation-only."""

    model_config = ConfigDict(extra="forbid")

    calibrated: bool = False
    yellow_available_memory_gib: float | None = Field(default=None, gt=0)
    red_available_memory_gib: float | None = Field(default=None, gt=0)
    yellow_available_disk_gib: float | None = Field(default=None, gt=0)
    red_available_disk_gib: float | None = Field(default=None, gt=0)


class PolicyConfig(BaseModel):
    """Global orchestration policy."""

    model_config = ConfigDict(extra="forbid")

    mode: Literal["observe"] = "observe"
    max_unresolved_ci_merges: Literal[2] = 2
    context_prepare_percent: int = Field(default=70, ge=1, le=100)
    context_rotate_percent: int = Field(default=80, ge=1, le=100)
    max_active_session_hours: int = Field(default=6, ge=1)
    stall_consultations_before_automation: Literal[2] = 2
    resource_thresholds: ResourcePolicy = Field(default_factory=ResourcePolicy)


class Settings(BaseModel):
    """Complete validated configuration used by the service."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    repo_root: Path
    state_dir: Path
    projects: dict[str, ProjectConfig]
    policy: PolicyConfig


def _read_yaml(path: Path) -> dict[str, Any]:
    with path.open(encoding="utf-8") as stream:
        loaded = yaml.safe_load(stream)
    if loaded is None:
        return {}
    if not isinstance(loaded, dict):
        raise ValueError(f"configuration root must be a mapping: {path}")
    return loaded


def _reject_secret_like_keys(value: object, location: str = "projects") -> None:
    if isinstance(value, Mapping):
        for raw_key, child in value.items():
            key = str(raw_key).lower().replace("-", "_")
            if any(part in key for part in _SECRET_KEY_PARTS):
                raise ValueError(
                    f"secret-like key is not allowed at {location}.{raw_key}"
                )
            _reject_secret_like_keys(child, f"{location}.{raw_key}")
    elif isinstance(value, list):
        for index, child in enumerate(value):
            _reject_secret_like_keys(child, f"{location}[{index}]")


def load_settings(repo_root: Path, state_dir: Path | None = None) -> Settings:
    """Load repository configuration without creating runtime directories."""

    resolved_root = repo_root.expanduser().resolve(strict=False)
    config_dir = resolved_root / "config"
    projects_path = config_dir / "projects.yaml"
    if not projects_path.exists():
        projects_path = config_dir / "projects.example.yaml"

    projects_document = _read_yaml(projects_path)
    project_values = projects_document.get("projects", {})
    if not isinstance(project_values, dict):
        raise ValueError("projects must be a mapping")
    _reject_secret_like_keys(project_values)

    policy_document = _read_yaml(config_dir / "policies.yaml")
    resolved_state = (
        state_dir
        if state_dir is not None
        else Path.home() / ".local" / "share" / "hermes-orchestrator"
    ).expanduser().resolve(strict=False)

    return Settings(
        repo_root=resolved_root,
        state_dir=resolved_state,
        projects={
            alias: ProjectConfig.model_validate(project)
            for alias, project in project_values.items()
        },
        policy=PolicyConfig.model_validate(policy_document),
    )
