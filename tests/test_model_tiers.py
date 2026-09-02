"""Tests for model tier configuration and characterization."""

import json
from pathlib import Path
from uuid import UUID

import pytest

from hermes_orchestrator import claude as claude_module
from hermes_orchestrator.claude import ClaudeRunner, LeadTurnRequest
from hermes_orchestrator.cli import subagent_gate
from hermes_orchestrator.model_tiers import ModelTier, load_model_tiers
from hermes_orchestrator.profiles import ProfileRegistry


def test_load_real_config():
    """Real config/model-tiers.yaml loads and contains exactly three tiers."""
    config_path = Path(__file__).parent.parent / "config" / "model-tiers.yaml"
    tiers = load_model_tiers(config_path)

    assert len(tiers) == 3
    assert set(tiers.keys()) == {"haiku", "sonnet", "fable"}


def test_haiku_tier_properties():
    """Haiku tier maps to the Agent-tool alias 'haiku' with correct properties."""
    config_path = Path(__file__).parent.parent / "config" / "model-tiers.yaml"
    tiers = load_model_tiers(config_path)

    haiku = tiers["haiku"]
    assert haiku.name == "haiku"
    assert haiku.model == "haiku"
    assert haiku.default_effort == "medium"
    assert haiku.policy
    assert len(haiku.policy) > 0


def test_sonnet_tier_properties():
    """Sonnet tier maps to the Agent-tool alias 'sonnet' with correct properties."""
    config_path = Path(__file__).parent.parent / "config" / "model-tiers.yaml"
    tiers = load_model_tiers(config_path)

    sonnet = tiers["sonnet"]
    assert sonnet.name == "sonnet"
    assert sonnet.model == "sonnet"
    # INFRA-211: ordinary bounded implementation work is medium.
    assert sonnet.default_effort == "medium"
    assert sonnet.policy
    assert len(sonnet.policy) > 0


def test_fable_tier_properties():
    """Fable tier maps to the Agent-tool alias 'fable' with correct properties."""
    config_path = Path(__file__).parent.parent / "config" / "model-tiers.yaml"
    tiers = load_model_tiers(config_path)

    fable = tiers["fable"]
    assert fable.name == "fable"
    assert fable.model == "fable"
    # INFRA-211: the managed lead plans at medium unless justified higher.
    assert fable.default_effort == "medium"
    assert fable.policy
    assert len(fable.policy) > 0


def test_model_tier_is_frozen():
    """ModelTier is a frozen dataclass."""
    tier = ModelTier(
        name="test", model="test", default_effort="medium", policy="test policy"
    )

    with pytest.raises((AttributeError, TypeError)):
        tier.name = "modified"  # type: ignore[attr-defined]


def test_missing_tier_refused(tmp_path):
    """Configuration missing a tier is refused."""
    config_file = tmp_path / "model-tiers.yaml"
    config_file.write_text(
        """\
tiers:
  haiku:
    model: "haiku"
    default_effort: "medium"
    policy: "mechanical work"
  sonnet:
    model: "sonnet"
    default_effort: "high"
    policy: "bounded multi-file"
"""
    )

    with pytest.raises(ValueError, match="exactly three tiers"):
        load_model_tiers(config_file)


def test_extra_tier_refused(tmp_path):
    """Configuration with extra tier is refused."""
    config_file = tmp_path / "model-tiers.yaml"
    config_file.write_text(
        """\
tiers:
  haiku:
    model: "haiku"
    default_effort: "medium"
    policy: "mechanical work"
  sonnet:
    model: "sonnet"
    default_effort: "high"
    policy: "bounded multi-file"
  fable:
    model: "fable"
    default_effort: "high"
    policy: "orchestration work"
  extra:
    model: "extra"
    default_effort: "high"
    policy: "extra tier"
"""
    )

    with pytest.raises(ValueError, match="exactly three tiers"):
        load_model_tiers(config_file)


def test_bad_effort_refused(tmp_path):
    """Configuration with invalid effort level is refused."""
    config_file = tmp_path / "model-tiers.yaml"
    config_file.write_text(
        """\
tiers:
  haiku:
    model: "haiku"
    default_effort: "invalid"
    policy: "mechanical work"
  sonnet:
    model: "sonnet"
    default_effort: "high"
    policy: "bounded multi-file"
  fable:
    model: "fable"
    default_effort: "high"
    policy: "orchestration work"
"""
    )

    with pytest.raises(ValueError, match="effort must be one of"):
        load_model_tiers(config_file)


def test_missing_model_refused(tmp_path):
    """Configuration with missing model field is refused."""
    config_file = tmp_path / "model-tiers.yaml"
    config_file.write_text(
        """\
tiers:
  haiku:
    default_effort: "medium"
    policy: "mechanical work"
  sonnet:
    model: "sonnet"
    default_effort: "high"
    policy: "bounded multi-file"
  fable:
    model: "fable"
    default_effort: "high"
    policy: "orchestration work"
"""
    )

    with pytest.raises(ValueError, match="non-empty string"):
        load_model_tiers(config_file)


def test_empty_model_refused(tmp_path):
    """Configuration with empty model string is refused."""
    config_file = tmp_path / "model-tiers.yaml"
    config_file.write_text(
        """\
tiers:
  haiku:
    model: ""
    default_effort: "medium"
    policy: "mechanical work"
  sonnet:
    model: "sonnet"
    default_effort: "high"
    policy: "bounded multi-file"
  fable:
    model: "fable"
    default_effort: "high"
    policy: "orchestration work"
"""
    )

    with pytest.raises(ValueError, match="non-empty string"):
        load_model_tiers(config_file)


def test_empty_effort_refused(tmp_path):
    """Configuration with empty effort string is refused."""
    config_file = tmp_path / "model-tiers.yaml"
    config_file.write_text(
        """\
tiers:
  haiku:
    model: "haiku"
    default_effort: ""
    policy: "mechanical work"
  sonnet:
    model: "sonnet"
    default_effort: "high"
    policy: "bounded multi-file"
  fable:
    model: "fable"
    default_effort: "high"
    policy: "orchestration work"
"""
    )

    with pytest.raises(ValueError, match="non-empty string"):
        load_model_tiers(config_file)


def test_empty_policy_refused(tmp_path):
    """Configuration with empty policy string is refused."""
    config_file = tmp_path / "model-tiers.yaml"
    config_file.write_text(
        """\
tiers:
  haiku:
    model: "haiku"
    default_effort: "medium"
    policy: ""
  sonnet:
    model: "sonnet"
    default_effort: "high"
    policy: "bounded multi-file"
  fable:
    model: "fable"
    default_effort: "high"
    policy: "orchestration work"
"""
    )

    with pytest.raises(ValueError, match="non-empty string"):
        load_model_tiers(config_file)


def test_missing_document_refused(tmp_path):
    """Configuration with missing document is refused."""
    config_file = tmp_path / "model-tiers.yaml"
    config_file.write_text("")

    with pytest.raises(ValueError, match="must be a mapping"):
        load_model_tiers(config_file)


def test_non_mapping_document_refused(tmp_path):
    """Configuration with non-mapping document is refused."""
    config_file = tmp_path / "model-tiers.yaml"
    config_file.write_text("- item1\n- item2\n")

    with pytest.raises(ValueError, match="must be a mapping"):
        load_model_tiers(config_file)


def test_missing_tiers_key_refused(tmp_path):
    """Configuration without tiers key is refused."""
    config_file = tmp_path / "model-tiers.yaml"
    config_file.write_text("other_key: value\n")

    with pytest.raises(ValueError, match=r"must contain.*tiers"):
        load_model_tiers(config_file)


def test_policies_are_non_empty():
    """All policies in real config are non-empty strings."""
    config_path = Path(__file__).parent.parent / "config" / "model-tiers.yaml"
    tiers = load_model_tiers(config_path)

    for tier_name, tier in tiers.items():
        assert tier.policy, f"Policy for {tier_name} is empty"
        assert isinstance(tier.policy, str)
        assert len(tier.policy) > 0


# INFRA-211: the effort a managed launch records must come from the tier
# config above, not from a literal at the launch site.

REAL_CONFIG = Path(__file__).parent.parent / "config" / "model-tiers.yaml"


def _runner(tmp_path: Path) -> ClaudeRunner:
    config = tmp_path / "profiles.yaml"
    config.write_text(
        "profiles:\n"
        + "".join(
            f"  - alias: max-{alias}\n    config_dir: {tmp_path / alias}\n"
            for alias in ("a", "b", "c", "d")
        ),
        encoding="utf-8",
    )
    return ClaudeRunner(
        ProfileRegistry.load(config),
        prompt_file=tmp_path / "claude-lead.md",
        base_env={"PATH": "/usr/bin"},
    )


def _lead_effort(runner: ClaudeRunner, tmp_path: Path) -> str:
    command, _env = runner.build_command(
        LeadTurnRequest(
            session_id=UUID("11111111-1111-4111-8111-111111111111"),
            cwd=tmp_path,
            prompt="Plan ENG-9",
            profile_alias="max-a",
        )
    )
    return command[command.index("--effort") + 1]


def test_lead_turn_effort_is_the_configured_fable_default(tmp_path):
    """The managed lead turn records the fable tier's configured effort."""
    claude_module.configured_model_tiers.cache_clear()
    assert _lead_effort(_runner(tmp_path), tmp_path) == "medium"
    assert (
        _lead_effort(_runner(tmp_path), tmp_path)
        == load_model_tiers(REAL_CONFIG)["fable"].default_effort
    )


def test_lead_turn_effort_follows_the_tier_config(tmp_path, monkeypatch):
    """Reconfiguring the fable tier moves the lead turn -- no literal survives."""
    config = tmp_path / "model-tiers.yaml"
    config.write_text(
        REAL_CONFIG.read_text(encoding="utf-8").replace(
            'model: "fable"\n    default_effort: "medium"',
            'model: "fable"\n    default_effort: "low"',
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(claude_module, "_MODEL_TIERS_PATH", config)
    claude_module.configured_model_tiers.cache_clear()
    try:
        assert _lead_effort(_runner(tmp_path), tmp_path) == "low"
    finally:
        claude_module.configured_model_tiers.cache_clear()


class _RecordingAdmission:
    """Records the effort the gate hands admission; always allows."""

    def __init__(self) -> None:
        self.effort: str | None = None

    def admit(self, *, session_id, packet_id, model, effort, tool_use_id):
        self.effort = effort
        return type("Decision", (), {"allowed": True, "reason": "reserved"})()


def _gate_effort(tmp_path: Path, tool_input: dict) -> str | None:
    admission = _RecordingAdmission()
    payload = json.dumps(
        {"session_id": "s-1", "tool_input": tool_input, "tool_use_id": "t-1"}
    )
    code, _message = subagent_gate(tmp_path, payload, admission=admission)
    assert code == 0
    return admission.effort


def test_gate_defaults_an_unspecified_effort_to_the_tier_default(tmp_path):
    """A sonnet launch that names no effort takes sonnet's configured default."""
    claude_module.configured_model_tiers.cache_clear()
    effort = _gate_effort(
        tmp_path,
        {"description": f"packet:{'a' * 32}", "model": "sonnet"},
    )
    assert effort == load_model_tiers(REAL_CONFIG)["sonnet"].default_effort
    assert effort == "medium"


def test_gate_preserves_an_explicitly_requested_high_effort(tmp_path):
    """An explicitly justified high effort still reaches admission unchanged."""
    claude_module.configured_model_tiers.cache_clear()
    effort = _gate_effort(
        tmp_path,
        {"description": f"packet:{'a' * 32}", "model": "sonnet", "effort": "high"},
    )
    assert effort == "high"
