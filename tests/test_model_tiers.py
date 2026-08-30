"""Tests for model tier configuration and characterization."""

from pathlib import Path

import pytest

from hermes_orchestrator.model_tiers import ModelTier, load_model_tiers


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
    assert sonnet.default_effort == "high"
    assert sonnet.policy
    assert len(sonnet.policy) > 0


def test_fable_tier_properties():
    """Fable tier maps to the Agent-tool alias 'fable' with correct properties."""
    config_path = Path(__file__).parent.parent / "config" / "model-tiers.yaml"
    tiers = load_model_tiers(config_path)

    fable = tiers["fable"]
    assert fable.name == "fable"
    assert fable.model == "fable"
    assert fable.default_effort == "high"
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
