from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml


PROJECT_ROOT = Path(__file__).resolve().parents[2]


def resolve_path(path: str | Path) -> Path:
    candidate = Path(path)
    if candidate.is_absolute():
        return candidate
    return PROJECT_ROOT / candidate


def load_yaml(path: str | Path) -> dict[str, Any]:
    with resolve_path(path).open("r", encoding="utf-8") as handle:
        return yaml.safe_load(handle)


def load_schema_mapping(path: str | Path = "configs/schema_mapping.yaml") -> dict[str, Any]:
    config = load_yaml(path)
    if "sources" not in config:
        raise ValueError("schema_mapping.yaml must define a top-level 'sources' key")
    if "canonical_columns" not in config:
        raise ValueError("schema_mapping.yaml must define canonical_columns")
    return config


def load_dq_rules(path: str | Path = "configs/dq_rules.yaml") -> dict[str, Any]:
    config = load_yaml(path)
    if "rules" not in config:
        raise ValueError("dq_rules.yaml must define a top-level 'rules' key")
    return config
