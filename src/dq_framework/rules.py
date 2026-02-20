from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import yaml


@dataclass(frozen=True)
class Rule:
    tag: str
    expected_type: str = "double"
    min_value: Optional[float] = None
    max_value: Optional[float] = None
    expected_frequency_sec: Optional[int] = None
    max_lateness_sec: Optional[int] = None
    severity: str = "warn"
    enabled: bool = True


def load_rules_yaml(path: str) -> List[Rule]:
    """Load rules from conf/rules.yml into strongly-typed Rule objects."""
    with open(path, "r", encoding="utf-8") as f:
        doc: Dict[str, Any] = yaml.safe_load(f)

    defaults = doc.get("defaults", {}) or {}
    rules_raw = doc.get("rules", []) or []

    rules: List[Rule] = []
    for r in rules_raw:
        tag = r["tag"]
        rules.append(
            Rule(
                tag=tag,
                expected_type=r.get("expected_type", defaults.get("expected_type", "double")),
                min_value=r.get("min_value"),
                max_value=r.get("max_value"),
                expected_frequency_sec=r.get(
                    "expected_frequency_sec", defaults.get("expected_frequency_sec")
                ),
                max_lateness_sec=r.get("max_lateness_sec", defaults.get("max_lateness_sec")),
                severity=r.get("severity", defaults.get("severity", "warn")),
                enabled=r.get("enabled", True),
            )
        )
    return rules


def rules_to_dicts(rules: List[Rule]) -> List[Dict[str, Any]]:
    """Convert Rule objects into dict rows suitable for Spark DataFrame creation."""
    out: List[Dict[str, Any]] = []
    for i, r in enumerate(rules, start=1):
        out.append(
            {
                "rule_id": f"rule_{i:03d}_{r.tag}",
                "tag": r.tag,
                "expected_type": r.expected_type,
                "min_value": r.min_value,
                "max_value": r.max_value,
                "expected_frequency_sec": r.expected_frequency_sec,
                "max_lateness_sec": r.max_lateness_sec,
                "enabled": r.enabled,
                "severity": r.severity,
            }
        )
    return out
