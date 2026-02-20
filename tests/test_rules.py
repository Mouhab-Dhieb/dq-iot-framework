from dq_framework.rules import load_rules_yaml, rules_to_dicts


def test_load_rules_yaml():
    rules = load_rules_yaml("conf/rules.yml")
    assert len(rules) >= 1
    assert rules[0].tag is not None

    rows = rules_to_dicts(rules)
    assert "rule_id" in rows[0]
    assert "tag" in rows[0]