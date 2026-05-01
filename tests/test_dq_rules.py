from src.data_quality.validators import (
    clean_phone_text,
    is_valid_email_text,
    is_valid_phone_text,
    is_valid_tax_id_text,
    match_key_text,
    normalize_supplier_name_text,
)
from src.utils.config import load_dq_rules


def test_dq_config_has_required_metric_fields():
    config = load_dq_rules()
    rule_names = {rule["rule_name"] for rule in config["rules"]}

    assert "supplier_name_not_null" in rule_names
    assert "email_format_valid" in rule_names
    assert "phone_length_valid" in rule_names
    assert "tax_id_format_valid" in rule_names


def test_validation_helpers_cover_email_phone_and_tax_id():
    assert is_valid_email_text("ops@example.com")
    assert not is_valid_email_text("ops-at-example")

    assert clean_phone_text("+1 (212) 555-0199") == "2125550199"
    assert is_valid_phone_text("+1 (212) 555-0199")
    assert not is_valid_phone_text("555")

    assert is_valid_tax_id_text("12-3456789")
    assert not is_valid_tax_id_text("BAD")


def test_supplier_name_normalization_supports_fuzzy_matching():
    assert normalize_supplier_name_text("ABC Technologies LLC") == "ABC TECH"
    assert normalize_supplier_name_text("ABC Tech LLC") == "ABC TECH"
    assert match_key_text("ABC Technologies LLC") == match_key_text("ABC Tech LLC")
