from src.utils.config import load_schema_mapping


def test_schema_mapping_defines_required_sources():
    config = load_schema_mapping()

    assert set(config["sources"]) == {
        "conference_csv",
        "government_vendor",
        "erp_export",
        "supplier_activity_api",
        "kaggle_procurement_kpi",
    }


def test_each_source_maps_required_source_fields():
    config = load_schema_mapping()

    for source_name, source_config in config["sources"].items():
        mapping = source_config["column_mapping"]
        assert mapping["supplier_name"], f"{source_name} must map supplier_name"
        if source_config.get("record_type") in {"supplier_profile", "supplier_activity"}:
            assert mapping["email"], f"{source_name} must map email"
            assert mapping["phone"], f"{source_name} must map phone"
        if source_config.get("record_type") == "procurement_transaction":
            assert mapping["po_id"], f"{source_name} must map po_id"
            assert mapping["quantity"], f"{source_name} must map quantity"
            assert mapping["negotiated_price"], f"{source_name} must map negotiated_price"


def test_canonical_columns_include_gold_ready_fields():
    config = load_schema_mapping()

    for column in ["supplier_name", "email", "phone", "category", "annual_spend", "activity_count", "po_id"]:
        assert column in config["canonical_columns"]
