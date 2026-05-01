from __future__ import annotations

import re
from typing import Optional


EMAIL_PATTERN = r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"
TAX_ID_PATTERN = r"^\d{2}-\d{7}$"

LEGAL_SUFFIXES = {
    "INC",
    "INCORPORATED",
    "LLC",
    "L L C",
    "LTD",
    "LIMITED",
    "CORP",
    "CORPORATION",
    "CO",
    "COMPANY",
}


def normalize_supplier_name_text(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    cleaned = re.sub(r"[^A-Za-z0-9 ]+", " ", value.upper())
    cleaned = re.sub(r"\bTECHNOLOGIES\b", "TECH", cleaned)
    cleaned = re.sub(r"\bTECHNOLOGY\b", "TECH", cleaned)
    tokens = [token for token in cleaned.split() if token not in LEGAL_SUFFIXES]
    normalized = " ".join(tokens)
    return normalized or None


def match_key_text(value: Optional[str]) -> Optional[str]:
    normalized = normalize_supplier_name_text(value)
    if normalized is None:
        return None
    return re.sub(r"\s+", "", normalized)


def is_valid_email_text(value: Optional[str]) -> bool:
    return bool(value and re.match(EMAIL_PATTERN, value.strip()))


def clean_phone_text(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    digits = re.sub(r"\D", "", value)
    if len(digits) == 11 and digits.startswith("1"):
        digits = digits[1:]
    return digits or None


def is_valid_phone_text(value: Optional[str]) -> bool:
    digits = clean_phone_text(value)
    return bool(digits and len(digits) == 10)


def is_valid_tax_id_text(value: Optional[str]) -> bool:
    return bool(value and re.match(TAX_ID_PATTERN, value.strip()))
