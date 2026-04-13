"""
Data validation layer for ETL-SYNC pipeline.
Validates CDC records before writing to MongoDB, catching data quality
issues early and routing bad records to the Dead Letter Queue.
"""

import logging

logger = logging.getLogger("etl_validators")


# ── Required fields per table ──────────────────────────────────
REQUIRED_FIELDS = {
    "products": ["id", "name", "category", "price"],
    "inventory": ["id", "product_id", "quantity"],
    "orders": ["id", "customer_name", "customer_email", "status", "total_amount"],
    "order_items": ["id", "order_id", "product_id", "quantity", "unit_price"],
}

# ── Numeric fields (must be >= 0) ──────────────────────────────
NON_NEGATIVE_FIELDS = {
    "products":    ["price"],
    "inventory":   ["quantity", "reserved"],
    "orders":      ["total_amount"],
    "order_items": ["quantity", "unit_price"],
}

# ── Allowed enum values ───────────────────────────────────────
ENUM_FIELDS = {
    "products": {
        "status": ["active", "inactive", "discontinued"],
    },
    "orders": {
        "status": ["pending", "confirmed", "processing", "shipped", "delivered", "cancelled"],
    },
}


class ValidationResult:
    """Result of a validation check."""

    def __init__(self, is_valid: bool, errors: list = None):
        self.is_valid = is_valid
        self.errors = errors or []

    def __bool__(self):
        return self.is_valid

    def __repr__(self):
        if self.is_valid:
            return "ValidationResult(OK)"
        return f"ValidationResult(FAILED: {self.errors})"


def validate_record(table: str, record: dict) -> ValidationResult:
    """
    Validate a single record for the given table.

    Returns ValidationResult with is_valid=True if the record passes
    all checks, or is_valid=False with a list of error descriptions.
    """
    errors = []

    if not record:
        return ValidationResult(False, ["empty record"])

    # 1. Required fields check
    required = REQUIRED_FIELDS.get(table, [])
    for field in required:
        if field not in record or record[field] is None:
            errors.append(f"missing required field: {field}")

    # 2. Non-negative numeric check
    non_neg = NON_NEGATIVE_FIELDS.get(table, [])
    for field in non_neg:
        val = record.get(field)
        if val is not None:
            try:
                if float(val) < 0:
                    errors.append(f"{field} must be >= 0, got {val}")
            except (TypeError, ValueError):
                errors.append(f"{field} is not numeric: {val}")

    # 3. Enum check
    enums = ENUM_FIELDS.get(table, {})
    for field, allowed in enums.items():
        val = record.get(field)
        if val is not None and val not in allowed:
            errors.append(f"{field} invalid value '{val}', allowed: {allowed}")

    # 4. Email format (basic check for orders)
    if table == "orders":
        email = record.get("customer_email")
        if email and ("@" not in email or "." not in email):
            errors.append(f"invalid email format: {email}")

    if errors:
        logger.warning("Validation failed for %s record: %s", table, errors)
        return ValidationResult(False, errors)

    return ValidationResult(True)


def validate_batch(table: str, records: list) -> tuple:
    """
    Validate a batch of records, returning (valid_records, invalid_records).
    Invalid records include the validation errors for DLQ reporting.
    """
    valid = []
    invalid = []

    for record in records:
        result = validate_record(table, record)
        if result:
            valid.append(record)
        else:
            invalid.append({
                "record": record,
                "errors": result.errors,
                "table": table,
            })

    if invalid:
        logger.warning(
            "Batch validation for %s: %d valid, %d invalid",
            table, len(valid), len(invalid)
        )

    return valid, invalid
