import logging
from typing import List, Dict, Any, Tuple

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


# -------------------------------
# 🔹 Record-Level Validation
# -------------------------------
def validate_record(record: Dict[str, Any]) -> Tuple[bool, List[str]]:
    """
    Validate a single record.
    Returns (is_valid, list_of_errors)
    """

    errors = []

    try:
        ticker = record.get("ticker")
        timestamp = record.get("timestamp")
        open_ = record.get("open")
        high = record.get("high")
        low = record.get("low")
        close = record.get("close")
        volume = record.get("volume")

        # --- Structural checks ---
        if not ticker:
            errors.append("Missing ticker")

        if not timestamp:
            errors.append("Missing timestamp")

        if any(v is None for v in [open_, high, low, close, volume]):
            errors.append("Missing price/volume fields")

        # --- Financial logic checks ---
        if open_ <= 0 or high <= 0 or low <= 0 or close <= 0:
            errors.append("Non-positive price detected")

        if volume < 0:
            errors.append("Negative volume")

        if high < low:
            errors.append("High < Low inconsistency")

        if not (low <= open_ <= high):
            errors.append("Open not between Low and High")

        if not (low <= close <= high):
            errors.append("Close not between Low and High")

        # --- Basic anomaly guard (tunable) ---
        if open_ > 0:
            change_ratio = abs(close - open_) / open_
            if change_ratio > 0.5:
                errors.append("Extreme price change (>50%)")

    except Exception as e:
        errors.append(f"Unexpected validation error: {e}")

    return (len(errors) == 0, errors)


# -------------------------------
# 🔹 Batch-Level Validation
# -------------------------------
def validate_batch(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Validate a batch of records:
    - applies record-level validation
    - removes duplicates
    - attaches validation metadata
    """

    valid_records = []
    seen_keys = set()

    for record in records:
        is_valid, errors = validate_record(record)

        # Add metadata (pro move)
        record["is_valid"] = is_valid
        record["validation_errors"] = errors

        key = (record.get("ticker"), record.get("timestamp"))

        # --- Duplicate check ---
        if key in seen_keys:
            record["is_valid"] = False
            record["validation_errors"].append("Duplicate record")
            logger.warning(f"Duplicate found: {key}")
            continue

        seen_keys.add(key)

        if is_valid:
            valid_records.append(record)
        else:
            logger.warning(f"Invalid record: {errors}")

    logger.info(f"Validated {len(records)} records → {len(valid_records)} valid")

    return valid_records


# -------------------------------
# 🔹 Optional: Separate Invalid Records
# -------------------------------
def split_valid_invalid(records: List[Dict[str, Any]]) -> Tuple[List[Dict], List[Dict]]:
    """
    Split records into valid and invalid groups.
    Useful for logging or storing bad data separately.
    """

    valid = []
    invalid = []

    for r in records:
        if r.get("is_valid"):
            valid.append(r)
        else:
            invalid.append(r)

    return valid, invalid