"""Shared event validation logic used by all starter modules.

Validates raw event dicts against the expected schema for each event type.
Returns lists of valid events and ``ValidationError`` objects for invalid ones.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any


@dataclass
class ValidationError:
    field: str
    message: str
    event_index: int


def _check_non_empty_str(data: dict, field: str, index: int, errors: list[ValidationError]) -> None:
    val = data.get(field)
    if not val or not isinstance(val, str) or not val.strip():
        errors.append(ValidationError(field=field, message=f"{field} is required and must be a non-empty string", event_index=index))


def _check_positive_number(
    data: dict, field: str, index: int, errors: list[ValidationError]
) -> None:
    val = data.get(field)
    if val is None:
        errors.append(ValidationError(field=field, message=f"{field} is required", event_index=index))
        return
    try:
        if float(val) <= 0:
            errors.append(ValidationError(field=field, message=f"{field} must be a positive number", event_index=index))
    except (TypeError, ValueError):
        errors.append(ValidationError(field=field, message=f"{field} must be a number", event_index=index))


def _check_iso8601(data: dict, field: str, index: int, errors: list[ValidationError]) -> None:
    val = data.get(field)
    if not val:
        errors.append(ValidationError(field=field, message=f"{field} is required", event_index=index))
        return
    # Accept both "Z" suffix and "+00:00" offset
    normalized = str(val).replace("Z", "+00:00")
    try:
        datetime.fromisoformat(normalized)
    except ValueError:
        errors.append(ValidationError(field=field, message=f"{field} must be a valid ISO 8601 datetime", event_index=index))


def _check_currency(data: dict, field: str, index: int, errors: list[ValidationError]) -> None:
    val = data.get(field)
    if not val or not isinstance(val, str) or len(val) != 3 or not val.isupper():
        errors.append(ValidationError(field=field, message=f"{field} must be a 3-character uppercase ISO 4217 currency code", event_index=index))


def _check_side(data: dict, field: str, index: int, errors: list[ValidationError]) -> None:
    val = data.get(field)
    if val not in ("buy", "sell"):
        errors.append(ValidationError(field=field, message=f"{field} must be 'buy' or 'sell'", event_index=index))


def validate_order(data: dict[str, Any], index: int) -> list[ValidationError]:
    """Validate a single raw order dict. Returns a list of errors (empty = valid)."""
    errors: list[ValidationError] = []
    for f in ("id", "tenant_id", "status", "symbol", "order_type"):
        _check_non_empty_str(data, f, index, errors)
    _check_iso8601(data, "transact_time", index, errors)
    _check_positive_number(data, "quantity", index, errors)
    _check_positive_number(data, "price", index, errors)
    _check_currency(data, "currency", index, errors)
    _check_side(data, "side", index, errors)
    return errors


def validate_execution(data: dict[str, Any], index: int) -> list[ValidationError]:
    """Validate a single raw execution dict."""
    errors: list[ValidationError] = []
    for f in ("id", "tenant_id", "status", "order_id", "symbol", "execution_venue"):
        _check_non_empty_str(data, f, index, errors)
    _check_iso8601(data, "transact_time", index, errors)
    _check_positive_number(data, "quantity", index, errors)
    _check_positive_number(data, "price", index, errors)
    _check_currency(data, "currency", index, errors)
    _check_side(data, "side", index, errors)
    return errors


def validate_transaction(data: dict[str, Any], index: int) -> list[ValidationError]:
    """Validate a single raw transaction dict."""
    errors: list[ValidationError] = []
    for f in ("id", "tenant_id", "status", "account_id", "counterparty_id", "transaction_type"):
        _check_non_empty_str(data, f, index, errors)
    _check_iso8601(data, "transact_time", index, errors)
    _check_positive_number(data, "amount", index, errors)
    _check_currency(data, "currency", index, errors)
    return errors


_VALIDATORS = {
    "order": validate_order,
    "execution": validate_execution,
    "transaction": validate_transaction,
}


def validate_events(
    event_type: str,
    events: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[ValidationError]]:
    """Validate a batch of events of a given type.

    Args:
        event_type: One of ``"order"``, ``"execution"``, ``"transaction"``.
        events: List of raw event dicts.

    Returns:
        ``(valid_events, all_errors)`` where ``all_errors`` may contain errors
        from multiple events (identified by ``event_index``).
    """
    validator = _VALIDATORS.get(event_type)
    if validator is None:
        raise ValueError(f"Unknown event_type: {event_type!r}. Expected one of {list(_VALIDATORS)}")

    valid: list[dict[str, Any]] = []
    all_errors: list[ValidationError] = []

    for i, event in enumerate(events):
        errs = validator(event, i)
        if errs:
            all_errors.extend(errs)
        else:
            valid.append(event)

    return valid, all_errors
