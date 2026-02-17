"""Batch ETL validation logic. Pure functions with no infrastructure dependencies."""

from __future__ import annotations

from dataclasses import dataclass, field

from de_platform.services.logger.interface import LoggingInterface


@dataclass
class ValidationResult:
    valid: list[dict] = field(default_factory=list)
    invalid: list[dict] = field(default_factory=list)
    error_counts: dict[str, int] = field(default_factory=dict)


def validate_single_event(event: dict) -> tuple[bool, str | None]:
    """Validate a single event. Returns (is_valid, error_reason)."""
    if not isinstance(event.get("event_type"), str) or not event["event_type"].strip():
        return False, "missing_or_empty_event_type"
    if not isinstance(event.get("payload"), dict):
        return False, "payload_not_dict"
    if not isinstance(event.get("source"), str) or not event["source"].strip():
        return False, "missing_or_empty_source"
    return True, None


def validate_events(events: list[dict], log: LoggingInterface) -> tuple[list[dict], int]:
    """Validate raw events. Returns (valid_events, invalid_count)."""
    result = ValidationResult()

    for event in events:
        is_valid, reason = validate_single_event(event)
        if is_valid:
            result.valid.append(event)
        else:
            result.invalid.append(event)
            assert reason is not None
            result.error_counts[reason] = result.error_counts.get(reason, 0) + 1

    if result.invalid:
        log.warn(
            "Validation complete",
            valid=len(result.valid),
            invalid=len(result.invalid),
            errors=result.error_counts,
        )
    else:
        log.info("Validation complete", valid=len(result.valid), invalid=0)

    return result.valid, len(result.invalid)
