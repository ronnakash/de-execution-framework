"""Custom argument generators for scheduled tasks.

Each generator function receives a task definition dict and the last run dict
(or None if no prior run), and returns a dict of args to pass to the module.
The returned dict keys are CLI arg names (without ``--``), values are strings.

Generators are referenced by dotted import path in the ``arg_generator``
column of ``task_definitions``.
"""

from __future__ import annotations

from datetime import datetime, timedelta


def batch_algos_args(task_def: dict, last_run: dict | None) -> dict:
    """Generate args for the next batch algo run.

    Increments the date range from where the last run left off.
    Each run covers a 1-day window.
    """
    if last_run and last_run.get("args"):
        args = last_run["args"]
        if isinstance(args, str):
            import json
            args = json.loads(args)
        last_end = args.get("end-date") or args.get("end_date", "")
        if last_end:
            start = (datetime.fromisoformat(last_end) + timedelta(days=1)).strftime("%Y-%m-%d")
        else:
            start = "2026-01-01"
    else:
        start = "2026-01-01"

    end = start  # 1-day window

    default_args = task_def.get("default_args", {})
    if isinstance(default_args, str):
        import json
        default_args = json.loads(default_args)

    tenant_id = default_args.get("tenant_id", "")

    return {
        "tenant-id": tenant_id,
        "start-date": start,
        "end-date": end,
    }
