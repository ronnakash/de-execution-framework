"""Loads .env files with KEY=VALUE format.

Supports:
- Comments (lines starting with #)
- Blank lines
- Quoted values (single or double quotes are stripped)
- Inline comments after values are NOT stripped (to keep values predictable)
"""

from pathlib import Path

# Project root: two levels up from de_platform/config/
_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent


def load_env_file(env_name: str = "local", project_root: Path | None = None) -> dict[str, str]:
    """Load .env/<env_name>.env and return as dict. Returns empty dict if file is missing."""
    root = project_root or _PROJECT_ROOT
    env_file = root / ".env" / f"{env_name}.env"
    if not env_file.exists():
        return {}
    return _parse_env_file(env_file)


def _parse_env_file(path: Path) -> dict[str, str]:
    result: dict[str, str] = {}
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip()
        value = value.strip()
        # Strip surrounding quotes
        if len(value) >= 2 and value[0] == value[-1] and value[0] in ('"', "'"):
            value = value[1:-1]
        result[key] = value
    return result
