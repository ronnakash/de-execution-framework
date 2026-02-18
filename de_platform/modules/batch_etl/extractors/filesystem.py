"""FileSystem extractor: reads JSONL files from the file system."""

from __future__ import annotations

import json
from collections.abc import Iterator
from typing import Any

from de_platform.services.filesystem.interface import FileSystemInterface
from de_platform.services.logger.factory import LoggerFactory
from de_platform.shared.etl.interfaces import Extractor


class FileSystemExtractor(Extractor):
    """Reads JSONL data files from the configured filesystem.

    Expected params::

        {
            "path": "raw_events",   # prefix / directory
            "date": "2025-01-01"    # appended to path: raw_events/2025-01-01/
        }

    Each line of every file under ``{path}/{date}/`` is parsed as JSON and
    yielded as a dict.
    """

    processing_method = "inline"
    workers = 1

    def __init__(self, fs: FileSystemInterface, logger: LoggerFactory) -> None:
        self.fs = fs
        self.log = logger.create()

    def extract(self, params: dict[str, Any]) -> Iterator[Any]:
        path = params.get("path", "")
        date = params.get("date", "")
        prefix = f"{path}/{date}" if date else path

        files = self.fs.list(prefix)
        self.log.info("Reading raw data", files=len(files), prefix=prefix)

        for file_path in files:
            data = self.fs.read(file_path)
            for line in data.decode("utf-8").splitlines():
                line = line.strip()
                if not line:
                    continue
                try:
                    yield json.loads(line)
                except json.JSONDecodeError:
                    self.log.error("Malformed JSON line", file=file_path, line=line[:100])
