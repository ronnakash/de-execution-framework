"""Abstract interfaces for the Extract-Transform-Load pipeline pattern.

Each step in the ETL process is represented by an abstract base class.
Concrete implementations declare infrastructure dependencies via constructor
type hints and are resolved by the DI container.

Processing hints
----------------
Each interface declares ``processing_method`` and ``workers`` class attributes
that serve as *defaults* for how the DataStream should invoke the step.
The batch_etl module allows these to be overridden via CLI flags.

- ``processing_method = "inline"``     — sequential, single-threaded.
- ``processing_method = "concurrent"`` — async I/O / thread pool.
- ``processing_method = "parallel"``   — multiprocessing (CPU-bound).
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Iterator
from typing import Any


class Extractor(ABC):
    """Extracts a stream of items from a source given a dict of parameters.

    Implement :meth:`extract` as a generator that yields individual records.
    The method is called once with the params dict supplied to the pipeline.

    Example::

        class ApiExtractor(Extractor):
            processing_method = "concurrent"
            workers = 5

            def extract(self, params: dict) -> Iterator[Any]:
                for page in range(params["pages"]):
                    yield from call_api(page)
    """

    processing_method: str = "inline"
    workers: int = 1

    @abstractmethod
    def extract(self, params: dict[str, Any]) -> Iterator[Any]:
        """Yield raw items from the source.

        Args:
            params: Arbitrary parameters driving the extraction
                    (e.g. date range, source path, API endpoint).
        """
        ...


class Transformer(ABC):
    """Transforms a single item from one form to another.

    Transformers are chained in order. Each transformer receives the output
    of the previous step. A transformer may also ``yield`` multiple items
    (acting as a flat-map / explode step).

    Example::

        class NormalizeTransformer(Transformer):
            processing_method = "parallel"
            workers = 4

            def transform(self, item: Any) -> Any:
                return {k: v.strip() for k, v in item.items()}
    """

    processing_method: str = "inline"
    workers: int = 1

    @abstractmethod
    def transform(self, item: Any) -> Any:
        """Transform *item* and return the result.

        May also be a generator function that yields multiple items.
        """
        ...


class Loader(ABC):
    """Loads items (or batches) to a destination.

    When used after a ``.batch(N)`` stage the loader receives lists; otherwise
    it receives individual items.  Document which mode your implementation expects.

    Example::

        class DatabaseLoader(Loader):
            def __init__(self, db: DatabaseInterface) -> None:
                self.db = db

            def load(self, batch: list[dict]) -> None:
                self.db.bulk_insert("events", batch)
    """

    processing_method: str = "inline"
    workers: int = 1

    @abstractmethod
    def load(self, item: Any) -> None:
        """Load *item* (or a batch list) to the destination."""
        ...
