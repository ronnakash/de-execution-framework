"""Lazy streaming pipeline with configurable processing strategies.

Usage example::

    results = await (
        DataStream(list_of_api_params)
        .map(fetch_from_api, processing_method="concurrent", workers=5)
        .map(normalize, processing_method="parallel", workers=4)
        .map(clean)
        .batch(10_000)
        .for_each(persist_batch)
    )

Processing methods:
- ``"inline"``     — sequential, no overhead. workers must be 1.
- ``"concurrent"`` — async I/O via asyncio tasks (good for network/disk ops).
                     Sync functions are run in a thread pool.
- ``"parallel"``   — true CPU parallelism via ProcessPoolExecutor.
                     Functions must be picklable (module-level, no closures/lambdas).
"""

from __future__ import annotations

import asyncio
import inspect
from collections.abc import AsyncIterable, AsyncIterator, Callable, Iterable
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass, field
from typing import Any

INLINE = "inline"
CONCURRENT = "concurrent"
PARALLEL = "parallel"


@dataclass
class FilterFailure:
    """Passed to filter failure handlers with the rejected item and its cause."""

    item: Any
    reason: Exception | None = field(default=None)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


async def _to_async_iter(source: Any) -> AsyncIterator[Any]:
    """Normalize any source value into an async iterator.

    - AsyncIterable  → iterated as-is.
    - Iterable (not str/bytes/dict) → each element is yielded.
    - Anything else (including dict, str, bytes, single object) → yielded as one item.
    """
    if hasattr(source, "__aiter__"):
        async for item in source:  # type: ignore[union-attr]
            yield item
    elif isinstance(source, Iterable) and not isinstance(source, (str, bytes, dict)):
        for item in source:
            yield item
    else:
        yield source


async def _call_streaming(func: Callable, item: Any) -> AsyncIterator[Any]:
    """Call ``func(item)`` and yield every result it produces.

    Handles:
    - Async generators          → yielded directly.
    - Coroutines                → awaited; result unwrapped if iterable.
    - Sync generators           → iterated synchronously.
    - Regular sync functions    → called; result unwrapped if iterable.
    """
    if inspect.isasyncgenfunction(func):
        async for r in func(item):
            yield r
    elif asyncio.iscoroutinefunction(func):
        result = await func(item)
        if isinstance(result, AsyncIterable) and not isinstance(result, (str, bytes)):
            async for r in result:  # type: ignore[union-attr]
                yield r
        elif isinstance(result, Iterable) and not isinstance(result, (str, bytes, dict)):
            for r in result:
                yield r
        else:
            yield result
    elif inspect.isgeneratorfunction(func):
        for r in func(item):
            yield r
    else:
        result = func(item)
        if isinstance(result, AsyncIterable) and not isinstance(result, (str, bytes)):
            async for r in result:  # type: ignore[union-attr]
                yield r
        elif isinstance(result, Iterable) and not isinstance(result, (str, bytes, dict)):
            for r in result:
                yield r
        else:
            yield result


def _collect_sync(func: Callable, item: Any) -> list[Any]:
    """Run a sync function in a thread and collect its results as a list.

    Used by the concurrent stage to avoid blocking the event loop.
    """
    if inspect.isgeneratorfunction(func):
        return list(func(item))
    result = func(item)
    if isinstance(result, Iterable) and not isinstance(result, (str, bytes, dict)):
        return list(result)
    return [result]


# ---------------------------------------------------------------------------
# Pipeline stages
# ---------------------------------------------------------------------------


class _Stage:
    def process(self, source: AsyncIterator[Any]) -> AsyncIterator[Any]:
        raise NotImplementedError


class _MapStage(_Stage):
    def __init__(self, func: Callable, processing_method: str, workers: int) -> None:
        self.func = func
        self.processing_method = processing_method
        self.workers = workers

    def process(self, source: AsyncIterator[Any]) -> AsyncIterator[Any]:
        if self.processing_method == INLINE:
            return self._inline(source)
        if self.processing_method == CONCURRENT:
            return self._concurrent(source)
        if self.processing_method == PARALLEL:
            return self._parallel(source)
        raise ValueError(f"Unknown processing_method: {self.processing_method!r}")

    async def _inline(self, source: AsyncIterator[Any]) -> AsyncIterator[Any]:
        async for item in source:
            async for result in _call_streaming(self.func, item):
                yield result

    async def _concurrent(self, source: AsyncIterator[Any]) -> AsyncIterator[Any]:
        """Process with bounded asyncio concurrency.

        Sync functions run in a thread pool (``asyncio.to_thread``).
        Async functions/generators run as asyncio tasks with a semaphore.
        Results are yielded as they arrive (unordered within a window).
        """
        result_q: asyncio.Queue[Any] = asyncio.Queue()
        sentinel = object()
        active = 0
        error_holder: list[BaseException] = []
        sem = asyncio.Semaphore(self.workers)
        is_async = asyncio.iscoroutinefunction(self.func) or inspect.isasyncgenfunction(
            self.func
        )

        async def worker(item: Any) -> None:
            nonlocal active
            try:
                if is_async:
                    async for r in _call_streaming(self.func, item):
                        await result_q.put(r)
                else:
                    results = await asyncio.to_thread(_collect_sync, self.func, item)
                    for r in results:
                        await result_q.put(r)
            except Exception as exc:
                error_holder.append(exc)
            finally:
                active -= 1
                if active == 0:
                    await result_q.put(sentinel)

        async def bounded_worker(item: Any) -> None:
            async with sem:
                await worker(item)

        async for item in source:
            active += 1
            asyncio.create_task(bounded_worker(item))

        if active == 0:
            return

        while True:
            val = await result_q.get()
            if val is sentinel:
                break
            yield val

        if error_holder:
            raise error_holder[0]

    async def _parallel(self, source: AsyncIterator[Any]) -> AsyncIterator[Any]:
        """Process with CPU parallelism via ProcessPoolExecutor.

        The function must be picklable (module-level, no closures or lambdas).
        Generator functions are not supported in parallel mode.
        """
        loop = asyncio.get_running_loop()
        result_q: asyncio.Queue[Any] = asyncio.Queue()
        sentinel = object()
        active = 0
        error_holder: list[BaseException] = []
        sem = asyncio.Semaphore(self.workers)
        executor = ProcessPoolExecutor(max_workers=self.workers)

        async def worker(item: Any) -> None:
            nonlocal active
            try:
                async with sem:
                    result = await loop.run_in_executor(executor, self.func, item)
                if isinstance(result, Iterable) and not isinstance(result, (str, bytes, dict)):
                    for r in result:
                        await result_q.put(r)
                else:
                    await result_q.put(result)
            except Exception as exc:
                error_holder.append(exc)
            finally:
                active -= 1
                if active == 0:
                    await result_q.put(sentinel)

        async for item in source:
            active += 1
            asyncio.create_task(worker(item))

        if active == 0:
            executor.shutdown(wait=False)
            return

        while True:
            val = await result_q.get()
            if val is sentinel:
                break
            yield val

        executor.shutdown(wait=False)
        if error_holder:
            raise error_holder[0]


class _BatchStage(_Stage):
    def __init__(self, size: int) -> None:
        self.size = size

    async def process(self, source: AsyncIterator[Any]) -> AsyncIterator[Any]:
        batch: list[Any] = []
        async for item in source:
            batch.append(item)
            if len(batch) >= self.size:
                yield list(batch)
                batch.clear()
        if batch:
            yield list(batch)


class _FilterStage(_Stage):
    def __init__(
        self,
        predicate: Callable[[Any], bool],
        failure_handler: Callable[[Any, FilterFailure], None] | None,
    ) -> None:
        self.predicate = predicate
        self.failure_handler = failure_handler

    async def process(self, source: AsyncIterator[Any]) -> AsyncIterator[Any]:
        async for item in source:
            try:
                passes = self.predicate(item)
            except Exception as exc:
                if self.failure_handler:
                    self.failure_handler(item, FilterFailure(item=item, reason=exc))
                continue

            if passes:
                yield item
            elif self.failure_handler:
                self.failure_handler(item, FilterFailure(item=item, reason=None))


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


class DataStream:
    """Declarative lazy streaming pipeline with configurable processing strategies.

    Build a pipeline with :meth:`map`, :meth:`batch`, and :meth:`filter`, then
    execute it with :meth:`for_each` (side-effects only) or :meth:`collect`
    (returns a list).

    Example::

        # 100 API calls → normalize in parallel → batch insert 10k at a time
        await (
            DataStream(range(100))
            .map(fetch_page, processing_method="concurrent", workers=8)
            .map(normalize, processing_method="parallel", workers=4)
            .filter(is_valid, failure_handler=log_invalid)
            .batch(10_000)
            .for_each(db.bulk_insert)
        )
    """

    def __init__(self, source: Any) -> None:
        """Initialize the stream.

        Args:
            source: An iterable, async iterable, or a single value.
                    Strings, bytes, and dicts are treated as single items
                    (not iterated character-by-character / key-by-key).
        """
        self._source = source
        self._stages: list[_Stage] = []

    # ------------------------------------------------------------------
    # Intermediate operations (return self for chaining)
    # ------------------------------------------------------------------

    def map(
        self,
        func: Callable,
        processing_method: str = INLINE,
        workers: int = 1,
    ) -> "DataStream":
        """Apply ``func`` to each item in the stream.

        ``func`` can return a single value **or** yield/return multiple values
        (flat-map behaviour). Generator functions, async generators, coroutines,
        and regular functions are all supported.

        Args:
            func: Transform function. Signature: ``func(item) -> Any``.
            processing_method: One of ``"inline"`` (default), ``"concurrent"``,
                               or ``"parallel"``.
            workers: Max concurrency (ignored for ``"inline"``).
        """
        if processing_method == INLINE and workers != 1:
            raise ValueError("inline processing_method only supports workers=1")
        self._stages.append(_MapStage(func, processing_method, workers))
        return self

    def batch(self, size: int) -> "DataStream":
        """Group items into lists of ``size`` (last batch may be smaller).

        After ``batch(N)``, downstream ``map`` / ``for_each`` receive ``list``
        objects instead of individual items.
        """
        if size < 1:
            raise ValueError(f"Batch size must be >= 1, got {size}")
        self._stages.append(_BatchStage(size))
        return self

    def filter(
        self,
        predicate: Callable[[Any], bool],
        failure_handler: Callable[[Any, FilterFailure], None] | None = None,
    ) -> "DataStream":
        """Keep only items where ``predicate(item)`` returns ``True``.

        Items that fail the predicate (or raise an exception) are passed to
        ``failure_handler(item, FilterFailure)`` when provided.
        """
        self._stages.append(_FilterStage(predicate, failure_handler))
        return self

    # ------------------------------------------------------------------
    # Terminal operations (consume the stream)
    # ------------------------------------------------------------------

    async def for_each(
        self,
        func: Callable,
        processing_method: str = INLINE,
        workers: int = 1,
    ) -> None:
        """Terminal: apply ``func`` to every item, discarding the return value.

        Supports the same processing methods and workers as :meth:`map`.
        """
        if processing_method == INLINE and workers != 1:
            raise ValueError("inline processing_method only supports workers=1")
        stage = _MapStage(func, processing_method, workers)
        async for _ in stage.process(self._build_pipeline()):
            pass

    async def collect(
        self,
        collector: Callable[[Any], None] | None = None,
    ) -> list[Any]:
        """Terminal: consume the stream and return all items as a list.

        If ``collector`` is provided it is called for each item as a side-effect
        (useful for counting, logging, etc.) — the list is still returned.
        """
        items: list[Any] = []
        async for item in self._build_pipeline():
            if collector:
                collector(item)
            items.append(item)
        return items

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _build_pipeline(self) -> AsyncIterator[Any]:
        stream: AsyncIterator[Any] = _to_async_iter(self._source)
        for stage in self._stages:
            stream = stage.process(stream)
        return stream


# ---------------------------------------------------------------------------
# Utility helpers
# ---------------------------------------------------------------------------


def bulk_accumulator(
    handler: Callable[[list[Any]], None],
    size: int,
) -> Any:
    """Return a single-item callable that batches calls to a bulk handler.

    Accumulates items until ``size`` is reached, then calls ``handler(batch)``.
    Call ``.flush()`` on the returned callable to process any remaining items.

    Example::

        save = bulk_accumulator(db.bulk_insert, size=500)
        stream.for_each(save)
        save.flush()  # flush trailing items
    """
    batch: list[Any] = []

    def add(item: Any) -> None:
        batch.append(item)
        if len(batch) >= size:
            handler(list(batch))
            batch.clear()

    def flush() -> None:
        if batch:
            handler(list(batch))
            batch.clear()

    add.flush = flush  # type: ignore[attr-defined]
    return add


def bulk_failure_handler(
    handler: Callable[[list[tuple[Any, FilterFailure]]], None],
    size: int,
) -> Any:
    """Return a filter failure handler that batches failures before processing.

    Accumulates ``(item, FilterFailure)`` pairs and calls ``handler(batch)``
    when ``size`` is reached. Call ``.flush()`` to process remaining failures.

    Example::

        on_fail = bulk_failure_handler(log_invalid_batch, size=100)
        stream.filter(is_valid, failure_handler=on_fail)
        await stream.for_each(process)
        on_fail.flush()
    """
    batch: list[tuple[Any, FilterFailure]] = []

    def add(item: Any, failure: FilterFailure) -> None:
        batch.append((item, failure))
        if len(batch) >= size:
            handler(list(batch))
            batch.clear()

    def flush() -> None:
        if batch:
            handler(list(batch))
            batch.clear()

    add.flush = flush  # type: ignore[attr-defined]
    return add
