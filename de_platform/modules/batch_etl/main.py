"""Generic Batch ETL module.

Runs a configurable Extract → Transform* → Load pipeline using the DataStream
streaming engine. All three stages are provided as dotted class paths and
resolved through the DI container, so they can declare any registered
infrastructure dependency (db, fs, cache, logger, etc.).

Example (using the built-in FileSystem extractor and Database loader)::

    python -m de_platform run batch_etl \\
        --extractor de_platform.modules.batch_etl.extractors.filesystem.FileSystemExtractor \\
        --transformers de_platform.modules.batch_etl.transformers.event_normalizer.EventNormalizerTransformer \\
        --loader de_platform.modules.batch_etl.loaders.database.DatabaseLoader \\
        --extractor-params '{"path":"raw_events","date":"2025-01-01"}' \\
        --loader-params '{"table":"cleaned_events"}' \\
        --batch-size 500 \\
        --db warehouse=postgres \\
        --fs local

Processing method / worker overrides::

    --extractor-method concurrent --extractor-workers 4
    --transformer-method parallel --transformer-workers 4
    --loader-method inline
"""

from __future__ import annotations

import importlib
import json
from typing import Any

from de_platform.config.container import Container
from de_platform.config.context import ModuleConfig
from de_platform.modules.base import AsyncModule
from de_platform.services.logger.factory import LoggerFactory
from de_platform.services.logger.interface import LoggingInterface
from de_platform.shared.data_stream import DataStream
from de_platform.shared.etl.interfaces import Extractor, Loader, Transformer


def _import_class(dotted_path: str) -> type:
    """Import and return a class from a dotted module.ClassName path."""
    module_path, class_name = dotted_path.rsplit(".", 1)
    module = importlib.import_module(module_path)
    return getattr(module, class_name)


class BatchEtlModule(AsyncModule):
    log: LoggingInterface

    def __init__(
        self,
        config: ModuleConfig,
        logger: LoggerFactory,
        container: Container,
    ) -> None:
        self.config = config
        self.logger = logger
        self.container = container

    async def initialize(self) -> None:
        self.log = self.logger.create()

    async def teardown(self) -> None:
        pass

    async def execute(self) -> int:
        # ----------------------------------------------------------------
        # Parse configuration
        # ----------------------------------------------------------------
        extractor_path: str = self.config.get("extractor")
        loader_path: str = self.config.get("loader")
        transformers_str: str = self.config.get("transformers", "")
        transformer_paths = [p.strip() for p in transformers_str.split(",") if p.strip()]

        extractor_params: dict[str, Any] = json.loads(self.config.get("extractor-params", "{}"))
        transformer_params: dict[str, Any] = json.loads(
            self.config.get("transformer-params", "{}")
        )
        loader_params: dict[str, Any] = json.loads(self.config.get("loader-params", "{}"))

        batch_size: int = self.config.get("batch-size", 0)
        dry_run: bool = self.config.get("dry-run", False)

        # ----------------------------------------------------------------
        # Resolve ETL implementations via the DI container
        # ----------------------------------------------------------------
        extractor_cls = _import_class(extractor_path)
        extractor: Extractor = self.container.resolve(extractor_cls)
        # Inject params if the implementation exposes a params attribute
        if hasattr(extractor, "params"):
            extractor.params = extractor_params  # type: ignore[union-attr]

        transformers: list[Transformer] = []
        for path in transformer_paths:
            t_cls = _import_class(path)
            t: Transformer = self.container.resolve(t_cls)
            if hasattr(t, "params"):
                t.params = transformer_params  # type: ignore[union-attr]
            transformers.append(t)

        loader_cls = _import_class(loader_path)
        loader: Loader = self.container.resolve(loader_cls)
        if hasattr(loader, "params"):
            loader.params = loader_params  # type: ignore[union-attr]

        # ----------------------------------------------------------------
        # Determine processing methods / workers (CLI overrides > impl defaults)
        # ----------------------------------------------------------------
        ext_method = self.config.get("extractor-method", None) or extractor.processing_method
        ext_workers = int(self.config.get("extractor-workers", None) or extractor.workers)

        loader_method = self.config.get("loader-method", None) or loader.processing_method
        loader_workers = int(self.config.get("loader-workers", None) or loader.workers)

        t_method_override = self.config.get("transformer-method", None)
        t_workers_override = self.config.get("transformer-workers", None)

        self.log.info(
            "Starting Batch ETL",
            extractor=extractor_path,
            transformers=transformer_paths,
            loader=loader_path,
            batch_size=batch_size,
            dry_run=dry_run,
        )

        # ----------------------------------------------------------------
        # Build the DataStream pipeline
        # ----------------------------------------------------------------
        # The stream starts with the extractor params as a single item.
        # The extractor's extract() method is called as a generator, yielding
        # individual records from the source.
        stream = DataStream(extractor_params).map(
            extractor.extract,
            processing_method=ext_method,
            workers=ext_workers,
        )

        # Chain transformers. Each transformer.transform() is mapped over items.
        for transformer in transformers:
            t_method = t_method_override or transformer.processing_method
            t_workers = int(t_workers_override or transformer.workers)
            stream = stream.map(transformer.transform, processing_method=t_method, workers=t_workers)

        # Drop None values (transformers return None to signal "drop this item")
        stream = stream.filter(lambda item: item is not None)

        # Optional batching before the loader
        if batch_size > 0:
            stream = stream.batch(batch_size)

        # ----------------------------------------------------------------
        # Execute
        # ----------------------------------------------------------------
        if dry_run:
            self.log.info("Dry run: consuming stream without loading")
            items = await stream.collect()
            self.log.info("Dry run complete", items_processed=len(items))
            return 0

        await stream.for_each(
            loader.load,
            processing_method=loader_method,
            workers=loader_workers,
        )

        self.log.info("Batch ETL complete")
        return 0


module_class = BatchEtlModule
