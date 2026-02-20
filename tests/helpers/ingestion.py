"""Shared ingestion helpers for E2E tests.

Provides ingestion functions for each combination of method (REST, Kafka, Files)
and infrastructure mode (in-memory vs real).
"""

from __future__ import annotations

import json
import uuid
from typing import Any

import aiohttp
from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer

from de_platform.config.context import ModuleConfig
from de_platform.modules.file_processor.main import FileProcessorModule
from de_platform.modules.kafka_starter.main import KafkaStarterModule
from de_platform.modules.rest_starter.main import dto_to_message_from_raw
from de_platform.pipeline.serialization import error_to_dict
from de_platform.pipeline.topics import NORMALIZATION_ERRORS
from de_platform.pipeline.validation import validate_events
from de_platform.services.lifecycle.lifecycle_manager import LifecycleManager
from de_platform.services.logger.factory import LoggerFactory

from tests.helpers.events import NORM_TOPIC, REST_ENDPOINT, CLIENT_TOPIC


# ── REST ingestion ───────────────────────────────────────────────────────────


def build_rest_app(mq: Any) -> web.Application:
    """Build the aiohttp ingestion app for REST-based tests (no full module lifecycle)."""

    async def _handle(request: web.Request, event_type: str) -> web.Response:
        body = await request.json()
        events = body.get("events", [])
        valid, errors = validate_events(event_type, events)
        topic = NORM_TOPIC[event_type]
        for raw in valid:
            mq.publish(topic, dto_to_message_from_raw(raw, event_type))
        for err in errors:
            raw_event = events[err.event_index] if err.event_index < len(events) else {}
            mq.publish(NORMALIZATION_ERRORS, error_to_dict(raw_event, event_type, [err]))
        rejected = {e.event_index for e in errors}
        return web.json_response({"accepted": len(valid), "rejected": len(rejected)})

    app = web.Application()
    for et, path in REST_ENDPOINT.items():
        app.router.add_post(f"/api/v1/{path}", lambda r, et=et: _handle(r, et))
    return app


async def ingest_rest_inline(mq: Any, event_type: str, events: list[dict]) -> None:
    """Ingest events via a temporary aiohttp TestClient (for in-memory manual-step tests)."""
    app = build_rest_app(mq)
    async with TestClient(TestServer(app)) as client:
        await client.post(
            f"/api/v1/{REST_ENDPOINT[event_type]}",
            json={"events": events},
        )


async def ingest_rest_http(port: int, event_type: str, events: list[dict]) -> None:
    """POST events to a running RestStarterModule HTTP endpoint."""
    url = f"http://127.0.0.1:{port}/api/v1/{REST_ENDPOINT[event_type]}"
    async with aiohttp.ClientSession() as session:
        resp = await session.post(url, json={"events": events})
        assert resp.status == 200


# ── Kafka ingestion ──────────────────────────────────────────────────────────


async def ingest_kafka_starter(mq: Any, event_type: str, events: list[dict]) -> None:
    """Ingest events by calling KafkaStarterModule._process_message directly."""
    starter = KafkaStarterModule(
        config=ModuleConfig({}),
        logger=LoggerFactory(default_impl="memory"),
        mq=mq,
        lifecycle=LifecycleManager(),
    )
    await starter.initialize()
    topic = NORM_TOPIC[event_type]
    for raw in events:
        starter._process_message(event_type, topic, raw)


def ingest_kafka_publish(producer: Any, event_type: str, events: list[dict]) -> None:
    """Publish events to client_* Kafka topics (for in-process or subprocess tests)."""
    topic = CLIENT_TOPIC[event_type]
    for event in events:
        producer.publish(topic, event)


# ── File ingestion ───────────────────────────────────────────────────────────


def ingest_files_memory(
    fs: Any, mq: Any, event_type: str, events: list[dict]
) -> None:
    """Write events to MemoryFileSystem and run FileProcessorModule inline."""
    path = f"ingest/{event_type}/{uuid.uuid4().hex[:8]}.jsonl"
    content = "\n".join(json.dumps(e) for e in events).encode()
    fs.write(path, content)
    module = FileProcessorModule(
        config=ModuleConfig({"file-path": path, "event-type": event_type}),
        logger=LoggerFactory(default_impl="memory"),
        fs=fs,
        mq=mq,
    )
    module.initialize()
    module.validate()
    module.execute()


def ingest_files_minio_inline(
    minio_fs: Any, mq: Any, event_type: str, events: list[dict]
) -> None:
    """Write events to MinIO and run FileProcessorModule inline (real FS, memory MQ)."""
    path = f"ingest/{event_type}/{uuid.uuid4().hex[:8]}.jsonl"
    content = "\n".join(json.dumps(e) for e in events).encode()
    minio_fs.write(path, content)
    module = FileProcessorModule(
        config=ModuleConfig({"file-path": path, "event-type": event_type}),
        logger=LoggerFactory(default_impl="memory"),
        fs=minio_fs,
        mq=mq,
    )
    module.initialize()
    module.validate()
    module.execute()


def ingest_files_minio_with_kafka(
    minio_fs: Any,
    kafka_producer: Any,
    event_type: str,
    events: list[dict],
    bootstrap_servers: str,
) -> None:
    """Write events to MinIO and run FileProcessorModule with a real KafkaQueue."""
    from de_platform.services.message_queue.kafka_queue import KafkaQueue
    from de_platform.services.secrets.env_secrets import EnvSecrets

    path = f"input/{event_type}/{uuid.uuid4().hex}.jsonl"
    minio_fs.write(
        path, ("\n".join(json.dumps(e) for e in events)).encode()
    )
    fp_secrets = EnvSecrets(overrides={
        "MQ_KAFKA_BOOTSTRAP_SERVERS": bootstrap_servers,
        "MQ_KAFKA_GROUP_ID": f"fp-{uuid.uuid4().hex[:8]}",
    })
    fp_mq = KafkaQueue(fp_secrets)
    fp_mq.connect()
    fp = FileProcessorModule(
        config=ModuleConfig({"file-path": path, "event-type": event_type}),
        logger=LoggerFactory(default_impl="memory"),
        fs=minio_fs,
        mq=fp_mq,
    )
    fp.run()
    fp_mq.disconnect()


def ingest_files_subprocess(
    minio_fs: Any,
    infra: Any,
    event_type: str,
    events: list[dict],
    launch_module_fn: Any,
) -> None:
    """Write events to MinIO and run file_processor as an OS subprocess."""
    path = f"input/{event_type}/{uuid.uuid4().hex}.jsonl"
    minio_fs.write(
        path, ("\n".join(json.dumps(e) for e in events)).encode()
    )
    gid = f"fp-{uuid.uuid4().hex[:8]}"
    proc = launch_module_fn(
        module_name="file_processor",
        infra=infra,
        db_flags=[],
        extra_flags=[
            "--fs", "minio",
            "--mq", "kafka",
            "--file-path", path,
            "--event-type", event_type,
        ],
        group_id=gid,
        health_port=0,
    )
    proc.wait(timeout=30)
    assert proc.returncode == 0, (
        f"file_processor failed: {proc.stderr.read().decode()[:500]}"
    )
