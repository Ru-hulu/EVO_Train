"""Thread-safe structured logging for EVO_Train.

Replaces ad-hoc ``print()`` calls in ``server_tcp`` and ``thread_pool``
with stdlib ``logging`` configured behind a ``QueueHandler`` /
``QueueListener`` pipeline so worker threads cannot interleave bytes
even under heavy concurrency.

Public API:

- :func:`setup_logging`   — call once at process start
- :func:`get_logger`      — module-local logger getter
- :func:`shutdown_logging` — drain queue + close handlers on exit
- :class:`RequestContext` — bundle of lifecycle ids for a request

Each ``LogRecord`` carries three extra fields, defaulting to ``"-"``:

- ``client_id``  — TCP peer address (host:port)
- ``worker_id``  — worker thread index, or ``"server"`` for the listener
- ``event_id``   — opaque per-request id assigned at accept time

This lets you grep one request's full lifecycle (accept → submit →
worker → handler → response → close) by filtering on ``event_id``.
"""

from __future__ import annotations

import json
import logging
import logging.handlers
import os
import queue
import sys
import threading
import time
import uuid
from dataclasses import dataclass
from logging import LogRecord
from pathlib import Path

DEFAULT_LOG_DIR = Path("logs")
DEFAULT_LOG_FILE = "server.log"
DEFAULT_MAX_BYTES = 100 * 1024 * 1024  # 100 MiB per file
DEFAULT_BACKUP_COUNT = 5
DEFAULT_LOG_LEVEL = "INFO"

_EXTRA_FIELDS = ("client_id", "worker_id", "event_id")
_DEFAULT_EXTRA = "-"

# Module-level state for orderly shutdown
_listener: logging.handlers.QueueListener | None = None
_log_queue: queue.Queue[logging.LogRecord] | None = None
_setup_lock = threading.Lock()
_setup_done = False


@dataclass
class RequestContext:
    """Per-request identifiers carried through the lifecycle."""

    client_id: str = _DEFAULT_EXTRA
    event_id: str = _DEFAULT_EXTRA

    @staticmethod
    def new(client_id: str) -> "RequestContext":
        """Create a context with a fresh event id for one request."""
        return RequestContext(client_id=client_id, event_id=uuid.uuid4().hex[:12])

    def as_extra(self, worker_id: str | int = _DEFAULT_EXTRA) -> dict[str, str]:
        """Return the dict to pass as ``extra=`` on a logger call."""
        return {
            "client_id": self.client_id,
            "worker_id": str(worker_id),
            "event_id": self.event_id,
        }


class _ContextFilter(logging.Filter):
    """Ensure every record has the three extra fields, even if caller forgot."""

    def filter(self, record: LogRecord) -> bool:
        for field in _EXTRA_FIELDS:
            if not hasattr(record, field):
                setattr(record, field, _DEFAULT_EXTRA)
        return True


class _HumanFormatter(logging.Formatter):
    """One line per record, fixed-width prefix, easy to read on a terminal."""

    def format(self, record: LogRecord) -> str:
        ts = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(record.created))
        ms = int((record.created - int(record.created)) * 1000)
        return (
            f"{ts}.{ms:03d} {record.levelname:<7} "
            f"[{record.name}] "
            f"client={getattr(record, 'client_id', _DEFAULT_EXTRA)} "
            f"worker={getattr(record, 'worker_id', _DEFAULT_EXTRA)} "
            f"event={getattr(record, 'event_id', _DEFAULT_EXTRA)} "
            f"{record.getMessage()}"
            + (f"\n{self.formatException(record.exc_info)}" if record.exc_info else "")
        )


class _JsonFormatter(logging.Formatter):
    """One JSON object per line — grep-friendly + ELK / Loki compatible."""

    def format(self, record: LogRecord) -> str:
        payload: dict[str, object] = {
            "ts": time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(record.created))
            + f".{int((record.created - int(record.created)) * 1000):03d}Z",
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "client_id": getattr(record, "client_id", _DEFAULT_EXTRA),
            "worker_id": getattr(record, "worker_id", _DEFAULT_EXTRA),
            "event_id": getattr(record, "event_id", _DEFAULT_EXTRA),
        }
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False)


def _resolve_format(name: str) -> logging.Formatter:
    if name == "json":
        return _JsonFormatter()
    return _HumanFormatter()


def setup_logging(
    *,
    log_dir: Path | str | None = None,
    log_file: str = DEFAULT_LOG_FILE,
    level: str | int = DEFAULT_LOG_LEVEL,
    console_format: str | None = None,
    file_format: str | None = None,
    max_bytes: int = DEFAULT_MAX_BYTES,
    backup_count: int = DEFAULT_BACKUP_COUNT,
) -> None:
    """Configure root logger with a thread-safe queue pipeline.

    Idempotent — calling twice is a no-op (returns immediately on the
    second call). Call once at process start before spawning workers.

    Environment variables (lower precedence than explicit args):

    - ``EVO_LOG_DIR``       directory for the rolling file
    - ``EVO_LOG_LEVEL``     ``DEBUG`` / ``INFO`` / ``WARNING`` / ``ERROR``
    - ``EVO_LOG_FORMAT_CONSOLE``  ``human`` (default) | ``json``
    - ``EVO_LOG_FORMAT_FILE``     ``json`` (default)  | ``human``
    """
    global _listener, _log_queue, _setup_done

    with _setup_lock:
        if _setup_done:
            return

        resolved_dir = Path(
            log_dir
            if log_dir is not None
            else os.environ.get("EVO_LOG_DIR", DEFAULT_LOG_DIR)
        ).expanduser()
        resolved_dir.mkdir(parents=True, exist_ok=True)

        resolved_level = os.environ.get("EVO_LOG_LEVEL", level)
        resolved_console_fmt = (
            console_format or os.environ.get("EVO_LOG_FORMAT_CONSOLE", "human")
        )
        resolved_file_fmt = (
            file_format or os.environ.get("EVO_LOG_FORMAT_FILE", "json")
        )

        log_path = resolved_dir / log_file

        console_handler = logging.StreamHandler(stream=sys.stderr)
        console_handler.setFormatter(_resolve_format(resolved_console_fmt))

        file_handler = logging.handlers.RotatingFileHandler(
            log_path,
            maxBytes=max_bytes,
            backupCount=backup_count,
            encoding="utf-8",
        )
        file_handler.setFormatter(_resolve_format(resolved_file_fmt))

        # Sink handlers run on the listener thread; no lock contention on workers.
        _log_queue = queue.Queue(-1)
        _listener = logging.handlers.QueueListener(
            _log_queue,
            console_handler,
            file_handler,
            respect_handler_level=False,
        )

        queue_handler = logging.handlers.QueueHandler(_log_queue)
        queue_handler.addFilter(_ContextFilter())

        root = logging.getLogger()
        # Drop any prior handlers so re-running tests / repeated imports
        # don't double-log.
        for handler in list(root.handlers):
            root.removeHandler(handler)
        root.addHandler(queue_handler)
        root.setLevel(resolved_level)

        _listener.start()
        _setup_done = True


def shutdown_logging() -> None:
    """Drain the queue and close sink handlers. Safe to call repeatedly."""
    global _listener, _log_queue, _setup_done

    with _setup_lock:
        if not _setup_done:
            return
        if _listener is not None:
            _listener.stop()
            for sink in _listener.handlers:
                sink.close()
            _listener = None
        _log_queue = None

        root = logging.getLogger()
        for handler in list(root.handlers):
            root.removeHandler(handler)
            handler.close()
        _setup_done = False


def get_logger(name: str) -> logging.Logger:
    """Return a logger that satisfies the EVO_Train context contract.

    The returned logger expects callers to pass ``extra={"client_id": ...,
    "worker_id": ..., "event_id": ...}`` on every call, but :class:`_ContextFilter`
    fills in ``"-"`` defaults if any field is omitted, so omitting extras
    is non-fatal — just less useful for tracing.
    """
    return logging.getLogger(name)
