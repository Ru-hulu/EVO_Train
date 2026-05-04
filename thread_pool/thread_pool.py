#!/usr/bin/env python3
"""
Author: Ru-hulu
Date: 2026-05-03

This module implements a daemon thread pool with 4 or 8 worker threads
to consume and process queued training tasks safely and efficiently.
It supports task submission, concurrent execution, and graceful shutdown.
"""
from __future__ import annotations

import argparse
import queue
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Callable

from evo_train_logging import get_logger

STOP_EVENT = object()

_logger = get_logger("thread_pool")


@dataclass
class TrainTaskEvent:
    client_id: str
    request_text: str
    response_callback: Callable[[str], None] | None = None
    event_id: str = field(default="-")


def fake_train_task_handler(request_text: str) -> str:
    """Return a temporary debug response for one request."""
    time.sleep(0.2)
    return f"debug response for: {request_text}"


def _extra(event: TrainTaskEvent | None, worker_id: str | int) -> dict[str, str]:
    """Build the structured-log extra dict from event + worker context."""
    return {
        "client_id": event.client_id if event is not None else "-",
        "worker_id": str(worker_id),
        "event_id": event.event_id if event is not None else "-",
    }


class ThreadPool:
    def __init__(self, workers: int, task_handler: Callable[[str], str] = fake_train_task_handler) -> None:
        """Create a 4-thread or 8-thread worker pool."""
        if workers not in (4, 8):
            raise ValueError("workers must be 4 or 8")
        self.workers = workers
        self.task_handler = task_handler
        self.train_task_queue: queue.Queue[TrainTaskEvent | object] = queue.Queue()
        self.threads: list[threading.Thread] = []

    def start(self) -> None:
        """Start all worker threads."""
        for index in range(self.workers):
            thread = threading.Thread(target=self._worker_loop, args=(index,), daemon=True)
            thread.start()
            self.threads.append(thread)
        _logger.info(
            "thread pool started with %d workers",
            self.workers,
            extra=_extra(None, "pool"),
        )

    def submit(self, event: TrainTaskEvent) -> None:
        """Push one training event into the worker queue."""
        self.train_task_queue.put(event)
        _logger.info(
            "queued event from %s: %s",
            event.client_id,
            event.request_text,
            extra=_extra(event, "pool"),
        )

    def stop(self) -> None:
        """Stop all worker threads after queued work is done."""
        for _ in self.threads:
            self.train_task_queue.put(STOP_EVENT)
        for thread in self.threads:
            thread.join()
        _logger.info("all worker threads stopped", extra=_extra(None, "pool"))

    def _worker_loop(self, worker_id: int) -> None:
        """Continuously consume queued events in one worker thread."""
        _logger.info("worker ready", extra=_extra(None, worker_id))
        while True:
            event = self.train_task_queue.get()  # thread safe no need lock
            try:
                if event is STOP_EVENT:
                    _logger.info("worker stopping", extra=_extra(None, worker_id))
                    return
                self._handle_event(worker_id, event)
            finally:
                self.train_task_queue.task_done()

    def _handle_event(self, worker_id: int, event: Any) -> None:
        """Run the task handler and optionally return its response."""
        _logger.info(
            "handling request: %s",
            event.request_text,
            extra=_extra(event, worker_id),
        )
        try:
            response = self.task_handler(event.request_text)
        except Exception:
            _logger.exception(
                "task handler raised for request: %s",
                event.request_text,
                extra=_extra(event, worker_id),
            )
            return
        if event.response_callback is not None:
            event.response_callback(response)
        _logger.info("finished request", extra=_extra(event, worker_id))


def build_parser() -> argparse.ArgumentParser:
    """Build command-line options for thread-pool debugging."""
    parser = argparse.ArgumentParser(description="Debug training task thread pool.")
    parser.add_argument(
        "--workers",
        type=int,
        choices=(4, 8),
        default=4,
        help="Worker thread count, default: 4",
    )
    return parser
# def main() -> int:
#     """Run a standalone thread-pool debug demo."""
#     args = build_parser().parse_args()
#     pool = ThreadPool(args.workers)
#     pool.start()

#     for index in range(10):
#         pool.submit(
#             TrainTaskEvent(
#                 client_id=f"client-{index}",
#                 request_text=f'{{"username":"user-{index % 3}","action":"任务同步"}}',
#             )
#         )

#     pool.train_task_queue.join()
#     pool.stop()
#     return 0


# if __name__ == "__main__":
#     raise SystemExit(main())
