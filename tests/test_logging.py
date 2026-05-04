#!/usr/bin/env python3
"""Tests for evo_train_logging — focuses on multi-thread safety."""

from __future__ import annotations

import json
import logging
import os
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from evo_train_logging import (
    RequestContext,
    get_logger,
    setup_logging,
    shutdown_logging,
)


class _LoggingTestBase(unittest.TestCase):
    """Common setup that isolates each test inside its own log directory."""

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.log_dir = Path(self._tmp.name)
        self.log_path = self.log_dir / "server.log"
        # Ensure no env var bleeds in from the host environment.
        for key in (
            "EVO_LOG_DIR",
            "EVO_LOG_LEVEL",
            "EVO_LOG_FORMAT_CONSOLE",
            "EVO_LOG_FORMAT_FILE",
        ):
            os.environ.pop(key, None)

    def tearDown(self) -> None:
        shutdown_logging()
        self._tmp.cleanup()

    def _read_log_lines(self) -> list[str]:
        # Allow listener thread to drain.
        time.sleep(0.05)
        return self.log_path.read_text(encoding="utf-8").splitlines()


class TestConcurrentLogging(_LoggingTestBase):
    """Verify that records from concurrent threads do not interleave bytes."""

    def test_four_workers_thousand_records_each_yields_well_formed_lines(self) -> None:
        setup_logging(log_dir=self.log_dir, file_format="json")
        logger = get_logger("concurrency_test")

        def worker(worker_id: int) -> None:
            ctx = RequestContext.new(client_id=f"client-{worker_id}")
            for _ in range(1000):
                logger.info("payload", extra=ctx.as_extra(worker_id=worker_id))

        threads = [threading.Thread(target=worker, args=(wid,)) for wid in range(4)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        shutdown_logging()  # ensure listener has flushed before we read

        lines = self.log_path.read_text(encoding="utf-8").splitlines()
        self.assertEqual(len(lines), 4 * 1000)

        # Every line must be a complete, well-formed JSON object — proves
        # bytes from different threads were never interleaved.
        worker_counts: dict[str, int] = {}
        for line in lines:
            payload = json.loads(line)
            worker_counts[payload["worker_id"]] = (
                worker_counts.get(payload["worker_id"], 0) + 1
            )
            self.assertEqual(payload["message"], "payload")
            self.assertEqual(payload["level"], "INFO")

        for worker_id in ("0", "1", "2", "3"):
            self.assertEqual(worker_counts[worker_id], 1000)


class TestExtraFieldsPresent(_LoggingTestBase):
    """Verify the three contract fields appear on every record."""

    def test_explicit_extra_round_trips_through_json_handler(self) -> None:
        setup_logging(log_dir=self.log_dir, file_format="json")
        logger = get_logger("extra_test")
        ctx = RequestContext(client_id="1.2.3.4:5678", event_id="abc123def456")
        logger.info("hello", extra=ctx.as_extra(worker_id=2))
        shutdown_logging()

        lines = self.log_path.read_text(encoding="utf-8").splitlines()
        self.assertEqual(len(lines), 1)
        payload = json.loads(lines[0])
        self.assertEqual(payload["client_id"], "1.2.3.4:5678")
        self.assertEqual(payload["worker_id"], "2")
        self.assertEqual(payload["event_id"], "abc123def456")
        self.assertEqual(payload["message"], "hello")
        self.assertEqual(payload["logger"], "extra_test")

    def test_missing_extra_falls_back_to_default_marker(self) -> None:
        setup_logging(log_dir=self.log_dir, file_format="json")
        logger = get_logger("extra_default_test")
        logger.info("no extras passed")  # caller forgot extras
        shutdown_logging()

        payload = json.loads(self.log_path.read_text(encoding="utf-8").splitlines()[0])
        self.assertEqual(payload["client_id"], "-")
        self.assertEqual(payload["worker_id"], "-")
        self.assertEqual(payload["event_id"], "-")


class TestRotatingFileHandler(_LoggingTestBase):
    """Verify that rotation kicks in at the configured size."""

    def test_log_file_rolls_over_when_max_bytes_exceeded(self) -> None:
        # Tiny cap forces rotation after a handful of records.
        setup_logging(
            log_dir=self.log_dir,
            file_format="json",
            max_bytes=512,
            backup_count=2,
        )
        logger = get_logger("rotation_test")
        ctx = RequestContext.new(client_id="rot-client")
        for _ in range(50):
            logger.info("rotation payload " + "x" * 50, extra=ctx.as_extra(worker_id=0))
        shutdown_logging()

        files = sorted(self.log_dir.iterdir())
        names = {p.name for p in files}
        self.assertIn("server.log", names)
        # At least one rotated backup must exist after 50 large records.
        rotated = [n for n in names if n.startswith("server.log.")]
        self.assertGreaterEqual(len(rotated), 1)


class TestShutdownDrainsQueue(_LoggingTestBase):
    """Verify that shutdown_logging blocks until every queued record is written."""

    def test_records_emitted_just_before_shutdown_are_persisted(self) -> None:
        setup_logging(log_dir=self.log_dir, file_format="json")
        logger = get_logger("shutdown_test")
        ctx = RequestContext.new(client_id="last-client")
        for index in range(200):
            logger.info("late record %d", index, extra=ctx.as_extra(worker_id="server"))

        shutdown_logging()  # must drain the queue before returning
        lines = self.log_path.read_text(encoding="utf-8").splitlines()
        self.assertEqual(len(lines), 200)
        # Sanity: the last record we sent is in the file.
        last = json.loads(lines[-1])
        self.assertIn("late record 199", last["message"])


class TestSetupIdempotent(_LoggingTestBase):
    """Calling setup_logging twice should not double-attach handlers."""

    def test_second_setup_call_is_noop(self) -> None:
        setup_logging(log_dir=self.log_dir, file_format="json")
        setup_logging(log_dir=self.log_dir, file_format="json")  # must not double-add

        logger = get_logger("idempotent_test")
        logger.info("only once please")
        shutdown_logging()

        lines = self.log_path.read_text(encoding="utf-8").splitlines()
        self.assertEqual(len(lines), 1)


if __name__ == "__main__":
    # Surface DEBUG noise from libraries during test runs.
    logging.captureWarnings(True)
    unittest.main(verbosity=2)
