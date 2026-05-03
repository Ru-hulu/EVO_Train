#!/usr/bin/env python3
"""SQLite helpers for tcp_read_server task storage."""

from __future__ import annotations

import os
import sqlite3
from pathlib import Path


DEFAULT_DB_PATH = Path(__file__).resolve().parent / "data" / "tasks.sqlite3"
DB_PATH = Path(os.environ.get("EVO_TRAIN_TASK_DB", DEFAULT_DB_PATH)).expanduser()


def _connect() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    _init_db(conn)
    return conn


def _init_db(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS user_tasks (
            username TEXT NOT NULL,
            task_name TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT '',
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (username, task_name)
        )
        """
    )
    conn.commit()


def sql_get_user_all_task(username: str) -> list[dict[str, str]]:
    """Return all tasks for one user in the response format expected by the TCP server."""
    with _connect() as conn:
        rows = conn.execute(
            """
            SELECT task_name, status
            FROM user_tasks
            WHERE username = ?
            ORDER BY created_at ASC, task_name ASC
            """,
            (username,),
        ).fetchall()

    return [{"taskName": row["task_name"], "status": row["status"]} for row in rows]


def sql_add_user_task(username: str, task_name: str) -> bool:
    """Add a task for one user. Return False when the task already exists."""
    try:
        with _connect() as conn:
            conn.execute(
                """
                INSERT INTO user_tasks (username, task_name, status)
                VALUES (?, ?, '')
                """,
                (username, task_name),
            )
        return True
    except sqlite3.IntegrityError:
        return False


def sql_delete_user_task(username: str, task_name: str) -> bool:
    """Delete a task for one user. Return True only when a row was deleted."""
    with _connect() as conn:
        cursor = conn.execute(
            """
            DELETE FROM user_tasks
            WHERE username = ? AND task_name = ?
            """,
            (username, task_name),
        )
        return cursor.rowcount > 0
