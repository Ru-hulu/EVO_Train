#!/usr/bin/env python3
"""Training task business functions for the TCP server."""

from __future__ import annotations

import json
from typing import Any

from sql_lite.sql_pack import sql_add_user_task, sql_delete_user_task, sql_get_user_all_task


def handle_request(text: str) -> dict[str, Any]:
    """Handle one complete JSON request and return a response dict."""
    try:
        request = json.loads(text)
    except json.JSONDecodeError:
        return {"message": "invalid json", "tasks": []}

    username = str(request.get("username") or "").strip()
    task_name = str(request.get("taskName") or "").strip()
    action = str(request.get("action") or "").strip()
    tasks = sql_get_user_all_task(username)
    if action == "任务同步":
        return {"message": "sync success", "tasks": tasks}
    if not username or not task_name:
        return {"message": "invalid request", "tasks": tasks}
    if action == "开始训练":
        if sql_add_user_task(username, task_name):
            message = "create task success"
            tasks = sql_get_user_all_task(username)
        else:
            message = "create task failed"
    elif action == "结束训练":
        if sql_delete_user_task(username, task_name):
            message = "delete task success"
            tasks = sql_get_user_all_task(username)
        else:
            message = "delete task failed"
    else:
        message = "invalid action"
    return {"message": message, "tasks": tasks}


def handle_request_text(text: str) -> str:
    """Handle one complete JSON request and return a JSON response string."""
    return json.dumps(handle_request(text), ensure_ascii=False)
