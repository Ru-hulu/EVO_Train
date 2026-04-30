#!/usr/bin/env python3
"""Create/start Alibaba Cloud PAI DSW instances with NAS mounted.

This script is intended to run on the ECS host as user `evomind`.
It uses Alibaba Cloud PAI DSW OpenAPI through the official Python SDK.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path
from typing import Any

try:
    from alibabacloud_pai_dsw20220101.client import Client as DSWClient
    from alibabacloud_pai_dsw20220101 import models as dsw_models
    from alibabacloud_tea_openapi import models as openapi_models
except ImportError as exc:
    print(
        "Missing Alibaba Cloud SDK dependencies. Install them with:\n"
        "  python3 -m pip install --user "
        "alibabacloud_pai-dsw20220101 alibabacloud_tea_openapi python-dotenv",
        file=sys.stderr,
    )
    raise SystemExit(2) from exc


DEFAULT_ENV_FILES = [
    Path.cwd() / ".env",
    Path.home() / "EVO_Train" / ".env",
    Path("/home/evomind/evo-data_backend/.env"),
]

RUNNING_STATUSES = {"Running", "RUNNING"}
READY_STATUSES = {"Running", "RUNNING"}
TERMINAL_FAILURE_STATUSES = {"Failed", "FAILED", "Stopped", "STOPPED", "Deleted", "DELETED"}


def log(message: str) -> None:
    print(f"[start_train] {message}", flush=True)


def load_dotenv_file(path: Path) -> None:
    if not path.exists():
        return
    for raw_line in path.read_text().splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.split(" #", 1)[0].strip().strip('"').strip("'")
        os.environ.setdefault(key, value)


def load_env(extra_env: str | None) -> None:
    for path in DEFAULT_ENV_FILES:
        load_dotenv_file(path)
    if extra_env:
        load_dotenv_file(Path(extra_env).expanduser())


def getenv_first(*names: str) -> str | None:
    for name in names:
        value = os.environ.get(name)
        if value:
            return value
    return None


def require_env(*names: str) -> str:
    value = getenv_first(*names)
    if not value:
        joined = " or ".join(names)
        raise SystemExit(f"Missing credential environment variable: {joined}")
    return value


def create_client(region_id: str) -> DSWClient:
    access_key_id = require_env("ALIBABA_CLOUD_ACCESS_KEY_ID", "ALIBABACLOUD_ACCESS_KEY_ID", "OSS_ACCESS_KEY_ID")
    access_key_secret = require_env(
        "ALIBABA_CLOUD_ACCESS_KEY_SECRET",
        "ALIBABACLOUD_ACCESS_KEY_SECRET",
        "OSS_ACCESS_KEY_SECRET",
    )
    endpoint = os.environ.get("PAI_DSW_ENDPOINT", f"pai-dsw.{region_id}.aliyuncs.com")
    config = openapi_models.Config(
        access_key_id=access_key_id,
        access_key_secret=access_key_secret,
        region_id=region_id,
        endpoint=endpoint,
    )
    return DSWClient(config)


def to_plain(value: Any) -> Any:
    if hasattr(value, "to_map"):
        return value.to_map()
    if isinstance(value, list):
        return [to_plain(item) for item in value]
    if isinstance(value, dict):
        return {key: to_plain(item) for key, item in value.items()}
    return value


def print_json(value: Any) -> None:
    print(json.dumps(to_plain(value), ensure_ascii=False, indent=2, sort_keys=True))


def ensure_success(response_body: Any) -> None:
    success = getattr(response_body, "success", None)
    code = getattr(response_body, "code", None)
    message = getattr(response_body, "message", None)
    if success is False:
        raise SystemExit(f"PAI API failed: code={code}, message={message}")


def make_dataset(uri: str, mount_path: str, mount_access: str, options: str | None) -> Any:
    dataset = dsw_models.CreateInstanceRequestDatasets(
        uri=uri,
        mount_path=mount_path,
        mount_access=mount_access,
    )
    if options:
        dataset.options = options
    return dataset


def build_create_request(args: argparse.Namespace) -> Any:
    request = dsw_models.CreateInstanceRequest(
        instance_name=args.instance_name,
        workspace_id=args.workspace_id,
        ecs_spec=args.ecs_spec,
        resource_id=args.resource_id,
        accessibility=args.accessibility,
        datasets=[make_dataset(args.nas_uri, args.nas_mount_path, args.nas_mount_access, args.nas_options)],
    )
    if args.image_id:
        request.image_id = args.image_id
    if args.image_url:
        request.image_url = args.image_url
    if args.user_id:
        request.user_id = args.user_id
    if args.workspace_source:
        request.workspace_source = args.workspace_source
    if args.cpu or args.memory:
        request.requested_resource = dsw_models.CreateInstanceRequestRequestedResource(
            cpu=args.cpu,
            memory=args.memory,
            gpu=0,
        )
    if args.start_command:
        request.user_command = dsw_models.CreateInstanceRequestUserCommand(
            on_start=dsw_models.CreateInstanceRequestUserCommandOnStart(content=args.start_command)
        )
    return request


def create_instance(client: DSWClient, args: argparse.Namespace) -> str:
    request = build_create_request(args)
    log("creating DSW instance")
    response = client.create_instance(request)
    body = response.body
    ensure_success(body)
    print_json(body)
    instance_id = getattr(body, "instance_id", None)
    if not instance_id:
        raise SystemExit("CreateInstance did not return an instance_id")
    return instance_id


def start_instance(client: DSWClient, instance_id: str) -> None:
    log(f"starting DSW instance: {instance_id}")
    response = client.start_instance(instance_id)
    ensure_success(response.body)
    print_json(response.body)


def get_instance(client: DSWClient, instance_id: str, verbose: bool = False) -> Any:
    response = client.get_instance(instance_id, dsw_models.GetInstanceRequest())
    body = response.body
    ensure_success(body)
    if verbose:
        print_json(body)
    else:
        summary = {
            "instance_id": body.instance_id,
            "instance_name": body.instance_name,
            "status": body.status,
            "ecs_spec": body.ecs_spec,
            "resource_id": body.resource_id,
            "workspace_id": body.workspace_id,
            "jupyterlab_url": body.jupyterlab_url,
            "instance_url": body.instance_url,
            "datasets": to_plain(body.datasets),
        }
        print_json(summary)
    return body


def wait_instance(client: DSWClient, instance_id: str, timeout: int, interval: int) -> Any:
    deadline = time.time() + timeout
    while True:
        body = get_instance(client, instance_id, verbose=False)
        status = getattr(body, "status", None)
        if status in READY_STATUSES:
            log(f"instance is ready: {status}")
            return body
        if status in TERMINAL_FAILURE_STATUSES:
            raise SystemExit(f"instance entered terminal status: {status}")
        if time.time() >= deadline:
            raise SystemExit(f"timeout waiting for instance status; last status={status}")
        time.sleep(interval)


def list_cpu_specs(client: DSWClient, args: argparse.Namespace) -> None:
    request = dsw_models.ListEcsSpecsRequest(
        accelerator_type="CPU",
        page_number=args.page_number,
        page_size=args.page_size,
    )
    response = client.list_ecs_specs(request)
    ensure_success(response.body)
    print_json(response.body)


def add_common_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--region", default=os.getenv("ALIYUN_REGION", "cn-hangzhou"))
    parser.add_argument("--env-file", help="Optional .env file containing Alibaba Cloud credentials")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Create/start a CPU PAI DSW instance with NAS mounted.")
    add_common_args(parser)
    sub = parser.add_subparsers(dest="command", required=True)

    create = sub.add_parser("create", help="Create a CPU DSW instance with NAS mounted")
    create.add_argument("--workspace-id", required=True)
    create.add_argument("--instance-name", default="evo-train-cpu")
    create.add_argument("--ecs-spec", required=True, help="CPU instance spec, for example ecs.c6.large")
    create.add_argument("--resource-id", help="Optional PAI resource group/resource ID")
    create.add_argument("--image-id", help="DSW image ID")
    create.add_argument("--image-url", help="Custom image URL")
    create.add_argument("--nas-uri", required=True, help="NAS URI accepted by PAI DSW, for example nas://.../")
    create.add_argument("--nas-mount-path", default="/mnt/nas")
    create.add_argument("--nas-mount-access", default="RW", choices=["RO", "RW"])
    create.add_argument("--nas-options", help="Optional DSW dataset mount options")
    create.add_argument("--accessibility", default="PRIVATE", choices=["PRIVATE", "PUBLIC"])
    create.add_argument("--workspace-source")
    create.add_argument("--user-id")
    create.add_argument("--cpu", type=int, help="Optional requested CPU count")
    create.add_argument("--memory", type=int, help="Optional requested memory in GB")
    create.add_argument("--start-command", help="Optional shell command run when the DSW instance starts")
    create.add_argument("--start", action="store_true", help="Start the instance after creation")
    create.add_argument("--wait", action="store_true", help="Wait until status is Running after start")
    create.add_argument("--timeout", type=int, default=1800)
    create.add_argument("--interval", type=int, default=15)

    start = sub.add_parser("start", help="Start an existing DSW instance")
    start.add_argument("--instance-id", required=True)
    start.add_argument("--wait", action="store_true")
    start.add_argument("--timeout", type=int, default=1800)
    start.add_argument("--interval", type=int, default=15)

    status = sub.add_parser("status", help="Show DSW instance status")
    status.add_argument("--instance-id", required=True)
    status.add_argument("--verbose", action="store_true")

    specs = sub.add_parser("list-cpu-specs", help="List available CPU ECS specs for DSW")
    specs.add_argument("--page-number", type=int, default=1)
    specs.add_argument("--page-size", type=int, default=50)

    return parser.parse_args()


def main() -> None:
    args = parse_args()
    load_env(args.env_file)
    client = create_client(args.region)

    if args.command == "create":
        instance_id = create_instance(client, args)
        if args.start:
            start_instance(client, instance_id)
        if args.wait:
            wait_instance(client, instance_id, args.timeout, args.interval)
    elif args.command == "start":
        start_instance(client, args.instance_id)
        if args.wait:
            wait_instance(client, args.instance_id, args.timeout, args.interval)
    elif args.command == "status":
        get_instance(client, args.instance_id, args.verbose)
    elif args.command == "list-cpu-specs":
        list_cpu_specs(client, args)
    else:
        raise SystemExit(f"unknown command: {args.command}")


if __name__ == "__main__":
    main()
