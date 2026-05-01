#!/usr/bin/env python3
"""Submit and manage Alibaba Cloud PAI DLC training jobs.

Typical usage:
  python3 start_train.py submit \
    --dataset-path /mnt/nas/libero_10_no_noops_1.0.0_lerobot \
    --epochs 10 \
    --checkpoint-path /mnt/nas/checkpoints/run-001 \
    --checkpoint-frequency 1 \
    --gpu-count 1

Required PAI settings can be passed as flags or environment variables:
  PAI_WORKSPACE_ID, PAI_DLC_IMAGE, PAI_RESOURCE_ID, PAI_ECS_SPEC
"""

from __future__ import annotations

import argparse
import json
import os
import shlex
import sys
import time
from pathlib import Path
from typing import Any

try:
    from alibabacloud_pai_dlc20201203.client import Client as DLCClient
    from alibabacloud_pai_dlc20201203 import models as dlc_models
    from alibabacloud_tea_openapi import models as openapi_models
except ImportError as exc:
    print(
        "Missing Alibaba Cloud SDK dependencies. Install them with:\n"
        "  python3 -m pip install --user "
        "alibabacloud_pai-dlc20201203 alibabacloud_tea_openapi python-dotenv",
        file=sys.stderr,
    )
    raise SystemExit(2) from exc


DEFAULT_ENV_FILES = [
    Path.cwd() / ".env",
    Path.home() / "EVO_Train" / ".env",
    Path("/home/evomind/evo-data_backend/.env"),
]

DONE_STATUSES = {"Succeeded", "Succeed", "SUCCESS", "SUCCEEDED"}
FAILED_STATUSES = {"Failed", "FAILED", "Stopped", "STOPPED", "Deleted", "DELETED"}


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


def first_value(*values: str | None) -> str | None:
    for value in values:
        if value:
            return value
    return None


def env_first(*names: str) -> str | None:
    for name in names:
        value = os.environ.get(name)
        if value:
            return value
    return None


def require_value(value: str | None, name: str) -> str:
    if not value:
        raise SystemExit(f"Missing required value: {name}")
    return value


def create_client(region_id: str) -> DLCClient:
    access_key_id = require_value(
        env_first("ALIBABA_CLOUD_ACCESS_KEY_ID", "ALIBABACLOUD_ACCESS_KEY_ID", "OSS_ACCESS_KEY_ID"),
        "ALIBABA_CLOUD_ACCESS_KEY_ID or OSS_ACCESS_KEY_ID",
    )
    access_key_secret = require_value(
        env_first("ALIBABA_CLOUD_ACCESS_KEY_SECRET", "ALIBABACLOUD_ACCESS_KEY_SECRET", "OSS_ACCESS_KEY_SECRET"),
        "ALIBABA_CLOUD_ACCESS_KEY_SECRET or OSS_ACCESS_KEY_SECRET",
    )
    endpoint = os.environ.get("PAI_DLC_ENDPOINT", f"pai-dlc.{region_id}.aliyuncs.com")
    config = openapi_models.Config(
        access_key_id=access_key_id,
        access_key_secret=access_key_secret,
        region_id=region_id,
        endpoint=endpoint,
    )
    return DLCClient(config)


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


def build_train_command(args: argparse.Namespace) -> str:
    if args.command:
        return args.command

    template = first_value(
        args.command_template,
        os.environ.get("DLC_TRAIN_COMMAND_TEMPLATE"),
        "python train.py --dataset-path {dataset_path} --epochs {epochs} "
        "--checkpoint-path {checkpoint_path} --checkpoint-frequency {checkpoint_frequency}",
    )
    values = {
        "dataset_path": shlex.quote(args.dataset_path),
        "epochs": args.epochs,
        "checkpoint_path": shlex.quote(args.checkpoint_path),
        "checkpoint_frequency": args.checkpoint_frequency,
        "gpu_count": args.gpu_count,
    }
    return template.format(**values)


def make_data_sources(args: argparse.Namespace) -> list[Any]:
    data_sources: list[Any] = []
    for spec in args.mount or []:
        # Format: URI=MOUNT_PATH[:RO|RW]
        try:
            uri, rest = spec.split("=", 1)
        except ValueError as exc:
            raise SystemExit(f"Invalid --mount value: {spec}. Expected URI=MOUNT_PATH[:RO|RW]") from exc
        mount_path = rest
        mount_access = "RW"
        if rest.endswith(":RO") or rest.endswith(":RW"):
            mount_path, mount_access = rest.rsplit(":", 1)
        data_sources.append(
            dlc_models.CreateJobRequestDataSources(
                uri=uri,
                mount_path=mount_path,
                mount_access=mount_access,
            )
        )
    return data_sources


def build_job_request(args: argparse.Namespace) -> Any:
    workspace_id = require_value(first_value(args.workspace_id, os.environ.get("PAI_WORKSPACE_ID")), "--workspace-id or PAI_WORKSPACE_ID")
    image = require_value(first_value(args.image, os.environ.get("PAI_DLC_IMAGE")), "--image or PAI_DLC_IMAGE")
    ecs_spec = require_value(first_value(args.ecs_spec, os.environ.get("PAI_ECS_SPEC")), "--ecs-spec or PAI_ECS_SPEC")
    resource_id = first_value(args.resource_id, os.environ.get("PAI_RESOURCE_ID"))

    resource_config = dlc_models.ResourceConfig(
        gpu=args.gpu_count,
        cpu=args.cpu,
        memory=args.memory,
    )
    if args.gpu_type:
        resource_config.gputype = args.gpu_type

    job_spec = dlc_models.JobSpec(
        type=args.role,
        image=image,
        pod_count=args.pod_count,
        ecs_spec=ecs_spec,
        resource_config=resource_config,
    )

    return dlc_models.CreateJobRequest(
        display_name=args.job_name,
        workspace_id=workspace_id,
        resource_id=resource_id,
        job_type=args.job_type,
        job_specs=[job_spec],
        data_sources=make_data_sources(args),
        user_command=build_train_command(args),
        accessibility=args.accessibility,
        job_max_running_time_minutes=args.max_running_minutes,
        description=args.description,
        thirdparty_libs=args.thirdparty_libs,
        thirdparty_lib_dir=args.thirdparty_lib_dir,
        envs={
            "DATASET_PATH": args.dataset_path,
            "EPOCHS": str(args.epochs),
            "CHECKPOINT_PATH": args.checkpoint_path,
            "CHECKPOINT_FREQUENCY": str(args.checkpoint_frequency),
            "GPU_COUNT": str(args.gpu_count),
        },
    )


def submit_job(client: DLCClient, args: argparse.Namespace) -> str:
    request = build_job_request(args)
    if args.dry_run:
        print_json(request)
        return ""
    log("submitting PAI DLC training job")
    response = client.create_job(request)
    print_json(response.body)
    job_id = getattr(response.body, "job_id", None)
    if not job_id:
        raise SystemExit("CreateJob did not return a job_id")
    return job_id


def get_job(client: DLCClient, job_id: str, need_detail: bool = False) -> Any:
    response = client.get_job(job_id, dlc_models.GetJobRequest(need_detail=need_detail))
    body = response.body
    if need_detail:
        print_json(body)
    else:
        print_json(
            {
                "job_id": body.job_id,
                "display_name": body.display_name,
                "status": body.status,
                "sub_status": body.sub_status,
                "reason_code": body.reason_code,
                "reason_message": body.reason_message,
                "workspace_id": body.workspace_id,
                "resource_id": body.resource_id,
                "user_command": body.user_command,
            }
        )
    return body


def wait_job(client: DLCClient, job_id: str, timeout: int, interval: int) -> None:
    deadline = time.time() + timeout
    while True:
        body = get_job(client, job_id, need_detail=False)
        status = getattr(body, "status", None)
        if status in DONE_STATUSES:
            log(f"job finished successfully: {status}")
            return
        if status in FAILED_STATUSES:
            raise SystemExit(f"job reached terminal status: {status}")
        if time.time() >= deadline:
            raise SystemExit(f"timeout waiting for job; last status={status}")
        time.sleep(interval)


def stop_job(client: DLCClient, job_id: str) -> None:
    response = client.stop_job(job_id, dlc_models.StopJobRequest())
    print_json(response.body)


def add_common_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--region", default=os.getenv("ALIYUN_REGION", "cn-hangzhou"))
    parser.add_argument("--env-file", help="Optional .env file with Alibaba Cloud credentials and PAI defaults")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Submit PAI DLC training jobs from ECS.")
    add_common_args(parser)
    sub = parser.add_subparsers(dest="command_name", required=True)

    submit = sub.add_parser("submit", help="Create a PAI DLC training job")
    submit.add_argument("--dataset-path", required=True, help="Path visible inside DLC container, for example /mnt/nas/dataset")
    submit.add_argument("--epochs", required=True, type=int)
    submit.add_argument("--checkpoint-path", required=True)
    submit.add_argument("--checkpoint-frequency", required=True, type=int)
    submit.add_argument("--gpu-count", required=True, type=int)
    submit.add_argument("--job-name", default="evo-train")
    submit.add_argument("--workspace-id", help="Defaults to PAI_WORKSPACE_ID")
    submit.add_argument("--resource-id", help="Defaults to PAI_RESOURCE_ID")
    submit.add_argument("--image", help="Training image. Defaults to PAI_DLC_IMAGE")
    submit.add_argument("--ecs-spec", help="PAI DLC ECS spec. Defaults to PAI_ECS_SPEC")
    submit.add_argument("--gpu-type")
    submit.add_argument("--cpu", type=int)
    submit.add_argument("--memory", type=int, help="Memory in GB")
    submit.add_argument("--pod-count", type=int, default=1)
    submit.add_argument("--role", default="Worker")
    submit.add_argument("--job-type", default="PyTorch")
    submit.add_argument("--accessibility", default="PRIVATE")
    submit.add_argument("--max-running-minutes", type=int)
    submit.add_argument("--description")
    submit.add_argument("--mount", action="append", help="Mount data source as URI=MOUNT_PATH[:RO|RW], for example nas://xxx/=/mnt/nas:RW")
    submit.add_argument("--command", help="Full training command. Overrides command template")
    submit.add_argument("--command-template", help="Template using {dataset_path}, {epochs}, {checkpoint_path}, {checkpoint_frequency}, {gpu_count}")
    submit.add_argument("--thirdparty-libs", action="append")
    submit.add_argument("--thirdparty-lib-dir")
    submit.add_argument("--wait", action="store_true")
    submit.add_argument("--timeout", type=int, default=86400)
    submit.add_argument("--interval", type=int, default=30)
    submit.add_argument("--dry-run", action="store_true", help="Print CreateJob request without submitting")

    status = sub.add_parser("status", help="Show DLC job status")
    status.add_argument("--job-id", required=True)
    status.add_argument("--detail", action="store_true")

    stop = sub.add_parser("stop", help="Stop a DLC job")
    stop.add_argument("--job-id", required=True)

    return parser.parse_args()


def main() -> None:
    args = parse_args()
    load_env(args.env_file)
    client = create_client(args.region)

    if args.command_name == "submit":
        job_id = submit_job(client, args)
        if job_id and args.wait:
            wait_job(client, job_id, args.timeout, args.interval)
    elif args.command_name == "status":
        get_job(client, args.job_id, args.detail)
    elif args.command_name == "stop":
        stop_job(client, args.job_id)
    else:
        raise SystemExit(f"unknown command: {args.command_name}")


if __name__ == "__main__":
    main()
