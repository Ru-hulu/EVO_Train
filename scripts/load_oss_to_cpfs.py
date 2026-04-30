#!/usr/bin/env python3
"""Load data from Alibaba Cloud OSS to a mounted CPFS directory."""

from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
from pathlib import Path


def log(message: str) -> None:
    print(f"[load_oss_to_cpfs] {message}", flush=True)


def fail(message: str) -> None:
    print(f"ERROR: {message}", file=sys.stderr)
    raise SystemExit(1)


def run(command: list[str], dry_run: bool = False) -> None:
    quoted = " ".join(shlex_quote(part) for part in command)
    if dry_run:
        log(f"dry-run: {quoted}")
        return
    log(quoted)
    subprocess.run(command, check=True)


def shlex_quote(value: str) -> str:
    import shlex

    return shlex.quote(value)


def find_executable(names: list[str], dry_run: bool = False) -> str:
    for name in names:
        path = shutil.which(name)
        if path:
            return path
    if dry_run:
        return names[0]
    fail(f"missing required command: one of {', '.join(names)}")


def is_mount_point(path: Path) -> bool:
    try:
        return path.is_mount()
    except OSError:
        return False


def existing_parent(path: Path) -> Path:
    current = path
    while not current.exists() and current != current.parent:
        current = current.parent
    if not current.is_dir():
        fail(f"no existing parent directory for destination: {path}")
    return current


def planned_parent(path: Path, args: argparse.Namespace) -> Path:
    if path.exists():
        return existing_parent(path)
    if args.dry_run and args.mount_source:
        return Path(args.mount_point or args.cpfs).expanduser().resolve()
    return existing_parent(path)


def filesystem_root(path: Path) -> str:
    output = subprocess.check_output(["df", "-P", str(path)], text=True)
    lines = output.strip().splitlines()
    if len(lines) < 2:
        return ""
    return lines[1].split()[-1]


def print_disk_usage(path: Path) -> None:
    subprocess.run(["df", "-h", str(path)], check=False)


def mount_cpfs(args: argparse.Namespace) -> None:
    mount_point = args.mount_point or args.cpfs
    mount_point = Path(mount_point).expanduser().resolve()
    cpfs_dst = Path(args.cpfs).expanduser().resolve()

    try:
        cpfs_dst.relative_to(mount_point)
    except ValueError:
        fail("--cpfs must be equal to or below --mount-point when --mount-source is used")

    if is_mount_point(mount_point):
        log(f"CPFS mount point is already mounted: {mount_point}")
        return

    command = [
        "mount",
        "-t",
        args.mount_type,
        "-o",
        args.mount_options,
        args.mount_source,
        str(mount_point),
    ]

    if args.dry_run:
        log(f"dry-run: mkdir -p {shlex_quote(str(mount_point))}")
        run(command, dry_run=True)
        return

    mount_point.mkdir(parents=True, exist_ok=True)
    run(command)


def build_ossutil_command(args: argparse.Namespace, ossutil_bin: str) -> list[str]:
    command = [ossutil_bin]
    if args.endpoint:
        command.extend(["--endpoint", args.endpoint])

    command.extend(["cp", "-r", args.oss, args.cpfs])

    if args.update:
        command.append("--update")

    command.extend(["--jobs", str(args.jobs)])
    return command


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Load data from Alibaba Cloud OSS to CPFS on an ECS instance.",
    )
    parser.add_argument("--oss", required=True, help="OSS source, for example oss://bucket/path/")
    parser.add_argument("--cpfs", required=True, help="CPFS destination directory")
    parser.add_argument("--mount-source", help="CPFS mount source, for example 192.168.0.10:/cpfs-xxx/")
    parser.add_argument("--mount-point", help="CPFS mount point, for example /mnt/cpfs")
    parser.add_argument("--mount-type", default=os.getenv("CPFS_MOUNT_TYPE", "nfs"))
    parser.add_argument("--mount-options", default=os.getenv("CPFS_MOUNT_OPTIONS", "vers=3,nolock,proto=tcp"))
    parser.add_argument("--endpoint", help="OSS endpoint, for example oss-cn-hangzhou-internal.aliyuncs.com")
    parser.add_argument("--jobs", type=int, default=int(os.getenv("OSS_LOAD_JOBS", "32")))
    parser.add_argument("--update", action="store_true", help="Skip files that are already up to date")
    parser.add_argument("--force", action="store_true", help="Allow destination under a non-mounted filesystem")
    parser.add_argument("--dry-run", action="store_true", help="Print actions without mounting or copying")
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    if not args.oss.startswith("oss://"):
        fail("--oss must start with oss://")
    if args.jobs <= 0:
        fail("--jobs must be greater than 0")

    find_executable(["df"], dry_run=args.dry_run)
    if args.mount_source:
        find_executable(["mount"], dry_run=args.dry_run)
        mount_cpfs(args)

    ossutil_bin = find_executable(["ossutil", "ossutil64"], dry_run=args.dry_run)
    cpfs_dst = Path(args.cpfs).expanduser().resolve()
    parent = planned_parent(cpfs_dst, args)

    if not is_mount_point(parent):
        root = filesystem_root(parent)
        if not args.force and root == "/":
            fail(
                "destination parent is on root filesystem, not a CPFS mount. "
                "Mount CPFS first, pass --mount-source, or pass --force."
            )
        log(f"destination parent is not itself a mount point; detected filesystem root: {root}")

    log(f"OSS source: {args.oss}")
    log(f"CPFS destination: {cpfs_dst}")
    log(f"ossutil: {ossutil_bin}")
    print_disk_usage(parent)

    if not args.dry_run:
        cpfs_dst.mkdir(parents=True, exist_ok=True)
    run(build_ossutil_command(args, ossutil_bin), dry_run=args.dry_run)
    log("load completed")


if __name__ == "__main__":
    main()
