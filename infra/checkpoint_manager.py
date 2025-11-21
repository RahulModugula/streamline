"""
Spark streaming checkpoint manager.

Checkpoints are critical: they store Kafka offsets and streaming state.
Deleting the wrong checkpoint kills exactly-once guarantees and causes
reprocessing from the earliest available Kafka offset.

This tool provides safe operations:
  inspect — show what's in a checkpoint (latest offsets, batch ID)
  reset   — delete a checkpoint with a mandatory confirmation prompt
  list    — show all checkpoints and their sizes

Usage:
    python checkpoint_manager.py list
    python checkpoint_manager.py inspect --name pushevent
    python checkpoint_manager.py reset --name pushevent --confirm
"""

import argparse
import json
import logging
import os
import shutil
import sys
from pathlib import Path
from typing import Optional

log = logging.getLogger("checkpoint-manager")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

CHECKPOINT_BASE = os.getenv("CHECKPOINT_DIR", "/tmp/streamline-checkpoints")


def dir_size_mb(path: Path) -> float:
    return sum(f.stat().st_size for f in path.rglob("*") if f.is_file()) / 1_048_576


def list_checkpoints(base: str) -> None:
    base_path = Path(base)
    if not base_path.exists():
        print(f"Checkpoint directory not found: {base}")
        return

    checkpoints = [d for d in base_path.iterdir() if d.is_dir()]
    if not checkpoints:
        print("No checkpoints found.")
        return

    print(f"\n{'Checkpoint':<30} {'Size (MB)':>12} {'Path'}")
    print("─" * 80)
    for cp in sorted(checkpoints):
        size_mb = dir_size_mb(cp)
        print(f"{cp.name:<30} {size_mb:>11.1f}  {cp}")


def inspect_checkpoint(base: str, name: str) -> None:
    cp_path = Path(base) / name
    if not cp_path.exists():
        log.error("checkpoint not found: %s", cp_path)
        sys.exit(1)

    print(f"\nCheckpoint: {cp_path}")
    print(f"Size: {dir_size_mb(cp_path):.1f} MB")

    # Read the latest committed batch ID from offsets/
    offsets_dir = cp_path / "offsets"
    if offsets_dir.exists():
        offset_files = sorted(offsets_dir.iterdir(), key=lambda f: int(f.name) if f.name.isdigit() else -1)
        if offset_files:
            latest = offset_files[-1]
            print(f"\nLatest committed batch: {latest.name}")
            try:
                content = latest.read_text()
                # Spark offset files have a JSON header after the first line
                lines = content.strip().split("\n")
                if len(lines) >= 2:
                    offsets = json.loads(lines[1])
                    print("Kafka offsets:")
                    for topic, partitions in offsets.items():
                        for partition, offset in partitions.items():
                            print(f"  {topic}[{partition}] = {offset}")
            except Exception as e:
                log.warning("could not parse offset file: %s", e)

    # Check for metadata file
    metadata_file = cp_path / "metadata"
    if metadata_file.exists():
        print(f"\nMetadata: {metadata_file.read_text().strip()}")


def reset_checkpoint(base: str, name: str, confirm: bool) -> None:
    cp_path = Path(base) / name
    if not cp_path.exists():
        log.error("checkpoint not found: %s", cp_path)
        sys.exit(1)

    size_mb = dir_size_mb(cp_path)
    print(f"\n⚠️  WARNING: Deleting checkpoint '{name}' ({size_mb:.1f} MB)")
    print("This will cause the streaming job to restart from the latest Kafka offset.")
    print("Any unprocessed messages between the last checkpoint and now may be skipped.")

    if not confirm:
        print("\nRe-run with --confirm to proceed.")
        sys.exit(0)

    response = input("\nType the checkpoint name to confirm deletion: ").strip()
    if response != name:
        print("Confirmation did not match. Aborting.")
        sys.exit(1)

    shutil.rmtree(cp_path)
    log.info("checkpoint deleted: %s", cp_path)


def main() -> None:
    parser = argparse.ArgumentParser(description="Spark checkpoint manager")
    sub = parser.add_subparsers(dest="action", required=True)

    sub.add_parser("list")

    inspect_p = sub.add_parser("inspect")
    inspect_p.add_argument("--name", required=True)
    inspect_p.add_argument("--base", default=CHECKPOINT_BASE)

    reset_p = sub.add_parser("reset")
    reset_p.add_argument("--name", required=True)
    reset_p.add_argument("--base", default=CHECKPOINT_BASE)
    reset_p.add_argument("--confirm", action="store_true",
                         help="Required to actually delete (prevents accidents)")

    args = parser.parse_args()

    if args.action == "list":
        list_checkpoints(CHECKPOINT_BASE)
    elif args.action == "inspect":
        inspect_checkpoint(args.base, args.name)
    elif args.action == "reset":
        reset_checkpoint(args.base, args.name, args.confirm)


if __name__ == "__main__":
    main()
