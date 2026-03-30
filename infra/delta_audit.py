"""
Delta Lake audit log reader.

Delta's _delta_log/ directory contains one JSON file per committed transaction.
Each file records: what operation was performed, when, by whom, and statistics
(rows added/removed, files added/removed, bytes written).

This tool parses those files to answer:
  - What operations ran on this table and when?
  - How many rows were written per micro-batch?
  - Which files were added or removed in a given time range?
  - Is the table being written to (last commit < N minutes ago)?

Runs without a Spark cluster — pure Python, no PySpark dependency.
Useful for monitoring, incident investigation, and capacity planning.

Usage:
    python delta_audit.py summary --table pushevent
    python delta_audit.py history --table pushevent --limit 20
    python delta_audit.py staleness --table pushevent --warn-minutes 10
"""

import argparse
import json
import logging
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

log = logging.getLogger("delta-audit")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

OUTPUT_BASE = os.getenv("OUTPUT_DIR", "./data/lake")


@dataclass
class CommitEntry:
    version: int
    timestamp: datetime
    operation: str
    user_metadata: str
    rows_added: int
    rows_removed: int
    files_added: int
    files_removed: int
    bytes_written: int

    def __str__(self) -> str:
        return (
            f"v{self.version:>6}  {self.timestamp.strftime('%Y-%m-%d %H:%M:%S')}  "
            f"{self.operation:<20}  +{self.rows_added:>8} rows  "
            f"{self.files_added:>4} files added  {self.bytes_written // 1024:>8} KB"
        )


def parse_commit(path: Path) -> Optional[CommitEntry]:
    """Parse a single Delta transaction log JSON file."""
    version = int(path.stem)
    rows_added = rows_removed = files_added = files_removed = bytes_written = 0
    operation = ""
    timestamp = None
    user_metadata = ""

    try:
        with path.open() as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                entry = json.loads(line)

                if "commitInfo" in entry:
                    ci = entry["commitInfo"]
                    operation = ci.get("operation", "UNKNOWN")
                    user_metadata = ci.get("userMetadata", "")
                    ts_ms = ci.get("timestamp", 0)
                    timestamp = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
                    metrics = ci.get("operationMetrics", {})
                    rows_added += int(metrics.get("numOutputRows", 0))
                    rows_removed += int(metrics.get("numDeletedRows", 0))
                    bytes_written += int(metrics.get("numOutputBytes", 0))

                if "add" in entry:
                    files_added += 1
                    stats_str = entry["add"].get("stats", "{}")
                    try:
                        stats = json.loads(stats_str)
                        rows_added = max(rows_added, int(stats.get("numRecords", 0)))
                    except Exception:
                        pass

                if "remove" in entry:
                    files_removed += 1

        if timestamp is None:
            return None

        return CommitEntry(
            version=version,
            timestamp=timestamp,
            operation=operation,
            user_metadata=user_metadata,
            rows_added=rows_added,
            rows_removed=rows_removed,
            files_added=files_added,
            files_removed=files_removed,
            bytes_written=bytes_written,
        )
    except Exception as e:
        log.warning("failed to parse %s: %s", path, e)
        return None


def load_commits(table_path: str, limit: Optional[int] = None) -> list[CommitEntry]:
    """Load commits from a Delta table's _delta_log directory."""
    log_dir = Path(table_path) / "_delta_log"
    if not log_dir.exists():
        log.error("_delta_log not found at %s", log_dir)
        return []

    json_files = sorted(log_dir.glob("*.json"), key=lambda p: int(p.stem), reverse=True)
    if limit:
        json_files = json_files[:limit]

    commits = []
    for f in json_files:
        entry = parse_commit(f)
        if entry:
            commits.append(entry)

    return sorted(commits, key=lambda c: c.version, reverse=True)


def cmd_history(table: str, limit: int) -> None:
    path = f"{OUTPUT_BASE}/{table}"
    commits = load_commits(path, limit)

    if not commits:
        print(f"No commits found for table '{table}' at {path}")
        return

    print(f"\nDelta History: {table}  ({len(commits)} commits shown)")
    print("─" * 90)
    print(f"{'Version':>8}  {'Timestamp':>20}  {'Operation':<20}  {'Rows Added':>10}  {'Files':>6}  {'Size':>10}")
    print("─" * 90)
    for c in commits:
        print(c)


def cmd_summary(table: str) -> None:
    path = f"{OUTPUT_BASE}/{table}"
    commits = load_commits(path)

    if not commits:
        print(f"No commits found for '{table}'")
        return

    total_rows = sum(c.rows_added for c in commits)
    total_bytes = sum(c.bytes_written for c in commits)
    ops = {}
    for c in commits:
        ops[c.operation] = ops.get(c.operation, 0) + 1

    oldest = min(commits, key=lambda c: c.version)
    newest = max(commits, key=lambda c: c.version)

    print(f"\nTable Summary: {table}")
    print(f"  Path:          {path}")
    print(f"  Total commits: {len(commits)}")
    print(f"  Total rows:    {total_rows:,}")
    print(f"  Total bytes:   {total_bytes / 1_048_576:.1f} MB")
    print(f"  First commit:  v{oldest.version} @ {oldest.timestamp}")
    print(f"  Last commit:   v{newest.version} @ {newest.timestamp}")
    print("  Operations:")
    for op, count in sorted(ops.items(), key=lambda x: -x[1]):
        print(f"    {op:<25} {count:>5}×")


def cmd_staleness(table: str, warn_minutes: int) -> None:
    path = f"{OUTPUT_BASE}/{table}"
    commits = load_commits(path, limit=1)

    if not commits:
        print(f"UNKNOWN: no commits found for '{table}'")
        sys.exit(2)

    latest = commits[0]
    age_seconds = (datetime.now(timezone.utc) - latest.timestamp).total_seconds()
    age_minutes = age_seconds / 60

    status = "OK" if age_minutes <= warn_minutes else "STALE"
    print(
        f"{status}: table={table} last_commit=v{latest.version} "
        f"age={age_minutes:.1f}m threshold={warn_minutes}m"
    )
    if status == "STALE":
        sys.exit(1)


def main() -> None:
    parser = argparse.ArgumentParser(description="Delta Lake audit log reader")
    sub = parser.add_subparsers(dest="cmd", required=True)

    hist = sub.add_parser("history", help="Show commit history")
    hist.add_argument("--table", required=True)
    hist.add_argument("--limit", type=int, default=20)

    summ = sub.add_parser("summary", help="Summarise table statistics")
    summ.add_argument("--table", required=True)

    stale = sub.add_parser("staleness", help="Alert if table hasn't been written recently")
    stale.add_argument("--table", required=True)
    stale.add_argument("--warn-minutes", type=int, default=10)

    args = parser.parse_args()

    if args.cmd == "history":
        cmd_history(args.table, args.limit)
    elif args.cmd == "summary":
        cmd_summary(args.table)
    elif args.cmd == "staleness":
        cmd_staleness(args.table, args.warn_minutes)


if __name__ == "__main__":
    main()
