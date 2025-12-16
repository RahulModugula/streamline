"""
Streaming watermark lag monitor.

Spark Structured Streaming writes query progress JSON to the checkpoint
directory after each micro-batch. These records include the current
watermark timestamp and the batch processing time.

Watermark lag = wall_clock_time - current_watermark
  - Low lag (< 1 min): healthy — watermark is close to now
  - Medium lag (1-10 min): acceptable — within the declared 10-min tolerance
  - High lag (> 10 min): problem — watermark has stalled or job is overwhelmed

This monitor reads the most recent progress file per checkpoint and
reports lag per streaming query. Suitable for cron-based alerting.

Usage:
    python watermark_monitor.py [--warn-minutes 10] [--crit-minutes 30]
    python watermark_monitor.py --json
"""

import argparse
import json
import logging
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

log = logging.getLogger("watermark-monitor")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

CHECKPOINT_BASE_DEFAULT = "/tmp/streamline-checkpoints"


@dataclass
class WatermarkStatus:
    query_name: str
    watermark: Optional[datetime]
    batch_id: int
    batch_duration_ms: int
    lag_minutes: float
    status: str  # "ok" | "warn" | "crit" | "unknown"

    def to_dict(self) -> dict:
        return {
            "query": self.query_name,
            "watermark": self.watermark.isoformat() if self.watermark else None,
            "batch_id": self.batch_id,
            "batch_duration_ms": self.batch_duration_ms,
            "lag_minutes": round(self.lag_minutes, 2),
            "status": self.status,
        }


def read_latest_progress(checkpoint_dir: Path) -> Optional[dict]:
    """
    Read the most recent streaming query progress JSON from a checkpoint dir.
    Spark writes these to <checkpoint>/sources/0/ or directly in the dir.
    Also checks for a 'progress' subdirectory used by some Spark versions.
    """
    # Try common locations Spark uses for query progress
    candidates = [
        checkpoint_dir / "sources" / "0",
        checkpoint_dir / "progress",
        checkpoint_dir,
    ]

    for search_dir in candidates:
        if not search_dir.exists():
            continue
        json_files = list(search_dir.glob("*.json"))
        if json_files:
            latest = max(json_files, key=lambda f: f.stat().st_mtime)
            try:
                return json.loads(latest.read_text())
            except Exception:
                continue

    # Fallback: look for the offsets directory and infer from batch IDs
    offsets_dir = checkpoint_dir / "offsets"
    if offsets_dir.exists():
        offset_files = [f for f in offsets_dir.iterdir() if f.name.isdigit()]
        if offset_files:
            latest_batch = max(int(f.name) for f in offset_files)
            return {"id": latest_batch, "timestamp": None, "eventTime": {}, "durationMs": {}}

    return None


def parse_watermark(progress: dict) -> Optional[datetime]:
    """
    Extract the event-time watermark from a query progress record.
    The watermark is stored under eventTime.watermark in ISO8601 format.
    """
    event_time = progress.get("eventTime", {})
    if not event_time:
        return None

    wm_str = event_time.get("watermark")
    if not wm_str:
        return None

    try:
        # Spark uses "yyyy-MM-ddTHH:mm:ss.SSSZ" or similar
        wm_str = wm_str.replace("Z", "+00:00")
        return datetime.fromisoformat(wm_str)
    except Exception:
        return None


def check_query(
    checkpoint_dir: Path,
    warn_minutes: float,
    crit_minutes: float,
    now: datetime,
) -> WatermarkStatus:
    name = checkpoint_dir.name
    progress = read_latest_progress(checkpoint_dir)

    if not progress:
        return WatermarkStatus(name, None, -1, 0, 0.0, "unknown")

    batch_id = progress.get("id", -1)
    duration_ms = sum(progress.get("durationMs", {}).values()) if isinstance(
        progress.get("durationMs"), dict
    ) else 0

    watermark = parse_watermark(progress)
    if watermark is None:
        return WatermarkStatus(name, None, batch_id, duration_ms, 0.0, "unknown")

    lag_seconds = (now - watermark).total_seconds()
    lag_minutes = lag_seconds / 60

    if lag_minutes <= warn_minutes:
        status = "ok"
    elif lag_minutes <= crit_minutes:
        status = "warn"
    else:
        status = "crit"

    return WatermarkStatus(name, watermark, batch_id, duration_ms, lag_minutes, status)


def main() -> None:
    parser = argparse.ArgumentParser(description="Streaming watermark lag monitor")
    parser.add_argument("--checkpoint-base", default=CHECKPOINT_BASE_DEFAULT)
    parser.add_argument("--warn-minutes", type=float, default=10.0,
                        help="Lag threshold for WARN status")
    parser.add_argument("--crit-minutes", type=float, default=30.0,
                        help="Lag threshold for CRIT status")
    parser.add_argument("--json", action="store_true", dest="as_json")
    args = parser.parse_args()

    base = Path(args.checkpoint_base)
    if not base.exists():
        log.error("checkpoint base not found: %s", base)
        sys.exit(2)

    checkpoint_dirs = [d for d in base.iterdir() if d.is_dir()]
    if not checkpoint_dirs:
        log.warning("no checkpoint directories found in %s", base)
        sys.exit(0)

    now = datetime.now(timezone.utc)
    statuses = [
        check_query(d, args.warn_minutes, args.crit_minutes, now)
        for d in sorted(checkpoint_dirs)
    ]

    if args.as_json:
        print(json.dumps([s.to_dict() for s in statuses], indent=2))
    else:
        print(f"\n{'Query':<30} {'Batch':>7} {'Lag (min)':>10} {'Status'}")
        print("─" * 65)
        for s in statuses:
            icon = {"ok": "✓", "warn": "⚠", "crit": "✗", "unknown": "?"}.get(s.status, "?")
            wm_str = s.watermark.strftime("%H:%M:%S") if s.watermark else "N/A"
            print(
                f"{s.query_name:<30} {s.batch_id:>7} {s.lag_minutes:>9.1f}m  "
                f"{icon} {s.status.upper()}"
                + (f"  (watermark={wm_str})" if s.watermark else "")
            )

    worst = max((s.status for s in statuses),
                key=lambda s: {"ok": 0, "unknown": 1, "warn": 2, "crit": 3}.get(s, 0))
    if worst == "crit":
        sys.exit(2)
    elif worst == "warn":
        sys.exit(1)


if __name__ == "__main__":
    main()
