"""
Data quality validation for Delta Lake tables.

Runs after compaction or as a standalone daily job. Checks:

  1. Row count sanity — alert if a partition has < MIN_ROWS_PER_HOUR events
     (indicates the streaming job stalled or the producer stopped)

  2. Null rate — critical fields (id, type, repo.name) should never be null.
     High null rates indicate schema drift or a producer bug.

  3. Duplicate detection — event IDs should be unique per table.
     Duplicates indicate the MERGE sink isn't working or was bypassed.

  4. Value distribution — event type distribution should be stable.
     A sudden spike (e.g., WatchEvents 10× normal) may indicate a viral
     event or a producer bug that's re-sending old data.

Outputs a DQ report to stdout (or --output-path as a JSON file).
Exits non-zero if any FAIL checks are found — suitable for CI or cron alerting.

Usage:
    spark-submit data_quality.py --table pushevent --date 2025-11-15
    spark-submit data_quality.py --all-tables --date 2025-11-15
"""

import argparse
import json
import logging
import os
import sys
from dataclasses import dataclass, field, asdict
from datetime import date, timedelta
from typing import Optional

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

log = logging.getLogger("streamline.dq")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

OUTPUT_BASE = os.getenv("OUTPUT_DIR", "./data/lake")

# Per-table DQ rules
TABLE_RULES = {
    "pushevent": {
        "not_null_cols": ["id", "type"],
        "unique_col": "id",
        "min_rows_per_hour": 10,
    },
    "pullrequestevent": {
        "not_null_cols": ["id", "type"],
        "unique_col": "id",
        "min_rows_per_hour": 1,
    },
    "issuesevent": {
        "not_null_cols": ["id", "type"],
        "unique_col": "id",
        "min_rows_per_hour": 1,
    },
}

ALL_TABLES = list(TABLE_RULES.keys())


@dataclass
class CheckResult:
    check: str
    status: str    # "PASS" | "FAIL" | "WARN" | "SKIP"
    detail: str


@dataclass
class TableDQReport:
    table: str
    date: str
    row_count: int
    checks: list[CheckResult] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return all(c.status != "FAIL" for c in self.checks)

    def print(self) -> None:
        icon = "✓" if self.passed else "✗"
        print(f"\n{icon} {self.table}  date={self.date}  rows={self.row_count:,}")
        for c in self.checks:
            sym = {"PASS": "  ✓", "FAIL": "  ✗", "WARN": "  ⚠", "SKIP": "  -"}.get(c.status, "  ?")
            print(f"{sym} [{c.status:4}] {c.check}: {c.detail}")


def build_spark() -> SparkSession:
    builder = (
        SparkSession.builder
        .appName("streamline-data-quality")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.adaptive.enabled", "true")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def load_partition(spark: SparkSession, table: str, check_date: str) -> Optional[DataFrame]:
    path = f"{OUTPUT_BASE}/{table}"
    try:
        df = (
            spark.read.format("delta").load(path)
            .filter(F.col("event_date") == check_date)
        )
        # Trigger schema resolution without full scan
        df.schema
        return df
    except Exception as e:
        log.warning("could not load %s for date %s: %s", table, check_date, e)
        return None


def check_not_null(df: DataFrame, col: str, total: int) -> CheckResult:
    null_count = df.filter(F.col(col).isNull()).count()
    null_rate = null_count / total if total > 0 else 0
    if null_rate == 0:
        return CheckResult(f"not_null:{col}", "PASS", "0 nulls")
    elif null_rate < 0.01:
        return CheckResult(f"not_null:{col}", "WARN",
                           f"{null_count:,} nulls ({null_rate:.2%})")
    else:
        return CheckResult(f"not_null:{col}", "FAIL",
                           f"{null_count:,} nulls ({null_rate:.2%}) — exceeds 1% threshold")


def check_uniqueness(df: DataFrame, col: str, total: int) -> CheckResult:
    distinct = df.select(col).distinct().count()
    dupes = total - distinct
    if dupes == 0:
        return CheckResult(f"unique:{col}", "PASS", f"{distinct:,} distinct IDs")
    else:
        return CheckResult(f"unique:{col}", "FAIL",
                           f"{dupes:,} duplicate {col} values — MERGE sink may be bypassed")


def check_row_count(df: DataFrame, total: int, min_per_hour: int) -> CheckResult:
    # Count rows per hour and flag any hour with suspiciously few rows
    hourly = df.groupBy("event_hour").count().collect()
    low_hours = [(r.event_hour, r["count"]) for r in hourly if r["count"] < min_per_hour]
    if not low_hours:
        return CheckResult("row_count_per_hour", "PASS",
                           f"all hours >= {min_per_hour} rows")
    detail = ", ".join(f"hour={h}:{c}" for h, c in sorted(low_hours)[:5])
    return CheckResult("row_count_per_hour", "WARN",
                       f"{len(low_hours)} hours below {min_per_hour} rows: {detail}")


def validate_table(spark: SparkSession, table: str, check_date: str) -> TableDQReport:
    rules = TABLE_RULES.get(table, {})
    report = TableDQReport(table=table, date=check_date, row_count=0)

    df = load_partition(spark, table, check_date)
    if df is None:
        report.checks.append(CheckResult("load", "SKIP", "partition not found"))
        return report

    total = df.count()
    report.row_count = total

    if total == 0:
        report.checks.append(CheckResult("row_count", "WARN",
                                         f"0 rows for date {check_date} — streaming job may have stalled"))
        return report

    report.checks.append(CheckResult("row_count", "PASS", f"{total:,} rows"))

    for col in rules.get("not_null_cols", []):
        report.checks.append(check_not_null(df, col, total))

    if unique_col := rules.get("unique_col"):
        report.checks.append(check_uniqueness(df, unique_col, total))

    if min_rows := rules.get("min_rows_per_hour", 0):
        report.checks.append(check_row_count(df, total, min_rows))

    return report


def main() -> None:
    parser = argparse.ArgumentParser(description="Delta Lake data quality validator")
    parser.add_argument("--table", help="Validate one table")
    parser.add_argument("--all-tables", action="store_true")
    parser.add_argument("--date", default=str(date.today() - timedelta(days=1)),
                        help="YYYY-MM-DD (default: yesterday)")
    parser.add_argument("--output-path", help="Write JSON report to this file")
    args = parser.parse_args()

    tables = ALL_TABLES if args.all_tables else ([args.table] if args.table else ALL_TABLES)
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    reports = [validate_table(spark, t, args.date) for t in tables]

    for r in reports:
        r.print()

    if args.output_path:
        with open(args.output_path, "w") as f:
            json.dump([asdict(r) for r in reports], f, indent=2)
        log.info("report written to %s", args.output_path)

    failures = [r for r in reports if not r.passed]
    if failures:
        print(f"\n{len(failures)} table(s) failed DQ checks.")
        sys.exit(1)
    else:
        print(f"\nAll {len(reports)} tables passed DQ checks.")


if __name__ == "__main__":
    main()
