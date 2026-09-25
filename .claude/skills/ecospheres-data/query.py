"""Read-only SQL runner for the ecospheres dashboard database.

Usage:
    .venv/bin/python .claude/skills/ecospheres-data/query.py "SELECT ..."
    .venv/bin/python .claude/skills/ecospheres-data/query.py -f some.sql
    echo "SELECT ..." | .venv/bin/python .claude/skills/ecospheres-data/query.py
    ... -o result.csv | -o result.json   (write the full result, print a preview)

Connects to the local restore of the prod database (`make restore_prod`), or to
$ANALYTICS_DATABASE_URL when set. The session is read-only with a statement timeout.
"""

import argparse
import csv
import json
import os
import sys
from datetime import date, datetime
from decimal import Decimal

import psycopg2

URL_VAR = "ANALYTICS_DATABASE_URL"
DEFAULT_URL = "postgresql://postgres:postgres@localhost:5432/dashboard_backend_prod"


def to_jsonable(value):
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    return value


def print_table(columns: list[str], rows: list[tuple], max_rows: int, max_width: int = 60):
    shown = rows[:max_rows]
    cells = [[str(to_jsonable(v)) if v is not None else "" for v in row] for row in shown]
    cells = [[c if len(c) <= max_width else c[: max_width - 1] + "…" for c in row] for row in cells]
    widths = [max([len(col)] + [len(row[i]) for row in cells]) for i, col in enumerate(columns)]
    print(" | ".join(col.ljust(w) for col, w in zip(columns, widths)))
    print("-+-".join("-" * w for w in widths))
    for row in cells:
        print(" | ".join(c.ljust(w) for c, w in zip(row, widths)))
    suffix = f", showing first {max_rows}" if len(rows) > max_rows else ""
    print(f"({len(rows)} rows{suffix})")


def write_output(path: str, columns: list[str], rows: list[tuple]):
    if path.endswith(".json"):
        data = [{c: to_jsonable(v) for c, v in zip(columns, row)} for row in rows]
        with open(path, "w") as f:
            json.dump(data, f, ensure_ascii=False, indent=1)
    else:
        with open(path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(columns)
            writer.writerows([[to_jsonable(v) for v in row] for row in rows])
    print(f"wrote {len(rows)} rows to {path}")


def run_psycopg(dsn: str, sql: str, timeout: int) -> tuple[list[str], list[tuple]]:
    options = f"-c default_transaction_read_only=on -c statement_timeout={timeout * 1000}"
    try:
        conn = psycopg2.connect(dsn, options=options)
    except psycopg2.OperationalError as e:
        sys.exit(f"Cannot connect ({e}). Ask the user to start the local database or restore it.")
    conn.set_session(readonly=True)
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            if cur.description is None:
                return [], []
            return [d.name for d in cur.description], cur.fetchall()
    except psycopg2.Error as e:
        sys.exit(f"SQL error: {e}")
    finally:
        conn.rollback()
        conn.close()


def main():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument("sql", nargs="?", help="SQL to run (default: -f file or stdin)")
    parser.add_argument("-f", "--file", help="read SQL from this file")
    parser.add_argument("-o", "--output", help="write full result to .csv or .json")
    parser.add_argument("-n", "--max-rows", type=int, default=50, help="rows to print (default 50)")
    parser.add_argument("--timeout", type=int, default=60, help="statement timeout in seconds")
    args = parser.parse_args()

    dsn = os.getenv(URL_VAR) or DEFAULT_URL

    if args.sql:
        sql = args.sql
    elif args.file:
        with open(args.file) as f:
            sql = f.read()
    else:
        sql = sys.stdin.read()

    columns, rows = run_psycopg(dsn, sql, args.timeout)

    if not columns:
        print("(no result set)")
        return
    if args.output:
        write_output(args.output, columns, rows)
    print_table(columns, rows, args.max_rows)


if __name__ == "__main__":
    main()
