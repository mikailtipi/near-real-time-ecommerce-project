"""
data_quality/dq_check.py
------------------------
Runs data quality checks on raw schema tables and prints
a colored summary table to terminal. Results are also saved
to raw.dq_results for Airflow monitoring.

Usage:
  python dq_check.py                    # check all tables
  python dq_check.py --table orders     # check one table
  python dq_check.py --fail-fast        # exit 1 on first failure (CI use)
"""

import argparse
import os
import sys
from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional

import psycopg2
from psycopg2.extras import execute_batch

RESET  = "\033[0m"
BOLD   = "\033[1m"
RED    = "\033[91m"
GREEN  = "\033[92m"
YELLOW = "\033[93m"
CYAN   = "\033[96m"
GRAY   = "\033[90m"

DB_CONFIG = {
    "host":     os.getenv("DB_HOST", "localhost"),
    "port":     int(os.getenv("DB_PORT", 5432)),
    "dbname":   os.getenv("DB_NAME", "ecommerce"),
    "user":     os.getenv("DB_USER", "pipeline"),
    "password": os.getenv("DB_PASSWORD", "pipeline"),
}

CHECKS = {
    "orders": [
        {"check": "null_order_id",        "sql": "SELECT COUNT(*) FROM raw.orders WHERE order_id IS NULL"},
        {"check": "null_customer_id",     "sql": "SELECT COUNT(*) FROM raw.orders WHERE customer_id IS NULL"},
        {"check": "null_status",          "sql": "SELECT COUNT(*) FROM raw.orders WHERE order_status IS NULL"},
        {"check": "null_purchase_ts",     "sql": "SELECT COUNT(*) FROM raw.orders WHERE order_purchase_ts IS NULL"},
        {"check": "duplicate_order_id",   "sql": "SELECT COUNT(*) FROM (SELECT order_id FROM raw.orders GROUP BY order_id HAVING COUNT(*) > 1) t"},
        {"check": "future_purchase_ts",   "sql": "SELECT COUNT(*) FROM raw.orders WHERE order_purchase_ts > NOW()"},
        {"check": "approved_before_purchase", "sql": "SELECT COUNT(*) FROM raw.orders WHERE order_approved_ts < order_purchase_ts"},
        {"check": "invalid_status",       "sql": "SELECT COUNT(*) FROM raw.orders WHERE order_status NOT IN ('delivered','shipped','processing','cancelled','returned')"},
    ],
    "customers": [
        {"check": "null_customer_id",     "sql": "SELECT COUNT(*) FROM raw.customers WHERE customer_id IS NULL"},
        {"check": "null_city",            "sql": "SELECT COUNT(*) FROM raw.customers WHERE customer_city IS NULL"},
        {"check": "duplicate_customer_id","sql": "SELECT COUNT(*) FROM (SELECT customer_id FROM raw.customers GROUP BY customer_id HAVING COUNT(*) > 1) t"},
    ],
    "products": [
        {"check": "null_product_id",      "sql": "SELECT COUNT(*) FROM raw.products WHERE product_id IS NULL"},
        {"check": "null_category",        "sql": "SELECT COUNT(*) FROM raw.products WHERE category_name IS NULL"},
        {"check": "negative_price",       "sql": "SELECT COUNT(*) FROM raw.products WHERE product_price < 0"},
        {"check": "zero_price",           "sql": "SELECT COUNT(*) FROM raw.products WHERE product_price = 0"},
        {"check": "duplicate_product_id", "sql": "SELECT COUNT(*) FROM (SELECT product_id FROM raw.products GROUP BY product_id HAVING COUNT(*) > 1) t"},
    ],
    "order_items": [
        {"check": "null_order_id",        "sql": "SELECT COUNT(*) FROM raw.order_items WHERE order_id IS NULL"},
        {"check": "null_product_id",      "sql": "SELECT COUNT(*) FROM raw.order_items WHERE product_id IS NULL"},
        {"check": "negative_price",       "sql": "SELECT COUNT(*) FROM raw.order_items WHERE price < 0"},
        {"check": "zero_quantity",        "sql": "SELECT COUNT(*) FROM raw.order_items WHERE quantity <= 0"},
        {"check": "orphan_order_id",      "sql": "SELECT COUNT(*) FROM raw.order_items oi WHERE NOT EXISTS (SELECT 1 FROM raw.orders o WHERE o.order_id = oi.order_id)"},
    ],
}


@dataclass
class CheckResult:
    table:      str
    check:      str
    status:     str        # PASS | WARN | FAIL
    fail_count: int
    row_count:  int
    detail:     str = ""


def get_conn():
    return psycopg2.connect(**DB_CONFIG)


def get_row_count(cur, table: str) -> int:
    cur.execute(f"SELECT COUNT(*) FROM raw.{table}")
    return cur.fetchone()[0]


def run_checks(tables: Optional[List[str]] = None) -> List[CheckResult]:
    conn = get_conn()
    cur  = conn.cursor()
    results = []

    target_tables = tables or list(CHECKS.keys())

    for table in target_tables:
        if table not in CHECKS:
            print(f"{YELLOW}[warn] No checks defined for table '{table}'{RESET}")
            continue

        row_count = get_row_count(cur, table)

        for chk in CHECKS[table]:
            cur.execute(chk["sql"])
            fail_count = cur.fetchone()[0]

            if fail_count == 0:
                status = "PASS"
                detail = ""
            elif fail_count / max(row_count, 1) < 0.01:
                status = "WARN"
                detail = f"{fail_count} rows affected ({fail_count/max(row_count,1)*100:.1f}%)"
            else:
                status = "FAIL"
                detail = f"{fail_count} rows affected ({fail_count/max(row_count,1)*100:.1f}%)"

            results.append(CheckResult(
                table=table,
                check=chk["check"],
                status=status,
                fail_count=fail_count,
                row_count=row_count,
                detail=detail,
            ))

    cur.close()
    conn.close()
    return results


def save_results(results: List[CheckResult]):
    conn = get_conn()
    cur  = conn.cursor()
    rows = [
        (r.table, r.check, r.status, r.detail, r.row_count, r.fail_count)
        for r in results
    ]
    execute_batch(cur, """
        INSERT INTO raw.dq_results (table_name, check_name, status, detail, row_count, fail_count)
        VALUES (%s, %s, %s, %s, %s, %s)
    """, rows)
    conn.commit()
    cur.close()
    conn.close()


def status_color(status: str) -> str:
    return {
        "PASS": f"{GREEN}PASS{RESET}",
        "WARN": f"{YELLOW}WARN{RESET}",
        "FAIL": f"{RED}FAIL{RESET}",
    }.get(status, status)


def print_report(results: List[CheckResult]):
    col_w = [20, 30, 6, 8, 10, 36]
    header = ["TABLE", "CHECK", "STATUS", "ROWS", "FAILURES", "DETAIL"]
    sep    = "─" * (sum(col_w) + len(col_w) * 3 + 1)

    print(f"\n{BOLD}{CYAN}  Data Quality Report  —  {datetime.now():%Y-%m-%d %H:%M:%S}{RESET}")
    print(f"  {sep}")
    print("  " + "  ".join(f"{BOLD}{h:<{w}}{RESET}" for h, w in zip(header, col_w)))
    print(f"  {sep}")

    current_table = None
    for r in results:
        if r.table != current_table:
            if current_table is not None:
                print(f"  {GRAY}{'─'*sum(col_w)}{RESET}")
            current_table = r.table

        status_str = status_color(r.status)
        padding    = 6 + (len(GREEN) + len(RESET) if r.status == "PASS" else
                          len(YELLOW) + len(RESET) if r.status == "WARN" else
                          len(RED) + len(RESET))
        row = (
            f"  {r.table:<{col_w[0]}}  "
            f"{r.check:<{col_w[1]}}  "
            f"{status_str:<{padding}}  "
            f"{r.row_count:<{col_w[3]}}  "
            f"{r.fail_count:<{col_w[4]}}  "
            f"{GRAY}{r.detail:<{col_w[5]}}{RESET}"
        )
        print(row)

    print(f"  {sep}")
    total  = len(results)
    passed = sum(1 for r in results if r.status == "PASS")
    warned = sum(1 for r in results if r.status == "WARN")
    failed = sum(1 for r in results if r.status == "FAIL")
    print(
        f"  {BOLD}Total: {total}{RESET}  "
        f"{GREEN}{passed} passed{RESET}  "
        f"{YELLOW}{warned} warned{RESET}  "
        f"{RED}{failed} failed{RESET}\n"
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Data quality checker")
    parser.add_argument("--table",     help="Check a single table only")
    parser.add_argument("--fail-fast", action="store_true", help="Exit 1 on first failure")
    parser.add_argument("--no-save",   action="store_true", help="Skip saving results to DB")
    args = parser.parse_args()

    tables  = [args.table] if args.table else None
    results = run_checks(tables)

    print_report(results)

    if not args.no_save:
        save_results(results)

    if args.fail_fast and any(r.status == "FAIL" for r in results):
        sys.exit(1)
