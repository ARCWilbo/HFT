import time
from typing import Iterable, Dict, Any, Optional
import psycopg


UPSERT_SQL = """
INSERT INTO option_snapshots
  (ticker, option_symbol, strike, exp, m1, m2, m3, m4, updated_at)
VALUES
  (%(ticker)s, %(option_symbol)s, %(strike)s, %(exp)s, %(m1)s, %(m2)s, %(m3)s, %(m4)s, NOW())
ON CONFLICT (ticker, option_symbol, strike, exp)
DO UPDATE SET
  m1 = EXCLUDED.m1,
  m2 = EXCLUDED.m2,
  m3 = EXCLUDED.m3,
  m4 = EXCLUDED.m4,
  updated_at = NOW();
"""


def get_conn(dsn: str) -> psycopg.Connection:
    """
    dsn example:
      "postgresql://user:password@localhost:5432/mydb"
    """
    return psycopg.connect(dsn)


def upsert_rows(conn: psycopg.Connection, rows: Iterable[Dict[str, Any]]) -> None:
    """
    rows: iterable of dicts with keys:
      ticker, option_symbol, strike, exp, m1, m2, m3, m4
    """
    rows = list(rows)
    if not rows:
        return

    with conn.cursor() as cur:
        cur.executemany(UPSERT_SQL, rows)
    conn.commit()


def periodic_updater(
    dsn: str,
    fetch_rows_fn,
    interval_seconds: int = 10,
    jitter_seconds: float = 0.0,
    run_forever: bool = True,
    max_loops: Optional[int] = None,
) -> None:
    """
    fetch_rows_fn: callable that returns an iterable of row dicts to upsert.
                  This is where you pull from your market data / API.

    interval_seconds: how often to update
    jitter_seconds: optional random-ish delay (you can implement if desired)
    """
    loops = 0
    with get_conn(dsn) as conn:
        while True:
            try:
                rows = fetch_rows_fn()
                upsert_rows(conn, rows)
            except Exception as e:
                # Keep it simple: log and continue
                print(f"[updater] error: {e}")

            loops += 1
            if not run_forever and max_loops is not None and loops >= max_loops:
                break

            time.sleep(interval_seconds)