import psycopg
import time
from datetime import datetime
import pandas as pd
import time

"""
Before Running the File, make sure that you have Postgre SQL and that you have a db created called: 

hft_db

Under your user!

You can use the following commands in the Terminal: 

dropdb hft_db  # to Delete a db
createdb hft_db # to Create a db 

"""

DB_CONFIG = "dbname=hft_db user=arcwilbo"

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS option_orders (
    id BIGSERIAL PRIMARY KEY,
    reqId INTEGER NOT NULL,
    ticker TEXT NOT NULL,
    exchange TEXT NOT NULL,
    option_exp DATE NULL,
    strike DOUBLE PRECISION NULL,
    option_right TEXT,
    isOption INTEGER NOT NULL,
    position INTEGER NOT NULL,
    operation INTEGER NOT NULL,
    side INTEGER NOT NULL,
    price DOUBLE PRECISION NOT NULL,
    size INTEGER NOT NULL,
    tickType INTEGER NOT NULL,
    tickString TEXT NOT NULL,
    impliedVol DOUBLE PRECISION,
    delta DOUBLE PRECISION,
    optPrice DOUBLE PRECISION,
    pvDividend DOUBLE PRECISION,
    gamma DOUBLE PRECISION,
    vega DOUBLE PRECISION,
    theta DOUBLE PRECISION,
    undPrice DOUBLE PRECISION,
    time TEXT NOT NULL,
    event_timestamp BIGINT NOT NULL
);
"""

INSERT_SQL = """
INSERT INTO option_orders (
    reqId,
    ticker,
    exchange,
    option_exp,
    strike,
    option_right,
    isOption,
    position,
    operation,
    side,
    price,
    size, 
    tickType,
    tickString,
    impliedVol, 
    delta, 
    optPrice, 
    pvDividend, 
    gamma, 
    vega, 
    theta, 
    undPrice, 
    time,
    event_timestamp
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

QUERY = """
SELECT *
FROM option_orders
ORDER BY event_timestamp DESC;
"""

TABLE_SHAPE_SQL = """
SELECT
    (SELECT COUNT(*) FROM option_orders) AS rows,
    (SELECT COUNT(*)
     FROM information_schema.columns
     WHERE table_name = 'option_orders') AS columns;
"""

TABLE_SIZE_SQL = """
SELECT
    pg_size_pretty(pg_relation_size('option_orders')) AS table_size,
    pg_size_pretty(pg_indexes_size('option_orders')) AS index_size,
    pg_size_pretty(pg_total_relation_size('option_orders')) AS total_size;
"""

da = [(-1,"Starter", "Starter", "20000101", -1, "C", -1, -1, -1, -1, -1, -1, -2, "Starter", -1, -1, -1, -1, -1, -1, -1, -1,"time", time.perf_counter_ns())] * 1

def table_stats():
    
    conn = psycopg.connect(DB_CONFIG)
    cur = conn.cursor()

    cur.execute(TABLE_SHAPE_SQL)
    rows, cols = cur.fetchone()

    print("\nTABLE SHAPE:")
    print((rows, cols))

    cur.execute(TABLE_SIZE_SQL)
    table_size, index_size, total_size = cur.fetchone()

    print("\nTABLE STORAGE:")
    print("table_size:", table_size)
    print("index_size:", index_size)
    print("total_size:", total_size)

    cur.close()
    conn.close()

def add(data):

    conn = psycopg.connect(DB_CONFIG)
    cur = conn.cursor()

    # 2️⃣ Ensure table exists
    cur.execute(CREATE_TABLE_SQL)
    conn.commit()

    try:

        cur.executemany(INSERT_SQL, data)
        conn.commit()

    except KeyboardInterrupt:
        print("Shutting down cleanly...")

    finally:
        cur.close()
        conn.close()


def pull():
    conn = psycopg.connect(DB_CONFIG)
    df = pd.read_sql(QUERY, conn)
    print(df.head())
    print(df.shape)
    conn.close()
    return df


if __name__ == "__main__":
    # add(da)
    # pull()

    table_stats()