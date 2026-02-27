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

DB_CONFIG = "dbname=hft_db user=rishiduggal"

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
    impliedVol DOUBLE PRECISION NOT NULL,
    delta DOUBLE PRECISION NOT NULL,
    optPrice DOUBLE PRECISION NOT NULL,
    pvDividend DOUBLE PRECISION NOT NULL,
    gamma DOUBLE PRECISION NOT NULL,
    vega DOUBLE PRECISION NOT NULL,
    theta DOUBLE PRECISION NOT NULL,
    undPrice DOUBLE PRECISION NOT NULL,
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
    event_timestamp
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

QUERY = """
SELECT *
FROM option_orders
ORDER BY event_timestamp DESC;
"""
da = [(-1,"Starter", "Starter", "20000101", -1, "C", -1, -1, -1, -1, -1, -1, -2, "Starter", -1, -1, -1, -1, -1, -1, -1, -1,time.perf_counter_ns())] * 1

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
    add(da)
    pull()