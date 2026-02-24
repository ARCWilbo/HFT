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
    ticker TEXT NOT NULL,
    exchange TEXT NOT NULL,
    option_exp DATE NULL,
    strike DOUBLE PRECISION NULL,
    option_right TEXT,
    position INTEGER NOT NULL,
    operation INTEGER NOT NULL,
    side INTEGER NOT NULL,
    price DOUBLE PRECISION NOT NULL,
    size INTEGER NOT NULL,
    event_timestamp BIGINT NOT NULL
);
"""

INSERT_SQL = """
INSERT INTO option_orders (
    ticker,
    exchange,
    option_exp,
    strike,
    option_right,
    position,
    operation,
    side,
    price,
    size, 
    event_timestamp
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

QUERY = """
SELECT *
FROM option_orders
ORDER BY event_timestamp DESC;
"""
da = [("Starter", "Starter", "20000101", -1, "C", -1, -1, -1, -1, -1, time.perf_counter_ns())] * 1

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