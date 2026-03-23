from store_data import pull
import psycopg
import time
from datetime import datetime
import pandas as pd
import time
import numpy as np

DB_CONFIG = "dbname=hft_db user=arcwilbo"

SUB_QUERY = """
    SELECT *
    FROM option_orders
    ORDER BY event_timestamp DESC
    LIMIT 100000;
    """

def pull_subset_pd(): # -> pd.DataFrame
    """
    Pulls Data from SQL 
    
    Output: DF
    """

    # Connects to DB
    conn = psycopg.connect(DB_CONFIG)
    df = pd.read_sql(SUB_QUERY, conn)
    conn.close()

    
    print(f"\nDF Shape: {df.shape}\n")

    return df




def create_OMM_pd(df: pd.DataFrame): # -> tuple[pd.DataFrame, pd.DataFrame]
    """
    Input: Dirty DF from data accumulation 

    Note: tickSize is called independently and after each tickPrice

    Columns
        -   id: enumerate index number
        -   secType 
            i) "STK" 
            ii) "OPT"
        -   reqId
        -   ticker
        -   exchange
            i)  OPT: "ARCA"
            ii) STK: "SMART"
        -   option_exp: Experation Date of Option Contract
        -   strike
        -   option_right
            i)  "C": Call 
            ii) "P": Put
        -   position: level on order book 0-9 (where 0 is the best bid/ask level)
        -   operation
            i)   0: Intial values
            ii)  1: Update values
            iii) 2: Delete values
        -   side
            i)   0: Ask 
            ii)  1: Bid
            iii) 2: Last
        -   price
        -   size
        -   time: Human Readable time
        -   event_timestamp: Nanosecond precision of unix for received ts of data

    Output: Clean X, y for ML or DL

    Feautures: 
        -   STK: Current mid price
        -   STK: std of price in last min / hr -> proxy of Historical Volatility
        -   STK: Volume of last minute

        -   OPT: Right (Call / put)
        -   OPT: std ask price in last min -> Proxy of Implied Volatility
        -   OPT: Volume of last min

        -   Type of OPT (American / European)
        -   Strike Price
        -   Time to exp in minutes
    """

    # df_STK = df[df['isoption']==0]
    print(df.info())

    # print(df.info())

    # print(df.head())

    # df['STK_mid_price'] = df['']

    return df, df
    


if __name__ == "__main__":
    
    # There are 1+ dummy lines

    df = pull_subset_pd()
    X, y = create_OMM_pd(df)

    unix = time.time()
    nano_unix = time.time_ns()

    print(f"UNIX: {unix}")
    print(f"NANO UNIX: {nano_unix}")

    print(f"difference: {nano_unix - unix * 1_000_000_000}")

    """"
    IDEAS 
    
    1. Possibly track the same strike the whole day
    2. Find a function to get Top of Book MKT data for options too

    """