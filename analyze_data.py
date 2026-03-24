from store_data import pull
import psycopg
import time
from datetime import datetime
import pandas as pd
import time
import numpy as np
import matplotlib.pyplot as plt

DB_CONFIG = "dbname=hft_db user=arcwilbo"

SUB_QUERY = """
    SELECT *
    FROM option_orders
    ORDER BY event_timestamp ASC
    LIMIT 1000000;
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

def df_Trades(secType: str, ticker: str, df: pd.DataFrame, option_strike: int = None, option_exp: str = "", option_right: str = ""): # -> tuple[pd.DataFrame, pd.DataFrame]
    """
    Input: 
        -   secType: STK or OPT
        -   ticker: SPY, QQQ, DIA
        -   df: full df
    
    Output: 
        -   df of prices 
        -   df of sizes

    Notes: 
        -   Price 
            i) update first
            ii) No two consecutive entries
        -   Size 
            i) Update second 
            ii) Contains many duplicates but also some different sizes between two price updates
    """

    if (secType == "STK"):
        df = df[(df['ticker'] == ticker) & (df['side'] == 2) & (df['sectype'] == secType)]
    elif (secType == "OPT"):
        df = df[(df['ticker'] == ticker) & (df['side'] == 2) & (df['sectype'] == secType) & (df['strike'] == option_strike) & (df['option_exp'] == option_exp) & (df['option_right'] == option_right)]
    else: 
        raise ValueError("Did not select STK or OPT as secType")

    # 2D List of Data to create pd.DataFrame
    data = [] 

    price = -1
    size_sum = 0

    vals = []
    for idx, row in df.iterrows(): 
        # sum up the size and once the new price is pushed, flush and update
        
        if (price == -1 and row['price'] != -1): # First Price

            # Initialize Values
            price = row['price']
            vals = [row['sectype'], row['ticker'], 0, 0, row['time'], row['event_timestamp'], row['option_exp'], row['strike'], row['option_right']]
        
        elif (price != -1 and row['size'] != -1):
            size_sum += row['size']

        elif (price != -1 and row['price'] != -1): # new price
            
            # add the missing values
            vals[2] = price 
            vals[3] = size_sum 
            data.append(vals)

            # Reset values
            price = row['price']
            size_sum = 0     
            vals = [row['sectype'], row['ticker'], 0, 0, row['time'], row['event_timestamp'], row['option_exp'], row['strike'], row['option_right']]   

    df_t = pd.DataFrame(data, columns=['secType', 'ticker', 'price', 'size', 'time', 'event_timestamp', 'option_exp', 'strike', 'option_right'])
    return df_t


def STK_Volume_last_min(ts: int, df: pd.DataFrame): # -> int
    """
    Input: 
        -   ts: Time stamp you want summation of previous STK volume from last minute
        -   df: DataFrame of already filtered STK size and 
    """

    return df[(df['event_timestamp'] >= ts - 60 * 1_000_000_000) & (df['event_timestamp'] <= ts)]['size'].sum()


def create_OMM_pd(df: pd.DataFrame): # -> tuple[pd.DataFrame, pd.DataFrame]
    """
    Input: Dirty DF from data accumulation 

    Note: tickSize is called independently and after each tickPrice

    Columns
        -   id: enumerate index number
        -   sectype 
            i) "STK" 
            ii) "OPT"
        -   reqid
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

        -   Strike Price
        -   Time to exp in minutes
    """
    Data = pd.DataFrame(columns = ["STK_mid_price", "STK_std_price", "STK_volume", "OPT_right", "OPT_std_price", "OPT_volume", "Strike_price", "time_to_exp"])

    # for idx, row in df.iterrows(): 
    #     row[]


    return df, df
    


if __name__ == "__main__":
    
    # There are 1+ dummy lines

    df = pull_subset_pd()

    print(df.info())
    
    SPY_OPT_Trades = df_Trades(secType = "OPT", ticker = "SPY", df = df, option_strike = 653.0 , option_exp = "20260324", option_right = "C" )
    SPY_STK_Trades = df_Trades(secType = "STK", ticker = "SPY", df = df)
    
    print(SPY_STK_Trades.describe())

    # Graphing
    # plt.figure(figsize=(16,5))
    plt.plot(SPY_STK_Trades['price'])
    # plt.show()
    
    
    # SPY_OPT_Trade_Prices, SPY_OPT_Trade_Sizes = Trades("OPT", "SPY", df)

    # print(SPY_STK_Trade_Prices.describe())

    # print("\n--------------------------------\n")

    # print(SPY_STK_Trade_Sizes.describe())
    # X, y = create_OMM_pd(df)

    # unix = time.time()
    # nano_unix = time.time_ns()

    # print(f"UNIX: {unix}")
    # print(f"NANO UNIX: {nano_unix}")

    # print(f"difference: {nano_unix - unix * 1_000_000_000}")

    """"
    IDEAS 
    
    1. Possibly track the same strike the whole day
    2. Find a function to get Top of Book MKT data for options too

    """