from store_data import pull
import psycopg
import time
from datetime import datetime
import pandas as pd
import time
import numpy as np
import matplotlib.pyplot as plt
from typing import Dict, List

"""
ts = ns unix (UTC)
"""

DB_CONFIG = "dbname=hft_db user=arcwilbo"

SUB_QUERY = """
    SELECT *
    FROM option_orders
    ORDER BY event_timestamp ASC;
    """
# LIMIT 100000;

class Security:
    
    def __init__(self, ticker: str, exp: str = None, strike: float = None, right: str = None): 
        """
        OPT_Security Attributes
        """

        self.ticker = ticker
        self.exp = exp
        self.strike = strike
        self.right = right
    
    def __str__(self): 
        return f"({self.ticker}: {self.right} at {self.strike} on {self.exp})"

class Data_Analysis: 

    def __init__(self): 
        """
        Data_Analysis attribute
        """
        self.main_df: pd.DataFrame = self.pull_subset_and_sort_pd()
        self.OPT_Security_list: List[Security] = self.find_all_unique_Contracts(df = self.main_df, secType="OPT")
        self.STK_Security_list: List[Security] = self.find_all_unique_Contracts(df = self.main_df, secType="STK")
    
    def pull_subset_and_sort_pd(self): # -> pd.DataFrame
        """
        Explanantion:
            -   Pulls Data from SQL 
            -   Makes ts the ns datetime index
            -   sorts based of index
        
        Output: DF
        """

        # Connects to DB
        conn = psycopg.connect(DB_CONFIG)
        df = pd.read_sql(SUB_QUERY, conn)
        conn.close()

        # No data
        if (df.empty): 
            raise ValueError("SQL Query returned nothing")

        # Sort and set index as ts (dtype = pd.datetime)
        df['event_timestamp'] = pd.to_datetime(df['event_timestamp'], unit = 'ns')
        df.set_index('event_timestamp', inplace=True)
        df.sort_index(inplace=True)

        print(f"\nDF Shape: {df.shape}\n")
        # print(df.columns)

        return df

    def find_all_unique_Contracts(self, df: pd.DataFrame, secType: str): # -> List[Security]
            """
            Input: 
                -   Master df
                -   secType: "STK" or "OPT
            Output: 
                -   List[Security]: All unique secType Contracts in Master df
            """

            if (secType != "STK" and secType != "OPT"):
                raise ValueError("Paramter secType must be STK or OPT")
            
            temp_master_list: List[Security] = [] 
            
            # Filter to secType only
            sec_df = df[self.main_df["sectype"] == secType]

            # STK Unique
            if (secType == "STK"): 
                
                STK_unique_df = sec_df[["ticker"]].drop_duplicates()
                
                for _, row in STK_unique_df.iterrows(): 
                            
                    temp: Security = Security(ticker = row['ticker'])
                    
                    temp_master_list.append(temp)
            
            # OPT Unique
            else: 
                
                OPT_unique_df = sec_df[["ticker", "option_exp", "strike", "option_right"]].drop_duplicates()

                for _, row in OPT_unique_df.iterrows(): 
                                
                    temp: Security = Security(ticker = row['ticker'], exp = row['option_exp'], strike = row['strike'], right = row['option_right'])
                    
                    temp_master_list.append(temp)
            
            return temp_master_list

    def df_STK_order_book(self, master_df: pd.DataFrame, sec: Security): # -> pd.DataFrame
        """
        Input: 
            -   master_df: SQL data  
            -   sec: Security
        Ouput
            -   all values for all bid-ask levels at a specific ts

        Note
            -   Operation = 0 for first 20 then 1
            -   How to Handle (Best Bid > Best Ask)
        """
        # Extract Security
        ticker = sec.ticker
        
        # Filter to only L2 STK Data
        df = master_df[(master_df['sectype'] == "STK") & (master_df['ticker'] == ticker) & (master_df['side'] != 2)]

        data = []

        template_val = {} 

        prev_val = {}

        add_temp: bool = True

        for idx, row in df.iterrows():

            # Creating Book 
            if (row['operation'] == 0): 
                
                template_val['time'] = row['time']
                template_val['event_timestamp'] = idx
                template_val['ticker'] = row['ticker']
                template_val['sectype'] = row['sectype']

                bid_or_ask = "ask" if row['side'] == 0 else "bid"
                template_val[f"{bid_or_ask}_size_{row['position']}"] = row['size']
                template_val[f"{bid_or_ask}_price_{row['position']}"] = row['price']

            # Updating Book
            elif (row['operation'] == 1):
                
                # Add the finished template_val to data
                if (add_temp): 
                    data.append(template_val)
                    add_temp = False
                    prev_val = template_val.copy()

                # Copy most reent values and update: time, ts, and 1 bid-ask levels

                bid_or_ask = "ask" if row['side'] == 0 else "bid"
                prev_val[f"{bid_or_ask}_size_{row['position']}"] = row['size']
                prev_val[f"{bid_or_ask}_price_{row['position']}"] = row['price']

                prev_val['time'] = row['time']
                prev_val['event_timestamp'] = idx

                # Prints if BID >= ASK
                if (data[-1]["bid_price_0"] >= data[-1]["ask_price_0"]):
                    print(f"Bid: {data[-1]['bid_price_0']}, Ask: {data[-1]['ask_price_0']} @ {data[-1]['time']}")

                # Only Push the row to Data if BID < ASK
                if (prev_val["bid_price_0"] < prev_val["ask_price_0"]):
                    data.append(prev_val.copy())

        # No data
        if (not data): 
            return pd.Dataframe()
        
        # Yes data
        STK_df = pd.DataFrame(data)
        STK_df["event_timestamp"] = pd.to_datetime(STK_df["event_timestamp"], unit = "ns")
        STK_df.set_index("event_timestamp", inplace = True)
        STK_df.sort_index(inplace = True)

        return STK_df

    def df_OPT_order_book(self, master_df: pd.DataFrame, OPT_sec: Security): # -> pd.DataFrame
        """
        Input: 
            -   master_df
            -   ticker
            -   exp: OPT Expiration date
            -   strike 
            -   right: "C" or "P"
        Output: 
            -   pd.DataFrame: Clean df order book per ts
        """

        # Local Copies of OPT_sec: Security Attributes
        ticker = OPT_sec.ticker 
        exp = OPT_sec.exp
        strike = OPT_sec.strike
        right = OPT_sec.right

        # Filter to only L1 Specific OPT Data
        df = master_df[(master_df['sectype'] == "OPT") & (master_df['ticker'] == ticker) & (master_df['side'] != 2) & (master_df['option_exp'] == exp) & (master_df['strike'] == strike) & (master_df['option_right'] == right)]

        data = []

        template_val = {"sectype": "OPT", "ticker": ticker, "option_exp": exp, "strike": strike, "option_right": right, "bid_price": None, "bid_size": 0, "ask_price": None, "ask_size": 0, "time": None, "event_timestamp": None} 

        for idx, row in df.iterrows(): 

            # Update Time based cols
            template_val["event_timestamp"] = idx
            template_val["time"] = row["time"]

            # Update Ask
            if (row["side"] == 0): 
                
                # Update Size
                if (row["size"] != -1):
                    
                    template_val["ask_size"] = row["size"]

                # Update Price & set Size to 0
                else: 

                    template_val["ask_price"] = row["price"]
                    template_val["ask_size"] = 0

            # Update Bid
            elif (row["side"] == 1): 

                # Update Size
                if (row["size"] != -1):
                    
                    template_val["bid_size"] = row["size"]

                # Update Price & set Size to 0
                else: 

                    template_val["bid_price"] = row["price"]
                    template_val["bid_size"] = 0
            
            # Add to data
            if ((template_val["bid_size"] != 0) and (template_val["ask_size"] != 0) and (template_val["bid_price"]) and (template_val["ask_price"]) and (template_val["bid_price"] < template_val["ask_price"])):
                data.append(template_val.copy())
        
        # No data
        if (not data): 
            return pd.DataFrame()
        
        # Yes data
        OPT_df = pd.DataFrame(data, columns = ["OPT", "ticker", "option_exp", "strike", "option_right", "bid_price", "bid_size", "ask_price", "ask_size","time", "event_timestamp"])
        
        # Datetime + index
        OPT_df['event_timestamp'] = pd.to_datetime(OPT_df['event_timestamp'], unit ='ns')
        OPT_df.set_index('event_timestamp', inplace = True)
        OPT_df.sort_index(inplace = True)
        # print("OPT", OPT_df.shape)
        
        return OPT_df
    
    def df_Trades(self, master_df: pd.DataFrame, sec: Security): # -> pd.DataFrame
        """
        Input: 
            -   master_df: Uncleaned SQL pulled data 
            -   sec: Security
        
        Output: 
            -   df of Trades

        Notes: 
            -   Price 
                i) update first
                ii) No two consecutive entries
            -   Size 
                i) Update second 
                ii) Contains many duplicates but also some different sizes between two price updates
        """
        
        # Unpacking sec
        secType = "STK" if (sec.strike is None) else "OPT"
        ticker = sec.ticker
        option_strike = sec.strike
        option_exp = sec.exp
        option_right = sec.right

        # Filter Master df to sec
        df = master_df[(master_df['ticker'] == ticker) & (master_df['side'] == 2) & (master_df['sectype'] == secType)]
        
        if (secType == "OPT"):
            
            df = df[(df['strike'] == option_strike) & (df['option_exp'] == option_exp) & (df['option_right'] == option_right)]

        # 2D List of Data to create pd.DataFrame
        data = [] 

        template_val = {'secType': secType, 'ticker': ticker, 'price': None, 'size': 0, 'time': None, 'event_timestamp': None, 'option_exp': option_exp, 'strike': option_strike, 'option_right': option_right}
        
        # sum up the size and once there is a new price, flush and update
        for idx, row in df.iterrows(): 

            # Update Time cols
            template_val['time'] = row['time']
            template_val['event_timestamp'] = idx
            
            # New Price
            if (row['price'] != -1):

                # If there is size
                if (template_val['size'] != 0): 
                    
                    data.append(template_val.copy())
                
                # Update price and reset size
                template_val['price'] = row['price']
                template_val['size'] = 0

            # New Size
            elif (row['size'] != -1):

                template_val['size'] += row['size']
        
        # Flush last trade
        if (template_val['size'] != 0): 
            data.append(template_val.copy())
        
        # No data
        if (not data): 
            return pd.DataFrame()
        
        # Yes data
        df_t = pd.DataFrame(data, columns=['secType', 'ticker', 'price', 'size', 'time', 'event_timestamp', 'option_exp', 'strike', 'option_right'])
        
        # Convert to datetime ts and set index
        df_t['event_timestamp'] = pd.to_datetime(df_t['event_timestamp'], unit = 'ns')
        df_t.set_index('event_timestamp', inplace = True)

        return df_t

    def STK_Volume_last_min(self, ts: int, df: pd.DataFrame): # -> int
        """
        Input: 
            -   ts: Time stamp you want summation of previous STK volume from last minute
            -   df: DataFrame of already filtered STK size and 
        Ouput 
            -   None if empty else sum
        """
        # Filter for last minute
        df = df[(df.index >= ts - 60 * 1_000_000_000) & (df.index <= ts)]
        
        if (df.empty): 
            return None
        
        return df['size'].sum()

    def create_OMM_pd(self, ticker: str, df: pd.DataFrame): # -> tuple[pd.DataFrame, pd.DataFrame]
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

        Conceptually: 
            -   (Target) y: The theo price for the next ts of that OPT secruity — exp, strike, right —
        """

        return df
    
    def create_OMM_df(self, master_df: pd.DataFrame, OPT_sec_list: List[Security], STK_sec: Security): # -> pd.DataFrame
        """
        Input: 
            -   master_df: SQL data
            -   OPT_sec_list: List[Security]
            -   STK_sec: Security, the solo STK sec
        Output: 
            -   X:
                1) strike
                2) min_to_exp - time to expiry
                3) isCall - binary variable (1 = C, 0 = P)
                4) OPT_vol_60s
                5) OPT_spread
                6) OPT_target_std_60s
                7) STK_vol_60s
                8) STK_mid_price
                9) STK_spread
                10) strike_STK_residual
                11) Target - Midprice of OPT bid and ask
        
        Note: 
            -   Ouput is not sorted
        """
        
        # Empty sec list
        if (len(OPT_sec_list) == 0): 
            raise ValueError("There are no Securities in OPT_sec_list")

        dfs = []
        
        # Constant STK Book and Trade df
        STK_book_df = self.df_STK_order_book(master_df=master_df, sec=STK_sec)
        if (STK_book_df.empty): 
            raise ValueError(f"No STK Book Data for {STK_sec}")
        
        STK_trades_df = self.df_Trades(master_df= master_df, sec= STK_sec)
        if (STK_trades_df.empty): 
            raise ValueError(f"No STK Trade Data for {STK_sec}")

        # OPT list for only STK ticker 
        ticker_OPT_sec_list = [sec for sec in OPT_sec_list if sec.ticker == STK_sec.ticker]
        # Loop each sec in List[sec]
        for OPT_sec in ticker_OPT_sec_list: 

            OPT_book_df = self.df_OPT_order_book(master_df= master_df, OPT_sec= OPT_sec)
            if (OPT_book_df.empty): 
                raise ValueError(f"No OPT Book Data for {OPT_sec}")

            OPT_trades_df = self.df_Trades(master_df= master_df, sec= OPT_sec)
            if (OPT_trades_df.empty): 
                raise ValueError(f"No OPT Trade Data for {OPT_sec}")
            
            # Filter final ts > min_ts
            min_ts: pd.Timestamp = OPT_book_df.index[0]

            ###### START Target ######
            
            # OPT Mid Price
            OPT_book_df['Target'] = round((OPT_book_df['bid_price'] + OPT_book_df['ask_price']) / 2,2)

            ###### END Target ######

            ###### START OPT Features ######

            ## min_to_exp - minutes till expiry 
            exp_dt = pd.to_datetime(OPT_book_df['option_exp'], format="%Y%m%d") + pd.Timedelta(hours=16, minutes=15)
            OPT_book_df['min_to_exp'] = round((exp_dt - OPT_book_df.index).dt.total_seconds() / 60)

            ## isCall
            OPT_book_df['isCall'] = (OPT_book_df['option_right'] == "C").astype(int)

            ## OPT_vol_60s - OPT Volume last min
            min_ts = max(min_ts, OPT_trades_df.index[0] + pd.Timedelta(seconds=60))
            OPT_trades_df['OPT_vol_60s'] = round(OPT_trades_df['size'].rolling('60s').sum())
            OPT_book_df = pd.merge_asof(left=OPT_book_df, right=OPT_trades_df[['OPT_vol_60s']], left_index=True, right_index=True, direction="backward")

            ## OPT_spread - OPT Spread between bid and ask
            OPT_book_df['OPT_spread'] = round(OPT_book_df['ask_price'] - OPT_book_df['bid_price'],2)

            ## OPT_target_std_60s - std of OPT midprice in the last 60s
            OPT_book_df['OPT_target_std_60s'] = round(OPT_book_df['Target'].rolling('60s').std(),2)

            ###### END OPT Features ######

            ###### START STK Features ######

            # idx filter
            min_ts = max(min_ts, STK_trades_df.index[0] + pd.Timedelta(seconds=60))
            
            ## STK_vol_60s - STK Volume last min
            STK_trades_df['STK_vol_60s'] = round(STK_trades_df['size'].rolling('60s').sum())
            
            ## STK_mid_price - STK Mid Price
            STK_book_df['STK_mid_price'] = round((STK_book_df['bid_price_0'] + STK_book_df['ask_price_0']) / 2,2)
            
            ## STK_spread
            STK_book_df['STK_spread'] = round(STK_book_df['ask_price_0'] - STK_book_df['bid_price_0'],2)
            
            # Merge STK_trades_df to OPT_book_df
            OPT_book_df = pd.merge_asof(left=OPT_book_df, right=STK_trades_df[['STK_vol_60s']], left_index=True, right_index=True, direction="backward")

            # Merge STK_book_df to OPT_book_df
            OPT_book_df = pd.merge_asof(left=OPT_book_df, right=STK_book_df[['STK_mid_price', 'STK_spread']], left_index=True, right_index=True, direction="backward")

            ## strike_STK_residual
            OPT_book_df['strike_STK_residual'] = round(OPT_book_df['strike'] - OPT_book_df['STK_mid_price'],2)

            ###### END STK Features ######

            # Filter df
            OPT_book_df = OPT_book_df[OPT_book_df.index > min_ts]

            # DL Features
            OPT_book_df = OPT_book_df[['min_to_exp', 'isCall', 'OPT_vol_60s', 'OPT_spread', 'OPT_target_std_60s', 'STK_vol_60s', 'STK_mid_price', 'STK_spread', 'strike_STK_residual', 'Target']]

            # Drop rows with NaN
            OPT_book_df = OPT_book_df.dropna()

            dfs.append(OPT_book_df)
        
        # Return Logic
        if (len(dfs) == 0): 
            
            return None
        
        else: 

            # No index
            df = pd.concat(dfs).sort_index()
            df = df.drop_duplicates()
            return df
        

if __name__ == "__main__":

    # Create Instance
    Data_Analysis_Object = Data_Analysis()

    df_analyze = Data_Analysis_Object.create_OMM_df(master_df= Data_Analysis_Object.main_df, OPT_sec_list= Data_Analysis_Object.OPT_Security_list, STK_sec= Data_Analysis_Object.STK_Security_list[0])

    df_analyze.to_csv("X.csv", index= True)
    
    # print(df_analyze.info())
    # print(df_analyze.describe())
    # print(df_analyze.corr().to_csv("df_analyze_corr.csv"))