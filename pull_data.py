from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
from ibapi.order import Order 
from ibapi.contract import ComboLeg
from ibapi.tag_value import TagValue
import threading
from threading import Event, Thread, Semaphore, Lock
from ibapi.common import BarData
import pandas as pd
from datetime import datetime, date, timedelta, time, timezone
import time as py_time
from collections import deque
import warnings
from queue import Queue
import numpy as np
import json
from zoneinfo import ZoneInfo

from store_data import add, pull # other .py file

# Mute Warnings
warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings("ignore", category=UserWarning)
warnings.filterwarnings("ignore", message="Choices for a categorical distribution.*")
warnings.filterwarnings("ignore", category=FutureWarning, message=".*weights_only=False.*")

# ================= IBKR Connection =================

stop_event = Event()

TickType= {
    0: "BidSize", 
    1: "BidPrice", 
    2: "AskPrice", 
    3: "AskSize", 
    4: "LastPrice (Price of Most Recent Trade)", 
    5: "LastSize (Volume of Most Recent Trade)", 
    6: "High (Highest Price of the Day)", 
    7: "Low (Lowest Price of the Day)", 
    8: "Volume (Daily)",
    32: "BidExchange (Exchange Posting the Best Bid Price)",
    33: "AskExchange (Exchange Posting the Best Ask Price)",
    45: "LastTimeStamp (Time of Last Trade in Unix)"
}

class TestApp(EClient, EWrapper):
    """ 
    IBKR Connection to place requests through TWS API
    """

    def __init__(self):
        EClient.__init__(self, self)

        # Order ID
        self.order_id = None 
        self.order_id_lock = Lock()

        # Contract Event
        self.contract_lock = Lock()
        self.contract_event = Event()
        self.contract_dict = {} 

        # Options
        self.options_lock = Lock()
        self.option_chain_event = Event()
        self.ticker_to_conId = {}
        self.conId_option_chain = {} # contains another dictionary with keys: "exp" and "strike"

        # Historical data
        self.historical_data_event = Event()
        self.conId_to_price = {}
        self.intermediate_prices = []

        # Options Meta Data 
        self.options_meta_data = {}

        # Stroing Data in PSQL
        self.tick_data_collection_lock = Lock()
        self.wake_db_worker = Event()
        self.tick_data_collection = []
        self.last_push_in_sec = int(py_time.time())
        self.already_pushed = 1

        # L2 STK Tick Data
        self.reqId_to_L2_STK_Contract = {}

        # L1 STK Tick-By-Tick Data (No OPT)
        self.reqId_to_stock_tick_by_tick_contract = {}

        # L1 OPT Top-Of-Book
        self.reqId_to_L1_Option_Contract = {}

        # L1 Historical Top-Of-Book
        self.reqId_to_Historical_option_contract = {}

        


    def nextValidId(self, orderId: int):
        """ 
        Retrieves initial valid Order ID needed for every IBKR API Call
        """

        self.order_id = orderId
        print(f"Next valid order ID received: {self.order_id}")

    def get_positions(self):
        """
        Requests all open positions
        """

        self.positions = []  
        self.positions_ready.clear()
        self.reqPositions()  
        self.positions_ready.wait(timeout=10)
        return self.positions

    def position(self, account, contract, position, avgCost):
        """
        Handles each position returned from IBKR
        """

        pos_info = {
            # "account": account,
            "symbol": contract.symbol,
            "conId": contract.conId,
            # "localSymbol": contract.localSymbol,
            "secType": contract.secType,
            "currency": contract.currency,
            "exchange": contract.exchange,
            # "primaryExchange": contract.primaryExchange,
            # "tradingClass": contract.tradingClass,
            "multiplier": contract.multiplier,
            # "lastTradeDateOrContractMonth": contract.lastTradeDateOrContractMonth,
            "position": position,
            "avgCost": avgCost,
            # Optionally:
            # "marketPrice": current_price,
            # "marketValue": current_price * position,
            # "unrealizedPnL": (current_price - avgCost) * position
        }
        if abs(pos_info['position']) > 1e-6:
            self.positions.append(pos_info)
    
    def positionEnd(self):
        """
        End Signal for Position Request 
        """

        print("All positions received:")
        for p in self.positions:
            print(f"{p} \n")
        self.positions_ready.set() 

    def create_stock_contract(self, symbol: str, exchange: str = "SMART"): # -> Contract
        """
        Creates a contract for a specified stock symbol
        """

        contract = Contract()
        contract.symbol = symbol
        contract.secType = "STK"
        contract.exchange = exchange
        contract.currency = "USD"
        return contract
    
    def create_opt_contract(self, symbol: str, strike: float, exp: str, right: str, exchange: str): # -> Contract
        """
        Creates an IBKR Option Contract.

        exp format: 'YYYYMMDD' (e.g. '20260320')
        right: 'C' for Call, 'P' for Put
        """

        contract = Contract()
        contract.symbol = symbol
        contract.secType = "OPT"
        contract.exchange = exchange # "PSE", "NASDAQOM", "SMART"
        contract.currency = "USD"

        contract.lastTradeDateOrContractMonth = exp
        contract.strike = strike
        contract.right = right
        contract.multiplier = "100"

        return contract

    def place_limit_order(self, symbol, limit_price, quantity=1):
        """
        Places a simple limit order for the specified symbol
        """

        if self.order_id is None:
            print("Error: Next valid order ID has not been received yet.")
            return

        contract = self.create_stock_contract(symbol)

        with self.order_id_lock:
            order_id = self.order_id
            self.order_id += 1

        myorder = Order()
        myorder.orderId = order_id
        myorder.action = "BUY"
        myorder.orderType = "LMT"
        myorder.lmtPrice = limit_price
        myorder.totalQuantity = quantity
        myorder.tif = "GTC"  
        myorder.eTradeOnly = ''  
        myorder.firmQuoteOnly = ''

        self.placeOrder(order_id, contract, myorder)
        print(f"Placed limit order for {symbol} at {limit_price}")
    
    def place_market_order(self, symbol, quantity=1, action = "BUY"):
        """
        Places a simple market order for the specified symbol
        """

        if self.order_id is None:
            print("Error: Next valid order ID has not been received yet.")
            return

        contract = self.create_stock_contract(symbol)

        with self.order_id_lock:
            order_id = self.order_id
            self.order_id += 1

        myorder = Order()
        myorder.orderId = order_id
        myorder.action = action
        myorder.orderType = "MKT"
        myorder.totalQuantity = quantity
        myorder.tif = "GTC"  
        myorder.eTradeOnly = ''  
        myorder.firmQuoteOnly = '' 

        self.placeOrder(order_id, contract, myorder)


    def place_bracket_order(self, symbol, position, entry_price, take_profit_price, stop_loss_price, quantity=1):
        """
        Places a bracket order (Limit Entry Order, Limit Stop Loss Order, Limit Take Profit Order) for the specified symbol
        """

        if self.order_id is None:
            print("Error: Next valid order ID has not been received yet.")
            return

        contract = self.create_stock_contract(symbol)

        with self.order_id_lock:
            order_id = self.order_id
            self.order_id += 3

        parent_order = Order()
        parent_order.orderId = order_id
        if position == "Long":
            parent_order.action = "BUY"
        else: 
            parent_order.action = "SELL"
        parent_order.orderType = "LMT"
        parent_order.lmtPrice = entry_price
        parent_order.totalQuantity = quantity
        parent_order.tif = "DAY"
        parent_order.transmit = False  
        parent_order.eTradeOnly = ''  
        parent_order.firmQuoteOnly = ''  

       
        take_profit_order = Order()
        take_profit_order.orderId = order_id + 1
        if position == "Long":
            take_profit_order.action = "SELL"
        else: 
            take_profit_order.action = "BUY"
        take_profit_order.orderType = "LMT"
        take_profit_order.lmtPrice = take_profit_price
        take_profit_order.totalQuantity = quantity
        take_profit_order.parentId = parent_order.orderId  
        take_profit_order.transmit = False
        take_profit_order.eTradeOnly = ''
        take_profit_order.firmQuoteOnly = ''

        stop_loss_order = Order()
        stop_loss_order.orderId = order_id + 2
        if position == "Long":
            stop_loss_order.action = "SELL"
        else: 
            stop_loss_order.action = "BUY"
        stop_loss_order.orderType = "STP"
        stop_loss_order.auxPrice = stop_loss_price
        stop_loss_order.totalQuantity = quantity
        stop_loss_order.parentId = parent_order.orderId  
        stop_loss_order.transmit = True 
        stop_loss_order.eTradeOnly = ''
        stop_loss_order.firmQuoteOnly = ''

        self._fill_tracker[order_id] = position
        self._fill_tracker[order_id + 1] = "Exit"
        self._fill_tracker[order_id + 2] = "Exit"

        
        self.placeOrder(parent_order.orderId, contract, parent_order)
        self.placeOrder(take_profit_order.orderId, contract, take_profit_order)
        self.placeOrder(stop_loss_order.orderId, contract, stop_loss_order)
        print(f"Placed bracket {position.upper()} order for {symbol} with entry of {quantity} shares @ ${entry_price}, take profit at {take_profit_price}, and stop loss at {stop_loss_price} with order_IDs {order_id} - {order_id +2}")


    def place_combo_order(self, symbol1, conId1, symbol2, conId2, quantity1=1, quantity2=1, action1="BUY", action2="SELL"):
        """
        Places a combo order with two legs, each identified by its conID
        """

        if self.order_id is None:
            print("Error: Next valid order ID has not been received yet.")
            return

        combo_contract = Contract()
        combo_contract.symbol = symbol1
        combo_contract.secType = "BAG"
        combo_contract.currency = "USD"
        combo_contract.exchange = "SMART"

        leg1 = ComboLeg()
        leg1.conId = conId1
        leg1.ratio = quantity1
        leg1.action = action1
        leg1.exchange = "SMART"

        leg2 = ComboLeg()
        leg2.conId = conId2
        leg2.ratio = quantity2
        leg2.action = action2
        leg2.exchange = "SMART"

        combo_contract.comboLegs = [leg1, leg2]

        with self.order_id_lock:
            order_id = self.order_id
            self.order_id +=1

        combo_order = Order()
        combo_order.orderId = order_id
        combo_order.action = "BUY"
        combo_order.orderType = "MKT"  
        combo_order.totalQuantity = min(quantity1, quantity2)  
        combo_order.eTradeOnly = ''  
        combo_order.firmQuoteOnly = ''  

        self.placeOrder(order_id, combo_contract, combo_order)
        print(f"Placed combo order with {symbol1} ({action1}, {quantity1}) and {symbol2} ({action2}, {quantity2})")
        
    ##################### ---------------- New Code ---------------- #####################

    ## Level 2 Market Data
    def req_opt_L2(self, ticker): # -> None
        """
        Starting L2 Tick Data STK Stream

        Note: 
            -   IB only suports 3 MKT Depth Streams
        """
       
        print(f"establishing request for L2: {ticker} on Exchange: PSE")

        with self.order_id_lock:
            order_id_1 = self.order_id
            self.order_id +=1
        
        self.reqId_to_L2_STK_Contract[order_id_1] = {"ticker": ticker, "exchange": "ARCA"}
        stock_contract = self.create_stock_contract(ticker, exchange="ARCA")

        self.reqMktDepth(order_id_1, stock_contract, 10, False, [])
        

    def updateMktDepth(self, reqId, position: int, operation: int, side: int, price: float, size):
        # Triggered by: SPY, QQQ, IWM 

        # Time Stamp Record Keeping
        received_ts = py_time.time_ns()
        ts = py_time.time()
        dt = datetime.fromtimestamp(ts)
        human_readable_ts = dt.strftime("%m/%d/%Y %H:%M:%S")

        reqId, size = int(reqId), float(size)

        data_now = (
            "STK",
            reqId,
            self.reqId_to_L2_STK_Contract[reqId]["ticker"],
            self.reqId_to_L2_STK_Contract[reqId]["exchange"], 
            "", 
            0,
            "",
            position, # position
            operation, # Operation
            side, # side
            price, # price
            size, # size
            human_readable_ts,
            received_ts
        )

        with self.tick_data_collection_lock: 
            
            self.tick_data_collection.append(data_now)

            curr_unix_time = int(py_time.time())
            if (curr_unix_time >  self.last_push_in_sec + 30): 
                self.wake_db_worker.set()
                self.last_push_in_sec = curr_unix_time

        # print("UpdateMarketDepth. ReqId:", reqId, "Position:", position, "Operation:", operation, "Side:", side, "Price:", price, "Size:", size)
            
    def updateMktDepthL2(self, reqId, position: int, marketMaker: str, operation: int, side: int, price: float, size, isSmartDepth: bool):
        reqId, size = int(reqId), float(size)
        print("UpdateMarketDepthL2. ReqId:", reqId, "Position:", position, "MarketMaker:", marketMaker, "Operation:", operation, "Side:", side, "Price:", price, "Size:", size, "isSmartDepth:", isSmartDepth)
    
    def shutdown_l2_opt_connection(self):

        for reqId, v in self.reqId_to_L2_STK_Contract.items(): 
            self.cancelMktDepth(reqId, False)
        
        print("ALL L2 (STK) Streams Closed")
    
    ##! Level 2 Market Data

    ## Option Chain
    def request_option_chain(self, ticker): 

        with self.order_id_lock:
            order_id = self.order_id
            self.order_id += 1
        
        contract = self.Req_Contract_details(ticker)

        self.option_chain_event.clear()
        self.reqSecDefOptParams(order_id, ticker, "", "STK", contract.contract.conId)
        self.option_chain_event.wait(10)

        conId = self.ticker_to_conId[ticker]

        if conId in self.conId_option_chain:

            sorted_exp = sorted(self.conId_option_chain[conId]["exp"])
            sorted_strikes = sorted(self.conId_option_chain[conId]["strike"])

            self.conId_option_chain[conId]["exp"] = sorted_exp
            self.conId_option_chain[conId]["strike"] = sorted_strikes

    def securityDefinitionOptionParameter(self, reqId: int, exchange: str, underlyingConId: int, tradingClass: str, multiplier: str, expirations, strikes):
        #print("SecurityDefinitionOptionParameter.", "ReqId:", reqId, "Exchange:", exchange, "Underlying conId:", underlyingConId, "TradingClass:", tradingClass, "Multiplier:", multiplier, "Expirations:", expirations, "Strikes:", strikes)
        
        # print(underlyingConId)
        if (underlyingConId not in self.conId_option_chain):
            self.conId_option_chain[underlyingConId] = {
            "exp": set(),
            "strike": set()
            }

        self.conId_option_chain[underlyingConId]["exp"].update(expirations)
        self.conId_option_chain[underlyingConId]["strike"].update(strikes)


    def securityDefinitionOptionParameterEnd(self, reqId: int):
        self.option_chain_event.set()

    ##! Option Chain

    ## Req Contract Details
    def Req_Contract_details(self, ticker): 

        with self.order_id_lock:
            order_id = self.order_id
            self.order_id += 1
        
        stock_contract = self.create_stock_contract(ticker)

        self.contract_event.clear()
        self.reqContractDetails(order_id, stock_contract)
        self.contract_event.wait(timeout=10)

        if (ticker not in self.ticker_to_conId):
            with self.contract_lock:
                contract = self.contract_dict[order_id]
                self.ticker_to_conId[ticker] = contract.contract.conId
        
        # print("Self", self.ticker_to_conId[ticker])
        
        return contract
    
    def contractDetails(self, reqId: int, contractDetails):
        
        with self.contract_lock:
            self.contract_dict[reqId] = contractDetails
        
        # print(reqId, contractDetails)
    
    def contractDetailsEnd(self, reqId: int):
        # print("ContractDetailsEnd. ReqId:", reqId)
        self.contract_event.set() 

    ##! Req Contract Details
    
    ## Current Asset Price 
    def req_historical_price(self, ticker): 

        with self.order_id_lock:
            order_id = self.order_id
            self.order_id += 1
        
        contract = self.create_stock_contract(ticker)
        contract.conId = self.ticker_to_conId[ticker]

        self.historical_data_event.clear()
        self.reqHistoricalData(order_id, contract, "", "1 W", "1 day", "TRADES", 0, 1, False, [])
        self.historical_data_event.wait(10)

        # Sort Data Histrical Bars and get most recent Price Close
        df = pd.DataFrame(self.intermediate_prices)
        # print(df)
        df["date"] = pd.to_datetime(df["date"])
        df.sort_values("date", ascending=False, inplace=True)
        self.conId_to_price[self.ticker_to_conId[ticker]] = df.iloc[0]["close"]
        self.intermediate_prices = []

    def historicalData(self, reqId:int, bar: BarData):
        # print("HistoricalData. ReqId:", reqId, "BarData.", bar)

        self.intermediate_prices.append({
            "date": bar.date,
            "open": bar.open,
            "high": bar.high,
            "low": bar.low,
            "close": bar.close,
            "volume": bar.volume
        })
    
    def historicalDataEnd(self, reqId: int, start: str, end: str):
        # print("HistoricalDataEnd. ReqId:", reqId, "from", start, "to", end)
        self.historical_data_event.set()

    ##! Current Asset Price 

    ## Saving Option Meta Data
    def create_options_metadata(self, ticker: str): # -> None: 

        # print(self.conId_option_chain[self.ticker_to_conId[ticker]])

        price = self.conId_to_price[self.ticker_to_conId[ticker]]

        ticker_strike = []
        last = 0
        
        ints = [x for x in self.conId_option_chain[self.ticker_to_conId[ticker]]['strike'] if (x%1==0)]

        for p in ints:
            
            if (p > price): 
                ticker_strike.append(last)
                ticker_strike.append(p)
                break

            last = p

        # today = datetime.today()

        # days_until_friday = (4 - today.weekday()) % 7
        
        # if days_until_friday == 0:
        #     days_until_friday = 7 

        # next_friday = today + timedelta(days=days_until_friday)
        # # next_next_friday = next_friday + timedelta(weeks=1)

        # next_friday = next_friday.strftime("%Y%m%d")

        # exp1 = False
        # option_exps = []
        # for exp in self.conId_option_chain[self.ticker_to_conId[ticker]]['exp']:
        #     if (exp1): 
        #         option_exps.append(str(exp))
        #         break

        #     if int(exp) >= int(next_friday): 
        #         exp1 = True
        #         option_exps.append(str(exp))

        option_exps = self.conId_option_chain[self.ticker_to_conId[ticker]]['exp'][:2]
        
        self.options_meta_data[ticker] = {"conId": self.ticker_to_conId[ticker], "price": price, "strike": ticker_strike, "exp": option_exps, "date": str(date.today())}
        print(self.options_meta_data[ticker])

    def save_options(self): 
        with open("options_meta_data.json", "w") as f:
            json.dump(self.options_meta_data, f, indent=4)
    
    def load_options(self): 
        with open("options_meta_data.json", "r") as f:
            temp = json.load(f)
        return temp

    ##! Saving Option Meta Data

    ## Checking Accepted Market Data Exchanges 

    def check_l2_exchanges(self): 

        self.reqMktDepthExchanges()

    def mktDepthExchanges(self, depthMktDataDescriptions):
        print("MktDepthExchanges:")
        for desc in depthMktDataDescriptions:
            if (desc.secType == "OPT"): # filtering for Options only
                print("DepthMktDataDescription.", desc)

    ##! Checking Accepted Market Data Exchanges 

    ## Top of Book Live Tick By Tick Data (No OPT)

    def req_top_of_book_tick_data(self,ticker: str): # -> None
        """
        Real time tick-by-tick data is currently NOT available for OPTIONS. Historical tick-by-tick data is available.
        """

        with self.order_id_lock:
            order_id = self.order_id
            self.order_id += 1

        stock_contract = self.create_stock_contract(ticker)

        req_data_type = "BidAsk"

        self.reqId_to_stock_tick_by_tick_contract = {"ticker": ticker, "Exchange": "SMART", "req_data_type": req_data_type}

        self.reqTickByTickData(order_id, stock_contract,req_data_type, 0, True) #BidAsk
        print(f"Establishing request for STK: {ticker}")
    
    def tickByTickAllLast(self, reqId: int, tickType: int, time: int, price: float, size, tickAtrribLast, exchange: str,specialConditions: str):
        print(" ReqId:", reqId, "Time:", time, "Price:", price, "Size:", size, "Exch:" , exchange, "Spec Cond:", specialConditions, "PastLimit:", tickAtrribLast.pastLimit, "Unreported:", tickAtrribLast.unreported)
    
    def tickByTickBidAsk(self, reqId: int, time: int, bidPrice: float, askPrice: float, bidSize, askSize, tickAttribBidAsk):
        # 0 is ask 
        # 1 is bid
        print("BidAsk. ReqId:", reqId, "Time:", time, "BidPrice:", bidPrice, "AskPrice:", askPrice, "BidSize:", bidSize, "AskSize:", askSize, "BidPastLow:", tickAttribBidAsk.bidPastLow, "AskPastHigh:", tickAttribBidAsk.askPastHigh)
    
    def tickByTickMidPoint(self, reqId: int, time: int, midPoint: float):
        print("Midpoint. ReqId:", reqId, "Time:", time, "MidPoint:", midPoint)
    
    def cancel_tick_data(self, reqId): 
        self.cancelTickByTickData(reqId)
    
    def shutdown_top_of_book_stk_connection(self):
    
        for reqId, v in self.reqId_to_stock_tick_by_tick_contract.items(): 
            self.cancelTickByTickData(reqId)
        
        print("ALL Top of Book (STK) Streams Closed")

    ##! Top of Book Live Tick By Tick Data (No OPT)

    ## L1 Options Market Data

    def req_L1_OPT_Market_Data(self, ticker: str, exp_pos: int, strike_pos: int, right: str, secType: str): # -> None
        """
        Return Order: 
        1. 45 - LastTime 
        2. 4 - LastPrice
        3. 5 - LastSize (prints twice)
        """

        with self.order_id_lock:
            order_id = self.order_id
            self.order_id += 1

        if (secType == "OPT"): 

            option_strike = self.options_meta_data[ticker]['strike'][strike_pos]
            option_exp = self.options_meta_data[ticker]['exp'][exp_pos]

            self.reqId_to_L1_Option_Contract[order_id] = {"ticker": ticker, "reqId": order_id, "exchange": "SMART", "exp": option_exp, "strike": option_strike, "right": right, "secType": "OPT"}
            
            contract = self.create_opt_contract(symbol = ticker, strike = option_strike, exp = option_exp, right = right, exchange = "SMART")
        else: 
            self.reqId_to_L1_Option_Contract[order_id] = {"ticker": ticker, "reqId": order_id, "exchange": "SMART", "exp": "", "strike": 0, "right": "", "secType": "STK"}
            contract = self.create_stock_contract(ticker, "SMART")

        self.reqMktData(order_id, contract, "", False, False, [])
        print(f"Starting L1 Market Stream: {self.reqId_to_L1_Option_Contract[order_id]}")
    

    def tickOptionComputation(self, reqId, tickType, tickAttrib: int, impliedVol: float, delta: float, optPrice: float, pvDividend: float, gamma: float, vega: float, theta: float, undPrice: float):
        """
        Triggered by tickType: 10 - bid (OPT Greeks), 11 - (OPT Greeks), 12 - Last (OPT Greeks)
        """

        temp = 1
        
        # reqId, tickType = int(reqId), int(tickType)

        # ts = py_time.time()
        # dt = datetime.fromtimestamp(ts)
        # formatted = dt.strftime("%m/%d/%Y %H:%M:%S")
        
        # data_now = (
        #     reqId,
        #     self.reqId_to_L1_Option_Contract[reqId]["ticker"],
        #     self.reqId_to_L1_Option_Contract[reqId]["exchange"], 
        #     self.reqId_to_L1_Option_Contract[reqId]["exp"], 
        #     self.reqId_to_L1_Option_Contract[reqId]["strike"],
        #     self.reqId_to_L1_Option_Contract[reqId]["right"],
        #     2, # OptionType: 0 = STK, 1 = OPT, 2 = OPT Greeks
        #     1, # position
        #     1, # Operation
        #     1, # side
        #     0, # price
        #     0, # size
        #     tickType, # tickType
        #     "", # tickString
        #     impliedVol, # Implied Volatility
        #     delta, # delta
        #     optPrice, # predicted optPrice
        #     pvDividend, # pvDividend
        #     gamma, # gamma
        #     vega, # vega
        #     theta, # theta
        #     undPrice, # underlying STK Price
        #     formatted,
        #     py_time.perf_counter_ns()
        # )

        # with self.tick_data_collection_lock: 
            
        #     self.tick_data_collection.append(data_now)

        #     curr_unix_time = int(py_time.time())
        #     if (curr_unix_time >  self.last_push_in_sec + 30): 
        #         self.wake_db_worker.set()
        #         self.last_push_in_sec = curr_unix_time

        # print("TickOptionComputation. TickerId:", reqId, "TickType:", tickType, "TickAttrib:", tickAttrib, "ImpliedVolatility:", impliedVol, "Delta:", delta, "OptionPrice:", optPrice, "pvDividend:", pvDividend, "Gamma: ", gamma, "Vega:", vega, "Theta:", theta, "UnderlyingPrice:", undPrice)

    def tickGeneric(self, reqId, tickType, value: float):
        temp = 1
        # print("TickGeneric. TickerId:", reqId, "TickType:", tickType, "Value:", value)
    
    def tickPrice(self, reqId, tickType, price: float, attrib):
        """
        Triggered by TickTypes: 1 - BidPrice, 2 - AskPrice, 4 - LastPrice, 
        """

        # Time Stamp Record Keeping
        received_ts = py_time.time_ns()
        ts = py_time.time()
        dt = datetime.fromtimestamp(ts)
        human_readable_ts = dt.strftime("%m/%d/%Y %H:%M:%S")

        reqId, tickType = int(reqId), int(tickType)

        position = 0
        operation = 1
        size = -1
        side = 1 if (tickType == 1) else (0 if (tickType == 2) else (2 if (tickType == 4) else 10))

        if (side == 10): 
            return

        if (self.reqId_to_L1_Option_Contract[reqId]["secType"] == "STK"  and side != 2): 
            return

        data_now = (
            self.reqId_to_L1_Option_Contract[reqId]["secType"],
            reqId,
            self.reqId_to_L1_Option_Contract[reqId]["ticker"],
            self.reqId_to_L1_Option_Contract[reqId]["exchange"], 
            self.reqId_to_L1_Option_Contract[reqId]["exp"], 
            self.reqId_to_L1_Option_Contract[reqId]["strike"],
            self.reqId_to_L1_Option_Contract[reqId]["right"],
            position, # position
            operation, # Operation
            side, # side
            price, # price
            size, # size
            human_readable_ts,
            received_ts
        )

        with self.tick_data_collection_lock: 
            
            self.tick_data_collection.append(data_now)

            curr_unix_time = int(py_time.time())
            if (curr_unix_time >  self.last_push_in_sec + 30): 
                self.wake_db_worker.set()
                self.last_push_in_sec = curr_unix_time
        
        # print("tickPrice. TickerId:", reqId, "TickType:", tickType, "Price:", price, "Attribute:", attrib)
    
    def tickSize(self, reqId, tickType, size):
        """
        Triggered by TickTypes: 0 - BidExchange, 3 - AskExchange, 5 - LastTime, 6 - Low, 7 - High, 9 - Prev Day Close Price
        """
        
        # Time Stamp Record Keeping
        received_ts = py_time.time_ns()
        ts = py_time.time()
        dt = datetime.fromtimestamp(ts)
        human_readable_ts = dt.strftime("%m/%d/%Y %H:%M:%S")

        reqId, tickType = int(reqId), int(tickType)

        position = 0
        operation = 1
        price = -1
        
        side = 1 if (tickType == 0) else (0 if (tickType == 3) else (2 if (tickType == 5) else 10))
        if (side == 10): 
            return

        if (self.reqId_to_L1_Option_Contract[reqId]["secType"] == "STK" and side != 2): 
            return
        
        data_now = (
            self.reqId_to_L1_Option_Contract[reqId]["secType"],
            reqId,
            self.reqId_to_L1_Option_Contract[reqId]["ticker"],
            self.reqId_to_L1_Option_Contract[reqId]["exchange"], 
            self.reqId_to_L1_Option_Contract[reqId]["exp"], 
            self.reqId_to_L1_Option_Contract[reqId]["strike"],
            self.reqId_to_L1_Option_Contract[reqId]["right"],
            position, # position
            operation, # Operation
            side, # side
            price, # price
            size, # size
            human_readable_ts,
            received_ts
        )

        with self.tick_data_collection_lock: 
            
            self.tick_data_collection.append(data_now)

            curr_unix_time = int(py_time.time())
            if (curr_unix_time >  self.last_push_in_sec + 30): 
                self.wake_db_worker.set()
                self.last_push_in_sec = curr_unix_time

        # print("TickSize. TickerId:", reqId, "TickType:", tickType, "Size: ", size)
    
    def tickString(self, reqId, tickType, value: str):
        """
        Triggered by TickTypes: 32 - BidExchange, 33 - AskExchange, 4 - LastTime
        """
        temp = 1

        # reqId, tickType = int(reqId), int(tickType)

        # ts = py_time.time()
        # dt = datetime.fromtimestamp(ts)
        # formatted = dt.strftime("%m/%d/%Y %H:%M:%S")

        # data_now = (
        #     reqId,
        #     self.reqId_to_L1_Option_Contract[reqId]["ticker"],
        #     self.reqId_to_L1_Option_Contract[reqId]["exchange"], 
        #     self.reqId_to_L1_Option_Contract[reqId]["exp"], 
        #     self.reqId_to_L1_Option_Contract[reqId]["strike"],
        #     self.reqId_to_L1_Option_Contract[reqId]["right"],
        #     1, # # OptionType: 0 = STK, 1 = OPT, 2 = OPT Greeks
        #     1, # position
        #     1, # Operation
        #     1, # side
        #     0, # price
        #     0, # size
        #     tickType, # tickType
        #     value, # tickString
        #     0, # Implied Volatility
        #     0, # delta
        #     0, # predicted optPrice
        #     0, # pvDividend
        #     0, # gamma
        #     0, # vega
        #     0, # theta
        #     0, # underlying STK Price
        #     formatted,
        #     py_time.perf_counter_ns()
        # )

        # with self.tick_data_collection_lock: 
            
        #     self.tick_data_collection.append(data_now)

        #     curr_unix_time = int(py_time.time())
        #     if (curr_unix_time >  self.last_push_in_sec + 30): 
        #         self.wake_db_worker.set()
        #         self.last_push_in_sec = curr_unix_time

        # print("TickString. TickerId:", reqId, "Type:", tickType, "Value:", value)

    def tickReqParams(self, tickerId:int, minTick:float, bboExchange:str, snapshotPermissions:int):
        temp = 1
        
        # print("TickReqParams. TickerId:", tickerId, "MinTick:", minTick, "BboExchange:", bboExchange, "SnapshotPermissions:", snapshotPermissions)

    def cancel_req_L1_OPT_Market_Data(self):
        """
        Cancels all L1 OPT Market Data Streams
        """
        print("Cancelling L1 OPT Market Stream!")

        for u, v in self.reqId_to_L1_Option_Contract.items():
            self.cancelMktData(u)
            print(f"    Cancelled: {v}")
        
        print("All L1 OPT Market Stream are Cancelled")

    ##! L1 Options Market Data

    ## Historical tick by tick (Sales & Time)

    def req_Historical_tick_by_tick_data(self, ticker: str): # -> None
        """
        Calls Historical Sales and Time Data (aka Top-Of-Book) for OPT
        
        Triggers (xor): 
            -   historicalTicks
            -   historicalTicksBidAsk
            -   historicalTicksLast
        
        Output Time: 
            -   1 sec (in Unix since 1970-01-01)
            -   Fills From back descending (aka got 1000 ticks from last 137 seconds = 2 min)
        """

        with self.order_id_lock:
            order_id = self.order_id
            self.order_id += 1
        
        option_strike = self.options_meta_data[ticker]['strike'][0]
        option_exp = self.options_meta_data[ticker]['exp'][0]

        self.reqId_to_Historical_option_contract[order_id] = {"ticker": ticker, "exchange": "SMART", "exp": option_exp, "strike": option_strike, "right": "C"}
        
        opt_contract_PSE = self.create_opt_contract(symbol = ticker, strike = option_strike, exp = option_exp, right = "C", exchange = "SMART")
        
        self.reqHistoricalTicks(order_id, opt_contract_PSE, "20260225 09:30:00 US/Eastern", "20260225 10:30:00 US/Eastern", 1000, "Midpoint", 1, True, [])
        # "Bid_Ask, Midpoint, or Trades"
    
    def historicalTicks(self, reqId: int, ticks, done: bool):
        """
        Triggered by 'Midpoint'

        Outputs: 
            -   ReqId
            -   Time (Unix sec)
            -   Price
            -   Size = 0 (always)
        """

        for tick in ticks:
            print("historicalTicks. ReqId:", reqId, tick)
        
        print("---------------------------------")
        
        print("historicalTicksBidAsk. ReqId:", reqId, ticks[0])
        print("historicalTicksBidAsk. ReqId:", reqId, ticks[-1])
        print(f"size of ticks: {len(ticks)}")
        print(done)
            
    def historicalTicksBidAsk(self, reqId: int, ticks, done: bool):
        """
        Triggered by 'Bid_Ask'

        Outputs: 
            -   ReqId
            -   Time (Unix sec)
            -   PriceBid
            -   PriceAsk
            -   SizeBid = 1 (always)
            -   SizeAsk = 1 (always)
        """

        for tick in ticks:
            print("historicalTicksBidAsk. ReqId:", reqId, tick)
        
        print("---------------------------------")
        
        print("historicalTicksBidAsk. ReqId:", reqId, ticks[0])
        print("historicalTicksBidAsk. ReqId:", reqId, ticks[-1])
        print(f"size of ticks: {len(ticks)}")
        print(done)

    def historicalTicksLast(self, reqId: int, ticks, done: bool):
        """
        Triggered by 'Trades'

        Outputs: 
            -   ReqId
            -   Time (Unix sec)
            -   Price
            -   Size
            -   Exchange
        """

        for tick in ticks:
            print("HistoricalTickLast. ReqId:", reqId, tick)

        print("---------------------------------")

        print("HistoricalTickLast. ReqId:", reqId, ticks[0])
        print("HistoricalTickLast. ReqId:", reqId, ticks[-1])
        print(f"size of ticks: {len(ticks)}")
        
    ##! Historical tick by tick (Sales & Time)















def unix_to_est_str(unix_seconds: int): # -> str
    """
    Convert Unix epoch seconds to human-readable US/Eastern datetime string.
    Handles DST automatically.
    """
    eastern = ZoneInfo("America/New_York")
    dt = datetime.fromtimestamp(unix_seconds, eastern)
    return dt.strftime("%Y-%m-%d %H:%M:%S %Z")

# ================= Time =================

def did_market_already_close(): 
    eastern = ZoneInfo("America/New_York")
    
    now = datetime.now(eastern)
    market_close = datetime.combine(now.date(), time(16, 15), tzinfo=eastern)
    
    seconds = (market_close - now).total_seconds()
    
    return int(seconds) < 0

def seconds_until_market_close():
    eastern = ZoneInfo("America/New_York")
    
    now = datetime.now(eastern)
    market_close = datetime.combine(now.date(), time(16, 15), tzinfo=eastern)
    
    seconds = (market_close - now).total_seconds()
    
    return max(0, int(seconds))

def seconds_until_market_open():
    eastern = ZoneInfo("America/New_York")
    
    now = datetime.now(eastern)
    market_open = datetime.combine(now.date(), time(9, 30), tzinfo=eastern)
    market_close = datetime.combine(now.date(), time(16, 15), tzinfo=eastern)

    
    wait_open = (market_open - now).total_seconds()
    wait_close = (market_close - now).total_seconds()

    if (wait_open < 0 and wait_close > 0): # Open Market
        return 0
    elif (wait_open > 0): # Pre-Market
        return wait_open
    else: # Post-Market
        tomorrow = now.date() + timedelta(days=1)
        next_open = datetime.combine(tomorrow, time(9, 30), tzinfo=eastern)
        (next_open - now).total_seconds()


# ================= DB Thread =================

def db_worker():

    count = 0
    STK_c = 0
    OPT_c = 0

    while not stop_event.is_set():
        app.wake_db_worker.clear()
        app.wake_db_worker.wait()

        with app.tick_data_collection_lock:
            retreived_data = app.tick_data_collection.copy()
            app.tick_data_collection.clear()
        
        
        if (len(retreived_data) > 0):

            for i in retreived_data:
                if (i[0] == "STK"): 
                    STK_c += 1
                elif (i[0] == "OPT"): 
                    OPT_c += 1

            ts = py_time.time()
            dt = datetime.fromtimestamp(ts)
            formatted = dt.strftime("%m/%d/%Y %H:%M:%S")
            
            count += len(retreived_data)
            print(f"Cols in PSQL: {count} @ {formatted}")
            print(f"STK Count: {STK_c}")
            print(f"OPT Count: {OPT_c}")
            add(retreived_data)
    
    print("DB WOrker Exited")
    

# ================= Live Trading =================

def setup_app_and_get_order_id(app, start_trade = False):
    """
    creates a local connection to TWS IBKR through the IBKR Class API
    Retrieves starting ConId
    """

    app.connect("127.0.0.1", 7497, 0)
    app_thread = threading.Thread(target=app.run, daemon=True)
    app_thread.start()

    while app.order_id is None:
        py_time.sleep(0.1)

    print(f"Main thread received order_id: {app.order_id}")

    return app_thread

# ================= Main Thread =================

if __name__ == "__main__":

    app = TestApp()

    ib_thread = setup_app_and_get_order_id(app, start_trade = True)
    db_worker_thread = threading.Thread(target=db_worker, daemon=True)
    db_worker_thread.start()

    tickers = ["SPY", "QQQ", "IWM"]

    # SPY QQQ IWM AAPL TSLA AMD, META MSFT
    
    for ticker in tickers:
        app.request_option_chain(ticker)
        app.req_historical_price(ticker)
        app.create_options_metadata(ticker)
    
    current_meta_data = app.load_options()
    ftoday = date.today()

    for u, v in current_meta_data.items():
        if ('date' in v and v['date'] == str(date.today())):
            app.options_meta_data = current_meta_data
        else:
            app.save_options()
        break
    
    # Sleep Till Market Open
    sleep_till_open = seconds_until_market_open()  # Buffer to start streams prior to market open
    if (sleep_till_open != 0): 
        sleep_till_open = max(0, sleep_till_open -10)
    if (sleep_till_open == 0):
        print("MARKET IS OPEN !!!")
    else: 
        print(f"Sleeping for {sleep_till_open} sec, {sleep_till_open / 60} minues, {sleep_till_open / 3600} hours")
        py_time.sleep(sleep_till_open)

    for ticker in tickers:
        app.req_opt_L2(ticker)
        py_time.sleep(0.1)
        app.req_L1_OPT_Market_Data(ticker, 0, 0, "", secType = "STK")
        for i in range(2): 
            for j in range(2): 
                for c in ["C", "P"]:
                    app.req_L1_OPT_Market_Data(ticker, i, j, c, secType = "OPT")
                    py_time.sleep(0.1)

        # 2 strikes, 2 exps, 2 rights = 8 Streams / ticker
        
    # Sleep Till Market Close
    sleep_till_close = seconds_until_market_close()
    print(f"Sleeping for {sleep_till_close} sec, {sleep_till_close / 60} minues, {sleep_till_close / 3600} hours")
    py_time.sleep(sleep_till_close)

    # Shutdown Streams
    app.cancel_req_L1_OPT_Market_Data()
    app.shutdown_l2_opt_connection()

    stop_event.set()
    app.wake_db_worker.set() 
    db_worker_thread.join()

    app.disconnect()    
    ib_thread.join()