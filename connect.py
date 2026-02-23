from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
from ibapi.order import Order 
from ibapi.contract import ComboLeg
import threading
from threading import Event, Thread, Semaphore, Lock
from ibapi.common import BarData
import pandas as pd
from datetime import datetime, timedelta
import time
from collections import deque
import warnings
from queue import Queue
import numpy as np
import json

# Mute Warnings
warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings("ignore", category=UserWarning)
warnings.filterwarnings("ignore", message="Choices for a categorical distribution.*")
warnings.filterwarnings("ignore", category=FutureWarning, message=".*weights_only=False.*")

# ================= IBKR Connection =================

TICK_TYPE_NAMES = {
    0: "Bid Size",
    1: "Bid Price",
    2: "Ask Price",
    3: "Ask Size",
    4: "Last Price",
    5: "Last Size",
    6: "High",
    7: "Low",
    8: "Volume",
    32: "Exchange",
    45: "Last Timestamp"
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

    def create_stock_contract(self, symbol):
        """
        Creates a contract for a specified stock symbol
        """

        contract = Contract()
        contract.symbol = symbol
        contract.secType = "STK"
        contract.exchange = "SMART"
        contract.currency = "USD"
        return contract
    
    def create_opt_contract(self, symbol: str, strike: float, exp: str, right: str, exchange: str) -> Contract:
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
    def reqL2(self,ticker, strike_pos: int, exp_pos: int, opt_right: str, opt = False):
       
        print("establishing request for L2:")

        with self.order_id_lock:
            order_id_1 = self.order_id
            self.order_id +=1
        
        with self.order_id_lock:
            order_id_2 = self.order_id
            self.order_id +=1

        if (opt): 
            option_strike = self.options_meta_data[ticker]['strike'][strike_pos]
            option_exp = self.options_meta_data[ticker]['exp'][exp_pos]
            opt_contract_PSE = self.create_opt_contract(symbol = ticker, strike = option_strike, exp = option_exp, right = opt_right, exchange = "PSE")
            opt_contract_NASDAQOM = self.create_opt_contract(symbol = ticker, strike = option_strike, exp = option_exp, right = opt_right, exchange = "NASDAQOM")

            # self.reqContractDetails(req_order_id, opt_contract)

            self.reqMktDepth(order_id_1, opt_contract_PSE, 10, False, [])
            self.reqMktDepth(order_id_2, opt_contract_NASDAQOM, 10, False, [])
        else: 
            stock_contract = self.create_stock_contract(ticker)
            self.reqMktDepth(order_id_1, stock_contract, 10, False, [])

    def updateMktDepth(self, reqId, position: int, operation: int, side: int, price: float, size):
        # Triggered by: SPY, QQQ, IWM 
        reqId, size = int(reqId), float(size)
        print("UpdateMarketDepth. ReqId:", reqId, "Position:", position, "Operation:", operation, "Side:", side, "Price:", price, "Size:", size)
            
    def updateMktDepthL2(self, reqId, position: int, marketMaker: str, operation: int, side: int, price: float, size, isSmartDepth: bool):
        reqId, size = int(reqId), float(size)
        print("UpdateMarketDepthL2. ReqId:", reqId, "Position:", position, "MarketMaker:", marketMaker, "Operation:", operation, "Side:", side, "Price:", price, "Size:", size, "isSmartDepth:", isSmartDepth)

    def cancel_L2(self, reqId): 
        self.cancelMktDepth(reqId, False)
        print("cancelled L2 MKT Data")
    
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
        self.reqHistoricalData(order_id, contract, "", "1 W", "1 day", "MIDPOINT", 1, 1, False, [])
        self.historical_data_event.wait(10)

        # Sort Data Histrical Bars and get most recent Price Close
        df = pd.DataFrame(self.intermediate_prices)
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
    def create_options_metadata(self, ticker: str) -> None: 

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
        
        self.options_meta_data[ticker] = {"conId": self.ticker_to_conId[ticker], "price": price, "strike": ticker_strike, "exp": option_exps}
        print(self.options_meta_data[ticker])

    def save_options(self): 
        with open("options_meta_data.json", "w") as f:
            json.dump(self.options_meta_data, f, indent=4)
    
    def load_options(self): 
        with open("options_meta_data.json", "r") as f:
            self.options_meta_data = json.load(f)

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

    ## Top of Book Live Tick Data

    def req_top_of_book_tick_data(self,ticker, strike_pos: int, exp_pos: int, opt_right: str, opt = False):

        with self.order_id_lock:
            order_id = self.order_id
            self.order_id += 1
        
        if (opt): 
            option_strike = self.options_meta_data[ticker]['strike'][strike_pos]
            option_exp = self.options_meta_data[ticker]['exp'][exp_pos]
            opt_contract = self.create_opt_contract(symbol = ticker, strike = option_strike, exp = option_exp, right = opt_right, exchange="SMART")

            self.reqTickByTickData(order_id, opt_contract, "BidAsk", 0, True)
        else: 
            stock_contract = self.create_stock_contract(ticker)
            self.reqTickByTickData(order_id, stock_contract, "BidAsk", 0, True)
    
    def tickByTickAllLast(self, reqId: int, tickType: int, time: int, price: float, size, tickAtrribLast, exchange: str,specialConditions: str):
        print(" ReqId:", reqId, "Time:", time, "Price:", price, "Size:", size, "Exch:" , exchange, "Spec Cond:", specialConditions, "PastLimit:", tickAtrribLast.pastLimit, "Unreported:", tickAtrribLast.unreported)
    
    def tickByTickBidAsk(self, reqId: int, time: int, bidPrice: float, askPrice: float, bidSize, askSize, tickAttribBidAsk):
        print("BidAsk. ReqId:", reqId, "Time:", time, "BidPrice:", bidPrice, "AskPrice:", askPrice, "BidSize:", bidSize, "AskSize:", askSize, "BidPastLow:", tickAttribBidAsk.bidPastLow, "AskPastHigh:", tickAttribBidAsk.askPastHigh)
    
    def tickByTickMidPoint(self, reqId: int, time: int, midPoint: float):
        print("Midpoint. ReqId:", reqId, "Time:", time, "MidPoint:", midPoint)
    
    def cancel_tick_data(self): 
        self.cancelTickByTickData(19001)

    ##! Top of Book Live Tick Data

    








    


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
        time.sleep(0.1)

    print(f"Main thread received order_id: {app.order_id}")

    return app_thread

# ================= Main Thread =================

if __name__ == "__main__":

    start_time = time.time()
    app = TestApp()

    ib_thread = setup_app_and_get_order_id(app, start_trade = True)

    tickers = ["IWM"] #"SPY", "QQQ", "IWM", "AAPL", "TSLA", "AMD"] # , "MSFT", "TSLA", "JPM", "BAC", "GS"

    # SPY QQQ IWM AAPL TSLA AMD, META MSFT
    
    for ticker in tickers:
        app.request_option_chain(ticker)
        app.req_historical_price(ticker)
        # print(f"{ticker}: {app.conId_to_price[app.ticker_to_conId[ticker]]}")
        app.create_options_metadata(ticker)
    
    # app.check_l2_exchanges()
    
    # app.reqL2(tickers[0], strike_pos = 0 , exp_pos= 0, opt_right= "C", opt = True)
    app.req_top_of_book_tick_data(tickers[0], strike_pos = 0 , exp_pos= 0, opt_right= "C", opt = False)
    # app.reqL2(tickers[0], strike_pos = 0 , exp_pos= 0, opt_right= "C", opt = True)
    time.sleep(1)
    
    # app.load_options()
    # app.save_options()
    # print(app.options_meta_data)

    app.disconnect()
    ib_thread.join()
  
    
    
