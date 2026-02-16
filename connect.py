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
        self.order_id = None 
        self.positions = []
        self.positions_ready = Event()
        self.last_trade = {
            "price": None,
            "size": None,
            "timestamp": None
        }
        self.historical_data = []
        self.historical_data_ready = Event()
        self.historical_data_lock = Lock()
        self.market_data_lock = Lock()
        self.order_id_lock = Lock()
        
        self.contract_event = Event() #contract event
        self.contract_dict = {} 
        self.contract_lock = Lock()
        
        self.market_data_events = {}
        self.market_data_results = {}
        self.historical_data_events = {}
        self.historical_data_results = {}
        self.late_historical_data = {}
        

        self.reqId_symbol = {} 
        self.symbol_tick_buffer = {}  
        self.symbol_tick_lock = Lock()

        self.bracket_fills = [] 
        self._fill_tracker = {}  
        self.tracker_lock = Lock()

        self.open_orders = {}  
        self.open_orders_event = Event()


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

    def execDetails(self, reqId, contract, execution):
        """
        Records All executed orders for individual stock thread profit calculations
        """

        symbol = contract.symbol
        side = execution.side.upper()
        price = execution.price
        shares = execution.shares
        order_id = execution.orderId
        if order_id not in self._fill_tracker: 
            return

        if self._fill_tracker[order_id] == "Long": 
            a = {"symbol": symbol,
                "order_id": order_id,
                "shares": shares,
                "entry_price": price,
                "exit_price": None,
                "direction": "Long",
            }
        elif self._fill_tracker[order_id] == "Short": 
            a = {"symbol": symbol,
                "order_id": order_id,
                "shares": shares,
                "entry_price": price,
                "exit_price": None,
                "direction": "Short"
            }
        elif self._fill_tracker[order_id] == "Exit": 
            a = {"symbol": symbol,
                "order_id": order_id,
                "shares": shares,
                "entry_price": None,
                "exit_price": price,
                "direction": None
            }
        with self.tracker_lock:
            self.bracket_fills.append(a)
    
    def retrive_and_empty_executed_orders(self): 
        """
        Returns the list of executed orders and resets it to a null set
        """
        
        with self.tracker_lock:
            a = self.bracket_fills  
            self.bracket_fills = []
        return a

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

    def request_stock_market_data(self, symbol):
        """
        Request live market data for a stock NOT historical data
        """

        with self.order_id_lock:
            ticker_id=self.order_id
            self.order_id +=1

        contract = self.create_stock_contract(symbol)

        with self.symbol_tick_lock:
            self.reqId_symbol[ticker_id] = symbol
            self.symbol_tick_buffer[symbol] = deque(maxlen=3000) 

        self.reqMktData(ticker_id, contract, "", False, False, [])
        print(f"📡 Started market data for {symbol} [reqId={ticker_id}]")
        
    def tickPrice(self, reqId, tickType, price, attrib):
        if tickType == 4: 
            with self.symbol_tick_lock:
                symbol = self.reqId_symbol.get(reqId)
                if symbol:
                    timestamp = datetime.now()
                    self.symbol_tick_buffer[symbol].append((timestamp, price, None))  

    def tickSize(self, reqId, tickType, size):
        if tickType == 5: 
            with self.symbol_tick_lock:
                symbol = self.reqId_symbol.get(reqId)
                if symbol and self.symbol_tick_buffer[symbol]:
                    last_timestamp, last_price, _ = self.symbol_tick_buffer[symbol][-1]
                    self.symbol_tick_buffer[symbol][-1] = (last_timestamp, last_price, size)


    def tickString(self, reqId, tickType, value):
        if tickType == 45: 
            with self.market_data_lock:
                ts = datetime.fromtimestamp(int(value))
                self.market_data_results.setdefault(reqId, {})["timestamp"] = ts
            self._maybe_set_event(reqId)

    def _maybe_set_event(self, reqId):
        with self.market_data_lock:
            result = self.market_data_results.get(reqId, {})
            if all(k in result for k in ["price", "size", "timestamp"]):
                if reqId in self.market_data_events:
                    self.market_data_events[reqId].set()

    def print_last_trade(self, reqId):
        trade = self.last_trade
        if None not in trade.values():
            #print(f"[{reqId}] LAST TRADE: {trade['price']} x {trade['size']} @ {trade['timestamp']}")
            # Reset to avoid duplicate prints
            self.last_trade = {"price": None, "size": None, "timestamp": None}

    def cancel_stock_market_data(self, ticker_id):
        """
        Cancel live market data stream for a stock
        """

        self.cancelMktData(ticker_id)
        print(f"Canceled market data for tickerId {ticker_id}")

    def request_historical_data(self, symbol: str, duration_str: str, bar_size: str, thread_id: int, end_datetime: str = ""):
        """
        Request Historical stock market data 
        """

        if self.order_id is None:
            print("Error: Next valid order ID has not been received yet.")
            return None

        contract = self.create_stock_contract(symbol)

        with self.order_id_lock:
            req_id = self.order_id
            self.order_id += 1

        event = Event()
        with self.historical_data_lock:
            self.historical_data_events[req_id] = event
            self.historical_data_results[req_id] = []

        self.reqHistoricalData(
            reqId=req_id,
            contract=contract,
            endDateTime=end_datetime,
            durationStr=duration_str,
            barSizeSetting=bar_size,
            whatToShow="TRADES",
            useRTH=0,
            formatDate=1,
            keepUpToDate=False,
            chartOptions=[]
        )

        event.wait(timeout=20)

        with self.historical_data_lock:
            bars = self.historical_data_results.pop(req_id, [])
            self.historical_data_events.pop(req_id, None)

        if not bars:
            print(f"[reqId={req_id}, T {thread_id}] ⏰ Timeout. Waiting thread gave up.")
            return None, req_id

        df = pd.DataFrame(bars)
        df["date"] = pd.to_datetime(df["date"])
        df.set_index("date", inplace=True)
        return df, req_id

    def historicalData(self, reqId: int, bar: BarData):
        with self.historical_data_lock:
            self.historical_data_results.setdefault(reqId, []).append({
                "date": bar.date,
                "open": bar.open,
                "high": bar.high,
                "low": bar.low,
                "close": bar.close,
                "volume": bar.volume
            })

    def historicalDataEnd(self, reqId: int, start: str, end: str):
        with self.historical_data_lock:
            if reqId in self.historical_data_events:
                self.historical_data_events[reqId].set()
            else:
                bars = self.historical_data_results.pop(reqId, [])
                if bars:
                    df = pd.DataFrame(bars)
                    df["date"] = pd.to_datetime(df["date"])
                    df.set_index("date", inplace=True)
                    self.late_historical_data[reqId] = df

    def cancel_historical_data_request(self, req_id: int):
        """
        Cancel a historical data request given a request ID
        """
        
        self.cancelHistoricalData(req_id)

    def request_available_funds(self):
        """
        Returns value of avaibale funds for trade
        """
        
        self.account_summary_ready = Event()
        self.account_summary_data = {}

        with self.order_id_lock:
            req_id = self.order_id
            self.order_id += 1

        self.reqAccountSummary(req_id, "All", "AvailableFunds,NetLiquidation")

        if not self.account_summary_ready.wait(timeout=60):
            print("⚠️ Timed out waiting for account value.")
            return 800000
        self.cancelAccountSummary(req_id)

        return self.account_summary_data.get("AvailableFunds", None)

    def accountSummary(self, reqId, account, tag, value, currency):
        if tag == "AvailableFunds":
            self.account_summary_data["AvailableFunds"] = float(value)
        elif tag == "NetLiquidation":
            self.account_summary_data["NetLiquidation"] = float(value)

    def accountSummaryEnd(self, reqId):
        print("✅ Account summary received.")
        self.account_summary_ready.set()
    
    def cancel_open_orders_for_symbol(self):
        """
        Gets a list of all open (unfilled) stock orders
        """

        self.open_orders = {}
        self.open_orders_event.clear()
        self.reqOpenOrders()
        if not self.open_orders_event.wait(timeout=2):
            print("⚠️ Timed out waiting for open orders.")
        self.open_orders_event.set()
    
    def cancel_all_stock_orders(self): 
        """
        Cancels all open (unfilled) stock orders for all stocks
        """

        for order_id, contract in self.open_orders.items():
            if contract.secType == "STK": 
                self.cancelOrder(order_id)

    def cancel_stock_orders(self, symbol: str):
        """
        Cancels all open (unfilled) stock orders for a specific stock
        """

        for order_id, contract in self.open_orders.items():
            if contract.secType == "STK" and contract.symbol.upper() == symbol.upper():
                self.cancelOrder(order_id)

    def openOrder(self, orderId, contract, order, orderState):
        if not hasattr(self, "open_orders"):
            self.open_orders = {}

        if orderState.status.lower() not in ["filled", "cancelled"]:
            self.open_orders[orderId] = contract
    
    def openOrderEnd(self):
        print("Finished receiving open orders.")
        self.open_orders_event.set()


##: Level 2 Market Data:

    def reqL2(self,ticker):
        print("establishing request for L2:")
        stock_contract = self.create_stock_contract(ticker)
        self.reqMktDepth(2001, stock_contract, 5, False, [])
        
    def updateMktDepth(self, reqId, position: int, operation: int, side: int, price: float, size):
        reqId = int(reqId)
        size = float(size)
        print("UpdateMarketDepth. ReqId:", reqId, "Position:", position, "Operation:", operation, "Side:", side, "Price:", (price), "Size:", (size))
 
    def updateMktDepthL2(self, reqId, position: int, marketMaker: str, operation: int, side: int, price: float, size, isSmartDepth: bool):
        reqId= int(reqId)
        size = float(size)
        print("UpdateMarketDepthL2. ReqId:", reqId, "Position:", position, "MarketMaker:", marketMaker, "Operation:", operation, "Side:", side, "Price:", (price), "Size:", (size), "isSmartDepth:", isSmartDepth)    
        
    def cancelL2(self,reqId):
        self.cancelMktDepth(2001, False)
        print("cancelled L2")

##: Options Market Data:

    def request_option_chain(self, ticker):

        with self.order_id_lock:
            order_id = self.order_id
            self.order_id += 1
        
        contract = self.Req_Contract_details(ticker)

        print(100000)

        self.reqSecDefOptParams(order_id, ticker, "", "STK", contract.contract.conId)
    
    def securityDefinitionOptionParameter(self, reqId: int, exchange: str, underlyingConId: int, tradingClass: str, multiplier: str, expirations, strikes):
        #print("SecurityDefinitionOptionParameter.", "ReqId:", reqId, "Exchange:", exchange, "Underlying conId:", underlyingConId, "TradingClass:", tradingClass, "Multiplier:", multiplier, "Expirations:", expirations, "Strikes:", strikes)
        print(expirations, strikes)

## Req Contract Details
    def Req_Contract_details(self, ticker): 

        with self.order_id_lock:
            order_id = self.order_id
            self.order_id += 1
        
        stock_contract = self.create_stock_contract(ticker)

        self.contract_event.clear()
        self.reqContractDetails(order_id, stock_contract)
        self.contract_event.wait(timeout=10)

        with self.contract_lock:
            contract = self.contract_dict[order_id]
        
        return contract
    
    def contractDetails(self, reqId: int, contractDetails):
        with self.contract_lock:
            self.contract_dict[reqId] = contractDetails
        
        print(reqId, contractDetails)
    
    def contractDetailsEnd(self, reqId: int):
        print("ContractDetailsEnd. ReqId:", reqId)
        self.contract_event.set() 
    



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

    #pos = app.get_positions()
    return app_thread

# ================= Main Thread =================

if __name__ == "__main__":
    # ================= Live Trading =================

    start_time = time.time()
    app = TestApp()
    ib_thread = setup_app_and_get_order_id(app, start_trade = True)
    app.request_option_chain("AAPL")
    time.sleep(10)
    available_funds = app.request_available_funds()
    print(available_funds)
    app.disconnect()
    ib_thread.join()
  
    
    