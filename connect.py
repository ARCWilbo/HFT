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

        # Order ID
        self.order_id = None 
        self.order_id_lock = Lock()

        # Contract Event
        self.contract_lock = Lock()
        self.contract_event = Event()
        self.contract_dict = {} # contains another dictionary with keys: "exp" and "strike"

        # Options
        self.options_lock = Lock()
        self.ticker_to_conId = {}
        self.conId_option_chain = {}


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
    def reqL2(self,ticker):
       
        print("establishing request for L2:")
        stock_contract = self.create_stock_contract(ticker)
        self.reqMktDepth(2001, stock_contract, 5, False, [])

    def updateMktDepth(self, reqId, position: int, operation: int, side: int, price: float, size):
        reqId, size = int(reqId), float(size)
        print("UpdateMarketDepth. ReqId:", reqId, "Position:", position, "Operation:", operation, "Side:", side, "Price:", floatMaxString(price), "Size:", decimalMaxString(size))
            
    def updateMktDepthL2(self, reqId, position: int, marketMaker: str, operation: int, side: int, price: float, size, isSmartDepth: bool):
        reqId, size = int(reqId), float(size)
        print("UpdateMarketDepthL2. ReqId:", reqId, "Position:", position, "MarketMaker:", marketMaker, "Operation:", operation, "Side:", side, "Price:", floatMaxString(price), "Size:", decimalMaxString(size), "isSmartDepth:", isSmartDepth)

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
        
        self.reqSecDefOptParams(order_id, ticker, "", "STK", contract.contract.conId)
    
    def securityDefinitionOptionParameter(self, reqId: int, exchange: str, underlyingConId: int, tradingClass: str, multiplier: str, expirations, strikes):
        #print("SecurityDefinitionOptionParameter.", "ReqId:", reqId, "Exchange:", exchange, "Underlying conId:", underlyingConId, "TradingClass:", tradingClass, "Multiplier:", multiplier, "Expirations:", expirations, "Strikes:", strikes)

        with self.contract_lock: 
            if (underlyingConId not in self.contract_dict):
                self.contract_dict[underlyingConId] = {"exp": expirations, "strike": strikes}
            else: 
                self.contract_dict[underlyingConId]["exp"].add(expirations)
                self.contract_dict[underlyingConId]["strike"].add(strikes)


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

        with self.contract_lock:
            contract = self.contract_dict[order_id]
            self.ticker_to_conId[ticker] = contract.contract.conId
        
        return contract
    
    def contractDetails(self, reqId: int, contractDetails):
        
        with self.contract_lock:
            self.contract_dict[reqId] = contractDetails
        
        # print(reqId, contractDetails)
    
    def contractDetailsEnd(self, reqId: int):
        print("ContractDetailsEnd. ReqId:", reqId)
        self.contract_event.set() 

    ##! Req Contract Details
    
    ## Current Asset Price 
    def req_historical_price(self, ticker): 

        with self.order_id_lock:
            order_id = self.order_id
            self.order_id += 1
        
        contract = self.Req_Contract_details(ticker)
        
        self.reqHistoricalData(order_id, contract, "", "1 W", "1 day", "MIDPOINT", 1, 1, False, [])

    def historicalData(self, reqId:int, bar: BarData):
        print("HistoricalData. ReqId:", reqId, "BarData.", bar)
    
    def historicalDataEnd(self, reqId: int, start: str, end: str):
        print("HistoricalDataEnd. ReqId:", reqId, "from", start, "to", end)

    ##! Current Asset Price 







    


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
   
    time.sleep(1)
    app.disconnect()
    ib_thread.join()
  
    
    
