import oandapyV20
import oandapyV20.endpoints.pricing as pricing
import json
import json 
import threading
import csv
import os
import time
import pandas as pd
import requests
from config import ACCOUNT_ID, ACCESS_TOKEN
from oandapyV20 import API
from oandapyV20.endpoints.orders import OrderCreate, OrderReplace, OrderDetails, OrderCancel
from oandapyV20.endpoints.transactions import TransactionList
from oandapyV20.endpoints.transactions import TransactionsSinceID
from datetime import datetime, timedelta, timezone
from collections import OrderedDict
from dateutil import parser  # Install with `pip install python-dateutil`
import pytz

UNITS = 1000  # Number of units to trade
INSTRUMENT = "EUR_USD"
PIP_SIZE = 0.0001
NUM_ORDERS_2 = 5
NUM_ORDERS_5 = 3
# Initialize order books and pending trades 2
buy_orders_2 = []
sell_orders_2 = []
last_price = None
pending_trades_2 = {}
# Initialize filled orders and order books 2 
filled_orders_2 = {}
filled_buy_orders_2 = []
filled_sell_orders_2 = []
# Initialize order books and pending trades 5
buy_orders_5 = []
sell_orders_5 = []
pending_trades_5 = {}
# Initialize filled orders and order books 5
filled_orders_5 = {}
filled_buy_orders_5 = []
filled_sell_orders_5 = []
# Initialize the API client
client = oandapyV20.API(access_token=ACCESS_TOKEN)

def get_latest_price():
    """Fetch the latest price from the pricing stream and convert time to Singapore Time."""
    pricing_stream = pricing.PricingStream(accountID=ACCOUNT_ID, params={"instruments": "EUR_USD"})
    try:
        for r in client.request(pricing_stream):
            if r['type'] == 'PRICE':
                bids = r['bids'][0]['price']
                asks = r['asks'][0]['price']
                utc_time = parser.parse(r['time'])  # Correctly parses long ISO8601 timestamps
                sgt_time = utc_time.astimezone(pytz.timezone("Asia/Singapore"))
                formatted_time = sgt_time.strftime("%Y-%m-%d %H:%M:%S %Z%z")
                return {"timestamp": formatted_time, "bid": bids, "ask": asks}
    except KeyboardInterrupt:
        print("Streaming stopped by user.")
    except Exception as e:
        print(f"Error fetching latest price: {e}")
    return None

def log_filled_order(order_type, price, order_id, direction, fill_price, log_file):
    """Log filled order details to a CSV file."""
    import csv
    from datetime import datetime
    with open(log_file, "a", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            datetime.now().isoformat(),
            order_type,
            price,
            order_id,
            direction,
            fill_price
        ])

def fetch_recent_closed_transactions(minutes=5, filter_type="all", instrument=None, filename="profit_loss_trades.csv"):
    """
    Fetch and export filtered OANDA trades to a CSV file.
    filter_type: "profit", "loss", or "all"
    instrument: e.g., "EUR_USD", or None for all instruments
    """
    try:
        now = datetime.now(timezone.utc)
        since_time = now - timedelta(minutes=minutes)
        since_time_str = since_time.strftime("%Y-%m-%dT%H:%M:%SZ")

        params = {"from": since_time_str, "type": "ORDER_FILL"}
        r = TransactionList(accountID=ACCOUNT_ID, params=params)
        response = client.request(r)
        transactions = response.get("transactions", [])

        filtered = []

        for tx in transactions:
            pl = float(tx.get("pl", 0))
            if filter_type == "profit" and pl <= 0:
                continue
            elif filter_type == "loss" and pl >= 0:
                continue
            if instrument and tx.get("instrument") != instrument:
                continue
            filtered.append(tx)

        if not filtered:
            print("No trades matched the filter.")
            return []

        # Write to CSV
        with open(filename, mode="w", newline="") as file:
            writer = csv.writer(file)
            writer.writerow(["Time", "Instrument", "Units", "Side", "Price", "P/L"])
            for tx in filtered:
                side = "BUY" if int(tx["units"]) > 0 else "SELL"
                writer.writerow([tx["time"], tx["instrument"], tx["units"], side, tx["price"], tx["pl"]])

        total_pl = sum(float(tx["pl"]) for tx in filtered)
        print(f"Exported {len(filtered)} trades to '{filename}'.")
        print(f"Net P/L: {round(total_pl, 2)} USD")

        return filtered

    except Exception as e:
        print(f"Error exporting trades: {e}")
        return []

def place_order(price, direction, take_profit_price, stop_loss_price): 
    """Place an order with the OANDA API including optional take-profit and stop-loss."""
    order_type = "MARKET_IF_TOUCHED"
    units = UNITS if direction == "BUY" else -UNITS

    order_data = {
        "order": {
            "instrument": INSTRUMENT,
            "units": str(units),
            "type": order_type,
            "price": str(round(price, 5)),
            "timeInForce": "GFD",
        }
    }
    if direction == "BUY" : # Use BID to trigger when market falls
        order_data["order"]["triggerCondition"]="BID"
    else:
        order_data["order"]["triggerCondition"]="ASK"

    order_data["order"]["takeProfitOnFill"] = {
            "price": str(round(take_profit_price, 5))
        }

    
    order_data["order"]["stopLossOnFill"] = {
            "price": str(round(stop_loss_price, 5))
        }

    try:
        order_request = OrderCreate(accountID=ACCOUNT_ID, data=order_data)
        response = client.request(order_request)
        print(f"Placed {direction} order at price: {round(price, 5)} | Response: {response}")
        data = response
        if "orderCreateTransaction" in response:
            return data["orderCreateTransaction"]["id"]
        else:
            print("Error:", response.text)
            return None
    except Exception as e:
        print(f"Failed to place {direction} order at price: {round(price, 5)} | Error: {e}")
        return None

def cancel_order(price):
    """Cancel an order at the specified price on OANDA's side."""
    price = round(price, 5)  # Ensure price is rounded to 5 decimal places
    if price in pending_trades_2:
        try:
            # Get the order ID from the dictionary
            order_id = pending_trades_2[price]
            if not order_id:
                print(f"No order ID found for price: {price}")
                return False

            # Send the cancel request to OANDA
            cancel_request = OrderCancel(accountID=ACCOUNT_ID, orderID=order_id)
            response = client.request(cancel_request)
            
            # A successful cancel returns a 'orderCancelTransaction'
            if 'orderCancelTransaction' in response:
                print(f"Order {order_id} successfully cancelled.")               
                return True
            else:
                print(f"Failed to cancel order {order_id}. Response: {response}")
                return False

        except Exception as e:
            print(f"Failed to cancel order at price: {price} | Error: {e}")
            return False
    else:
        print(f"No  order found at price: {price}")
        return False

def place_updated_orders_2(current_bid_price, current_ask_price,m,n,wait=1):
    """Place updated orders in both upward and downward directions."""
    current_ask_price = float(current_ask_price)
    current_bid_price = float(current_bid_price)
    for i in range(m, n+1):
        direction = "SELL" if i > 0 else "BUY"
        current_price = current_ask_price + 2*i * PIP_SIZE if direction == "BUY" else current_bid_price + 2*i * PIP_SIZE
        price = current_price
        if direction == "BUY":
           take_profit = price +  2*PIP_SIZE
           stop_loss = price - 2 * PIP_SIZE
        else:  # SELL
           take_profit = price - 2* PIP_SIZE
           stop_loss = price + 2 * PIP_SIZE

        order_id = place_order(price, direction, take_profit, stop_loss)
        if order_id is not None:  # Check if order was placed successfully
            pending_trades_2[round(price, 5)] = order_id
            if direction == "BUY":
                buy_orders_2.append(round(price, 5))
            else:
                sell_orders_2.append(round(price, 5))
        time.sleep(wait)  # Sleep to avoid hitting API limits
    # Sort the order books after placing initial orders
    buy_orders_2.sort()
    sell_orders_2.sort(reverse=True)  # Sort sell orders in descending order
def place_updated_orders_5(current_bid_price, current_ask_price,m,n,wait=1):
    """Place updated orders in both upward and downward directions."""
    current_ask_price = float(current_ask_price)
    current_bid_price = float(current_bid_price)
    for i in range(m, n+1):
        direction = "SELL" if i > 0 else "BUY"
        current_price = current_ask_price + 5*i * PIP_SIZE if direction == "BUY" else current_bid_price + 5*i * PIP_SIZE
        price = current_price
        if direction == "BUY":
           take_profit = price + 4*PIP_SIZE
           stop_loss = price - 2 * PIP_SIZE
        else:  # SELL
           take_profit = price -  4*PIP_SIZE
           stop_loss = price + 2 * PIP_SIZE

        order_id = place_order(price, direction, take_profit, stop_loss)
        if order_id is not None:  # Check if order was placed successfully
            pending_trades_5[round(price, 5)] = order_id
            if direction == "BUY":
                buy_orders_5.append(round(price, 5))
            else:
                sell_orders_5.append(round(price, 5))
        time.sleep(wait)  # Sleep to avoid hitting API limits
    # Sort the order books after placing initial orders
    buy_orders_5.sort()
    sell_orders_5.sort(reverse=True)  # Sort sell orders in descending order
def place_initial_orders_2(current_bid_price, current_ask_price,m,n,wait=1):
    """Place initial pending orders in both upward and downward directions."""
    current_ask_price = float(current_ask_price)
    current_bid_price = float(current_bid_price)
    for i in range(m, n+1):
        if i == 0 :
            continue
        
        direction = "BUY" if i > 0 else "SELL"
        current_price = current_ask_price + 2*i * PIP_SIZE if direction == "BUY" else current_bid_price + 2*i * PIP_SIZE
        price = current_price
        if direction == "BUY":
           take_profit = price +  PIP_SIZE
           stop_loss = price - 2 * PIP_SIZE
        else:  # SELL
           take_profit = price -  PIP_SIZE
           stop_loss = price + 2 * PIP_SIZE

        order_id = place_order(price, direction, take_profit, stop_loss)
        if order_id is not None:  # Check if order was placed successfully
            pending_trades_2[round(price, 5)] = order_id
            if direction == "BUY":
                buy_orders_2.append(round(price, 5))
            else:
                sell_orders_2.append(round(price, 5))
        time.sleep(wait)  # Sleep to avoid hitting API limits
    # Sort the order books after placing initial orders
    buy_orders_2.sort()
    sell_orders_2.sort(reverse=True)  # Sort sell orders in descending order
def place_initial_orders_5(current_bid_price, current_ask_price,m,n,wait=1):
    """Place initial pending orders in both upward and downward directions."""
    current_ask_price = float(current_ask_price)
    current_bid_price = float(current_bid_price)
    for i in range(m, n+1):
        if i == 0 :
            continue
        
        direction = "BUY" if i > 0 else "SELL"
        current_price = current_ask_price + 5*i * PIP_SIZE if direction == "BUY" else current_bid_price + 5*i * PIP_SIZE
        price = current_price
        if direction == "BUY":
           take_profit = price +  4*PIP_SIZE
           stop_loss = price - 2 * PIP_SIZE
        else:  # SELL
           take_profit = price -  4*PIP_SIZE
           stop_loss = price + 2 * PIP_SIZE

        order_id = place_order(price, direction, take_profit, stop_loss)
        if order_id is not None:  # Check if order was placed successfully
            pending_trades_5[round(price, 5)] = order_id
            if direction == "BUY":
                buy_orders_5.append(round(price, 5))
            else:
                sell_orders_5.append(round(price, 5))
        time.sleep(wait)  # Sleep to avoid hitting API limits
    # Sort the order books after placing initial orders
    buy_orders_5.sort()
    sell_orders_5.sort(reverse=True)  # Sort sell orders in descending order
"""
def monitor_and_update_filled_orders_2(current_bid_price, current_ask_price):
    # Monitor price changes and update filled orders.
    # Check if the current price is in the order books
    closed_buy_prices = [price for price in filled_buy_orders_2 if price <= current_ask_price]
    closed_buy_prices.sort
    closed_buy_prices.pop()
    closed_sell_prices = [price for price in filled_sell_orders_2 if price >= current_bid_price]
    closed_sell_prices.sort(reverse=True)
    closed_sell_prices.pop()
    closed_prices = closed_sell_prices + closed_buy_prices # Combine filled prices
    for price in closed_prices:
        print(f"Order at price {price} closed. Removing from filled_order_2 and order books.")
        del filled_orders_2[price]
        
        if price in filled_buy_orders_2:
           filled_buy_orders_2.remove(price)
        if price in filled_sell_orders_2:
           filled_sell_orders_2.remove(price)
    filled_buy_orders_2.sort()
    filled_sell_orders_2.sort(reverse=True)  # Sort sell orders in descending order

"""
def monitor_and_update_orders_2(current_bid_price, current_ask_price):
    """Monitor price changes and update orders based on conditions."""
    current_ask_price = float(current_ask_price)
    current_bid_price = float(current_bid_price)
    # Check if the current price is in the order books
    filled_prices_buy = [price for price in buy_orders_2 if price <= current_ask_price]
    filled_prices_sell = [price for price in sell_orders_2 if price >= current_bid_price]
    filled_prices = filled_prices_sell + filled_prices_buy # Combine filled prices
    for price in filled_prices:
        print(f"Order at price {price} filled. Removing from pending_trades_2 and order books.")
        filled_orders_2[price] = pending_trades_2[price]
        del pending_trades_2[price]
        if price in buy_orders_2:
           log_filled_order("pending_trades_2", price, filled_orders_2[price], "BUY", current_ask_price, "filled_orders_2_log.csv")
           buy_orders_2.remove(price)
        if price in sell_orders_2:
           log_filled_order("pending_trades_2", price, filled_orders_2[price], "SELL", current_bid_price, "filled_orders_2_log.csv")
           sell_orders_2.remove(price)
    buy_orders_2.sort()
    sell_orders_2.sort(reverse=True)  # Sort sell orders in descending order
    """
    if not filled_prices:
       print("No orders filled.")
       print(f"ask price:{current_ask_price} is between sell_order: {sell_orders_2[0]} and buy_order:{buy_orders_2[0]}" )
    print("volatility is low, no action taken.")
    """
    if len(filled_prices_sell)<=0 and len(filled_prices_buy)>0:
        print(f"No sell orders filled but {len(filled_prices_buy)} buy orders filled.")
        price_diff = current_bid_price - sell_orders_2[0]
        price_remainder = price_diff % (2*PIP_SIZE)
        if price_remainder > 1.8*PIP_SIZE:
           order_to_place = int(price_diff/(2*PIP_SIZE))
        else:
           order_to_place = int(price_diff/(2*PIP_SIZE)) - 1
        place_updated_orders_2(sell_orders_2[0],sell_orders_2[0] +PIP_SIZE ,1,order_to_place,wait=0.1)
        
    if len(filled_prices_buy)<=0 and len(filled_prices_sell)>0:
        print(f"No buy orders filled but {len(filled_prices_sell)} sell orders filled.")
        price_diff = buy_orders_2[0] - current_ask_price
        price_remainder = price_diff % (2*PIP_SIZE)
        if price_remainder > 1.8*PIP_SIZE:
           order_to_place = int(price_diff/(2*PIP_SIZE))
        else:
           order_to_place = int(price_diff/(2*PIP_SIZE)) - 1
        place_updated_orders_2(buy_orders_2[0] - PIP_SIZE,buy_orders_2[0] ,-order_to_place,-1,wait=0.1)

    # Case 1: Price changes downward more than 1 pip and less than 2 pips
    bid_price_change = current_bid_price - sell_orders_2[0]
    ask_price_change = buy_orders_2[0] - current_ask_price 
    if bid_price_change < PIP_SIZE :
        cancel_order(sell_orders_2[0])
        # Remove the order from the dictionary
        del pending_trades_2[sell_orders_2[0]]
        if sell_orders_2[0] in sell_orders_2:
            sell_orders_2.remove(sell_orders_2[0])
        sell_orders_2.sort(reverse=True)  # Sort sell orders in descending order         
        print(f"Bid price and sell_order price {sell_orders_2[0]} difference is less than 1 pip. Cancelled sell order at price: price: {sell_orders_2[0]}")
    elif bid_price_change >=3.5*PIP_SIZE:
        place_updated_orders_2(sell_orders_2[0],sell_orders_2[0] +PIP_SIZE ,1,1,wait=0.1)
        print(f"Bid price and sell_order price {sell_orders_2[0]} difference is greater than 3.5 pip. place sell order at price: price: {sell_orders_2[0]}")
    # Case 2: Price changes downward more than 1 pip and less than 2 pips
    if ask_price_change < PIP_SIZE:
        cancel_order(buy_orders_2[0])
        # Remove the order from the dictionary
        del pending_trades_2[buy_orders_2[0]]
        if buy_orders_2[0] in buy_orders_2:
            buy_orders_2.remove(buy_orders_2[0])
        buy_orders_2.sort()        
        print(f"Ask price and buy_order price: {buy_orders_2[0]} difference is less than 1 pip. Cancelled buy order at price: {buy_orders_2[0]}")
    elif ask_price_change >=3.5*PIP_SIZE:
        place_updated_orders_2(buy_orders_2[0] - PIP_SIZE,buy_orders_2[0] ,-1,-1,wait=0.1)
        print(f"Ask price and buy_order price: {buy_orders_2[0]} difference is greater than 3.6 pip. place buy order at price: {buy_orders_2[0]}")
def monitor_and_update_orders_5(current_bid_price, current_ask_price):
    """Monitor price changes and update orders based on conditions."""
    current_ask_price = float(current_ask_price)
    current_bid_price = float(current_bid_price)
    # Check if the current price is in the order books
    filled_prices_buy = [price for price in buy_orders_5 if price <= current_ask_price]
    filled_prices_sell = [price for price in sell_orders_5 if price >= current_bid_price]
    filled_prices = filled_prices_sell + filled_prices_buy # Combine filled prices
    for price in filled_prices:
        print(f"Order at price {price} filled. Removing from pending_trades_5 and order books.")
        filled_orders_5[price] = pending_trades_5[price]
        del pending_trades_5[price]
        if price in buy_orders_5:
           log_filled_order("pending_trades_5", price, filled_orders_5[price], "BUY", current_ask_price, "filled_orders_5_log.csv")
           buy_orders_5.remove(price)
        if price in sell_orders_5:
           log_filled_order("pending_trades_5", price, filled_orders_5[price], "SELL", current_bid_price, "filled_orders_5_log.csv")
           sell_orders_5.remove(price)
    buy_orders_5.sort()
    sell_orders_5.sort(reverse=True)  # Sort sell orders in descending order
    """
    if not filled_prices:
       print("No orders filled.")
       print(f"ask price:{current_ask_price} is between sell_order: {sell_orders_5[0]} and buy_order:{buy_orders_5[0]}" )
       print("volatility is low, no action taken.")
    """
    if len(filled_prices_sell)<=0 and len(filled_prices_buy)>0:
        print(f"No sell orders filled but {len(filled_prices_buy)} buy orders filled.")
        price_diff = current_bid_price - sell_orders_5[0]
        price_remainder = price_diff % 5*PIP_SIZE
        if price_remainder > 4.5*PIP_SIZE:
           order_to_place = int(price_diff/5*PIP_SIZE)
        else:
           order_to_place = int(price_diff/5*PIP_SIZE) - 1
        place_updated_orders_5(sell_orders_5[0],sell_orders_5[0] +PIP_SIZE ,1,order_to_place,wait=0.1)
        
    if len(filled_prices_buy)<=0 and len(filled_prices_sell)>0:
        print(f"No buy orders filled but {len(filled_prices_sell)} sell orders filled.")
        price_diff = buy_orders_5[0] - current_ask_price
        price_remainder = price_diff % 5*PIP_SIZE
        if price_remainder > 4.5*PIP_SIZE:
           order_to_place = int(price_diff/5*PIP_SIZE)
        else:
           order_to_place = int(price_diff/5*PIP_SIZE) - 1
        place_updated_orders_5(buy_orders_5[0] - PIP_SIZE,buy_orders_5[0] ,-order_to_place,-1,wait=0.1)

    # Case 1: Price changes downward more than 1 pip and less than 5 pips
    bid_price_change = current_bid_price - sell_orders_5[0]
    ask_price_change = buy_orders_5[0] - current_ask_price 
    if bid_price_change < 2.5*PIP_SIZE :
        cancel_order(sell_orders_5[0])
        # Remove the order from the dictionary
        del pending_trades_5[sell_orders_5[0]]
        if sell_orders_5[0] in sell_orders_5:
            sell_orders_5.remove(sell_orders_5[0])
        sell_orders_5.sort(reverse=True)  # Sort sell orders in descending order         
        print(f"Bid price changed downward more than 1 pip. Cancelled sell order at price: {sell_orders_5[0]}")
    elif bid_price_change >=9.5*PIP_SIZE:
        place_updated_orders_5(sell_orders_5[0],sell_orders_5[0] +PIP_SIZE ,1,1,wait=0.1)
    # Case 5: Price changes downward more than 1 pip and less than 5 pips
    if ask_price_change < 2.5*PIP_SIZE:
        cancel_order(buy_orders_5[0])
        # Remove the order from the dictionary
        del pending_trades_5[buy_orders_5[0]]
        if buy_orders_5[0] in buy_orders_5:
            buy_orders_5.remove(buy_orders_5[0])
        buy_orders_5.sort()        
        print(f"Ask price changed upward more than 1 pip. Cancelled buy order at price: {buy_orders_5[0]}")
    elif ask_price_change >=9.5*PIP_SIZE:
        place_updated_orders_5(buy_orders_5[0] - PIP_SIZE,buy_orders_5[0] ,-1,-1,wait=0.1)

def main():
    """Main function to place initial orders and monitor price changes."""
    global last_price
    try:
        # Fetch the initial price and place initial orders
        initial_price_data = get_latest_price()
        if not initial_price_data:
            print("Failed to fetch the initial price.")
            return
        last_price = initial_price_data
        print(f"Initial price: {last_price}")
        place_initial_orders_2(last_price['bid'], last_price['ask'],-NUM_ORDERS_2,NUM_ORDERS_2)  # Place initial orders with a range of -50 to 50
        place_initial_orders_5(last_price['bid'], last_price['ask'],-NUM_ORDERS_5,NUM_ORDERS_5)  # Place initial orders with a range of -15 to 15
        # Continuously monitor price changes
        fetch_flag = 0
        while True:
            latest_price_data = get_latest_price()
            if latest_price_data:
                current_bid_price = float(latest_price_data["bid"])
                current_ask_price = float(latest_price_data["ask"])
                print(f"Latest Price - Time: {latest_price_data['timestamp']} | Bid: {latest_price_data['bid']} | Ask: {latest_price_data['ask']}")
                monitor_and_update_orders_2(current_bid_price, current_ask_price)
                #monitor_and_update_orders_5(current_bid_price, current_ask_price)
                price_ask_change = abs(current_ask_price - float(last_price["ask"]))
                price_bid_change = abs(current_bid_price - float(last_price["bid"]))
                last_price = latest_price_data

                print(f"Price Change - Bid: {price_bid_change/PIP_SIZE}pips | Ask: {price_ask_change/PIP_SIZE}pips")
            if fetch_flag == 600:
                fetch_recent_closed_transactions(5)  # Fetch recent closed transactions every 5 minutes
                fetch_flag = 0
            fetch_flag += 1
            if fetch_flag % 60 == 0:
                print(f"pending_trades_2: {pending_trades_2} | buy_orders_2: {buy_orders_2} | sell_orders_2: {sell_orders_2}")
                print(f"filled_orders_2: {filled_orders_2} | filled_buy_orders_2: {filled_buy_orders_2} | filled_sell_orders_2: {filled_sell_orders_2}")
            time.sleep(1)  # Wait for 0.5 second before fetching the next price
    except KeyboardInterrupt:
        print("Price monitoring stopped by user.")


if __name__ == "__main__":
    main()
