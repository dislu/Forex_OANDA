import oandapyV20
import oandapyV20.endpoints.pricing as pricing
from oandapyV20.endpoints.orders import OrderCreate, OrderCancel
from oandapyV20.endpoints.transactions import TransactionList
from config import ACCOUNT_ID, ACCESS_TOKEN
from datetime import datetime, timedelta, timezone
from dateutil import parser
import pytz
import time
import csv

UNITS = 1000
INSTRUMENT = "EUR_USD"
PIP_SIZE = 0.0001
NUM_ORDERS_2 = 5

# Order books and pending/fill tracking
buy_orders_2 = []
sell_orders_2 = []
pending_trades_2 = {}
filled_orders_2 = {}

# Initialize API client
client = oandapyV20.API(access_token=ACCESS_TOKEN)

# ------------------------- Helper Functions -------------------------

def get_latest_price():
    pricing_stream = pricing.PricingStream(accountID=ACCOUNT_ID, params={"instruments": INSTRUMENT})
    try:
        for r in client.request(pricing_stream):
            if r['type'] == 'PRICE':
                bids = r['bids'][0]['price']
                asks = r['asks'][0]['price']
                utc_time = parser.parse(r['time'])
                sgt_time = utc_time.astimezone(pytz.timezone("Asia/Singapore"))
                formatted_time = sgt_time.strftime("%Y-%m-%d %H:%M:%S %Z%z")
                return {"timestamp": formatted_time, "bid": bids, "ask": asks}
    except Exception as e:
        print(f"Error fetching latest price: {e}")
    return None


def log_filled_order(order_type, price, order_id, direction, fill_price, log_file):
    with open(log_file, "a", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            datetime.now().isoformat(), order_type, price, order_id, direction, fill_price
        ])


def fetch_recent_closed_transactions(minutes=5, filter_type="all", instrument=None, filename="profit_loss_trades.csv"):
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
            return []

        with open(filename, mode="w", newline="") as file:
            writer = csv.writer(file)
            writer.writerow(["Time", "Instrument", "Units", "Side", "Price", "P/L"])
            for tx in filtered:
                side = "BUY" if int(tx["units"]) > 0 else "SELL"
                writer.writerow([tx["time"], tx["instrument"], tx["units"], side, tx["price"], tx["pl"]])

        return filtered

    except Exception as e:
        print(f"Error exporting trades: {e}")
        return []


def place_order(price, direction, take_profit_price, stop_loss_price):
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

    if direction == "BUY":
        order_data["order"]["triggerCondition"] = "BID"
    else:
        order_data["order"]["triggerCondition"] = "ASK"

    order_data["order"]["takeProfitOnFill"] = {"price": str(round(take_profit_price, 5))}
    order_data["order"]["stopLossOnFill"] = {"price": str(round(stop_loss_price, 5))}

    try:
        order_request = OrderCreate(accountID=ACCOUNT_ID, data=order_data)
        response = client.request(order_request)
        if "orderCreateTransaction" in response:
            return response["orderCreateTransaction"]["id"]
        return None
    except Exception as e:
        print(f"Failed to place {direction} order at price {price}: {e}")
        return None

# ------------------------- Breakout Functions -------------------------

def place_initial_orders_2_breakout(current_bid, current_ask, m, n, wait=1):
    for i in range(m, n + 1):
        if i == 0: continue
        if i > 0:
            direction = "BUY"
            price = current_ask + 2 * i * PIP_SIZE
            tp = price + 2 * PIP_SIZE
            sl = price - 4 * PIP_SIZE
        else:
            direction = "SELL"
            price = current_bid + 2 * i * PIP_SIZE
            tp = price - 2 * PIP_SIZE
            sl = price + 4 * PIP_SIZE

        order_id = place_order(price, direction, tp, sl)
        if order_id:
            pending_trades_2[round(price, 5)] = order_id
            if direction == "BUY": buy_orders_2.append(round(price, 5))
            else: sell_orders_2.append(round(price, 5))
        time.sleep(wait)

    buy_orders_2.sort()
    sell_orders_2.sort(reverse=True)


def place_updated_orders_2_breakout(current_bid, current_ask, m, n, wait=1):
    # Same as initial but dynamically replenishes ladder
    for i in range(m, n + 1):
        if i > 0:
            direction = "BUY"
            price = current_ask + 2 * i * PIP_SIZE
            tp = price + 2 * PIP_SIZE
            sl = price - 2 * PIP_SIZE
        else:
            direction = "SELL"
            price = current_bid + 2 * i * PIP_SIZE
            tp = price - 2 * PIP_SIZE
            sl = price + 2 * PIP_SIZE

        order_id = place_order(price, direction, tp, sl)
        if order_id:
            pending_trades_2[round(price, 5)] = order_id
            if direction == "BUY": buy_orders_2.append(round(price, 5))
            else: sell_orders_2.append(round(price, 5))
        time.sleep(wait)

    buy_orders_2.sort()
    sell_orders_2.sort(reverse=True)


def monitor_and_update_orders_2_breakout(current_bid, current_ask):
    current_bid = float(current_bid)
    current_ask = float(current_ask)

    filled_buy = [p for p in buy_orders_2 if p <= current_ask]
    filled_sell = [p for p in sell_orders_2 if p >= current_bid]
    filled = filled_buy + filled_sell

    for price in filled:
        order_id = pending_trades_2.get(price)
        if order_id:
            filled_orders_2[price] = order_id
            del pending_trades_2[price]
            if price in buy_orders_2:
                log_filled_order("pending_trades_2", price, order_id, "BUY", current_ask, "filled_orders_2_log.csv")
                buy_orders_2.remove(price)
            if price in sell_orders_2:
                log_filled_order("pending_trades_2", price, order_id, "SELL", current_bid, "filled_orders_2_log.csv")
                sell_orders_2.remove(price)

    buy_orders_2.sort()
    sell_orders_2.sort(reverse=True)

    # Replenish ladder
    if filled_buy:
        highest_buy = max(filled_buy)
        place_updated_orders_2_breakout(highest_buy, highest_buy + PIP_SIZE, 1, len(filled_buy), wait=0.1)
    if filled_sell:
        lowest_sell = min(filled_sell)
        place_updated_orders_2_breakout(lowest_sell - PIP_SIZE, lowest_sell, -len(filled_sell), -1, wait=0.1)

# ------------------------- Main Loop -------------------------

def main():
    """Main function to run breakout MIT strategy."""
    global last_price
    try:
        # Step 1: Fetch initial price
        initial_price_data = get_latest_price()
        if not initial_price_data:
            print("Failed to fetch initial price.")
            return

        last_price = initial_price_data
        print(f"Initial price: {last_price}")

        # Step 2: Place initial breakout orders (2-pip ladder)
        place_initial_orders_2_breakout(
            float(last_price['bid']), float(last_price['ask']), -NUM_ORDERS_2, NUM_ORDERS_2
        )

        # Step 3: Continuous monitoring
        fetch_flag = 0
        while True:
            latest_price_data = get_latest_price()
            if latest_price_data:
                current_bid = float(latest_price_data["bid"])
                current_ask = float(latest_price_data["ask"])

                print(f"Latest Price - Time: {latest_price_data['timestamp']} | Bid: {current_bid} | Ask: {current_ask}")

                # Step 4: Monitor and update breakout orders
                monitor_and_update_orders_2_breakout(current_bid, current_ask)

                # Track price change since last fetch
                bid_change = abs(current_bid - float(last_price["bid"]))
                ask_change = abs(current_ask - float(last_price["ask"]))
                print(f"Price Change - Bid: {bid_change / PIP_SIZE} pips | Ask: {ask_change / PIP_SIZE} pips")
                last_price = latest_price_data

            # Step 5: Periodic fetch of closed transactions
            if fetch_flag >= 300:  # Every 5 minutes
                fetch_recent_closed_transactions(5)
                fetch_flag = 0
            fetch_flag += 1

            # Step 6: Debug print for pending and filled orders every 60 seconds
            if fetch_flag % 60 == 0:
                print(f"Pending Trades: {pending_trades_2}")
                print(f"Buy Orders: {buy_orders_2} | Sell Orders: {sell_orders_2}")
                print(f"Filled Orders: {filled_orders_2}")

            time.sleep(1)  # Wait before next price fetch

    except KeyboardInterrupt:
        print("Breakout MIT monitoring stopped by user.")

def cancel_all_orders():
    """Cancel all pending orders in pending_trades_2."""
    for price, order_id in list(pending_trades_2.items()):
        try:
            cancel_request = OrderCancel(accountID=ACCOUNT_ID, orderID=order_id)
            response = client.request(cancel_request)
            print(f"Cancelled order {order_id} at price {price}")
            # Remove from pending_trades_2 and order lists
            del pending_trades_2[price]
            if price in buy_orders_2:
                buy_orders_2.remove(price)
            if price in sell_orders_2:
                sell_orders_2.remove(price)
        except Exception as e:
            print(f"Failed to cancel order {order_id} at price {price}: {e}")

if __name__ == "__main__":
    main()
