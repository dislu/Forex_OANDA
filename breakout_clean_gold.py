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
import threading

class PriceStreamHandler:
    def __init__(self, client, account_id, instrument):
        self.client = client
        self.account_id = account_id
        self.instrument = instrument
        self.current_price = {}
        self._running = False

    def _stream(self):
        ps = pricing.PricingStream(accountID=self.account_id, params={"instruments": self.instrument})
        for r in self.client.request(ps):
            if not self._running:
                break
            if r["type"] == "PRICE":
                self.current_price["bid"] = float(r["bids"][0]["price"])
                self.current_price["ask"] = float(r["asks"][0]["price"])
                utc_time = parser.parse(r["time"])
                sgt_time = utc_time.astimezone(pytz.timezone("Asia/Singapore"))
                self.current_price["time"] = sgt_time.strftime("%Y-%m-%d %H:%M:%S %Z%z")

    def start(self):
        self._running = True
        threading.Thread(target=self._stream, daemon=True).start()

    def stop(self):
        self._running = False

class BreakoutTrader:
    def __init__(self, account_id, token, instrument="XAU_USD", units=1, pip_size=0.01):
        self.client = oandapyV20.API(access_token=token)
        self.account_id = account_id
        self.instrument = instrument
        self.units = units
        self.pip_size = pip_size

        self.buy_orders = []
        self.sell_orders = []
        self.pending_trades = {}
        self.filled_orders = {}

        self.stream = PriceStreamHandler(self.client, self.account_id, self.instrument)

    def place_order(self, price, direction, tp, sl):
        order_type = "MARKET_IF_TOUCHED"
        units = self.units if direction == "BUY" else -self.units

        order_data = {
            "order": {
                "instrument": self.instrument,
                "units": str(units),
                "type": order_type,
                "price": str(round(price, 2)),
                "timeInForce": "GFD",
                "triggerCondition": "BID" if direction == "BUY" else "ASK",
                "takeProfitOnFill": {"price": str(round(tp, 2))},
                "stopLossOnFill": {"price": str(round(sl, 2))},
            }
        }

        try:
            order_request = OrderCreate(accountID=self.account_id, data=order_data)
            response = self.client.request(order_request)
            if "orderCreateTransaction" in response:
                return response["orderCreateTransaction"]["id"]
        except Exception as e:
            print(f"Failed to place {direction} order at {price}: {e}")
        return None

    def cancel_order(self, order_id):
        try:
            r = OrderCancel(accountID=self.account_id, orderID=order_id)
            self.client.request(r)
            print(f"Cancelled order {order_id}")
        except Exception as e:
            print(f"Failed to cancel order {order_id}: {e}")

    def place_ladder_orders(self, bid, ask, m, n, wait=0.5):
        for i in range(m, n + 1):
            if i == 0:
                continue
            if i > 0:
                direction = "BUY"
                price = ask + 5 * i * self.pip_size
                tp, sl = price + 5 * self.pip_size, price - 5 * self.pip_size
            else:
                direction = "SELL"
                price = bid + 5 * i * self.pip_size
                tp, sl = price - 5 * self.pip_size, price + 5 * self.pip_size

            order_id = self.place_order(price, direction, tp, sl)
            if order_id:
                self.pending_trades[round(price, 2)] = order_id
                if direction == "BUY":
                    self.buy_orders.append(round(price, 2))
                else:
                    self.sell_orders.append(round(price, 2))
            time.sleep(wait)

        self.buy_orders.sort()
        self.sell_orders.sort(reverse=True)

    def monitor_fills(self, bid, ask):
        filled_buy = [p for p in self.buy_orders if p <= ask]
        filled_sell = [p for p in self.sell_orders if p >= bid]
        filled = filled_buy + filled_sell

        for price in filled:
            order_id = self.pending_trades.get(price)
            if order_id:
                self.filled_orders[price] = order_id
                del self.pending_trades[price]
                if price in self.buy_orders:
                    self.buy_orders.remove(price)
                    self.log_fill("BUY", price, order_id, ask)
                if price in self.sell_orders:
                    self.sell_orders.remove(price)
                    self.log_fill("SELL", price, order_id, bid)

        if filled_buy:
            highest = max(filled_buy)
            self.place_ladder_orders(highest, ask, 1, len(filled_buy), wait=0.1)
        if filled_sell:
            lowest = min(filled_sell)
            self.place_ladder_orders(bid, lowest, -len(filled_sell), -1, wait=0.1)

    def log_fill(self, side, price, order_id, fill_price, log_file="xau_fills.csv"):
        with open(log_file, "a", newline="") as f:
            writer = csv.writer(f)
            writer.writerow([datetime.now().isoformat(), side, price, order_id, fill_price])

    def export_recent_trades(self, minutes=5, filename="xau_closed_trades.csv"):
        try:
            now = datetime.now(timezone.utc)
            since_time = now - timedelta(minutes=minutes)
            params = {"from": since_time.strftime("%Y-%m-%dT%H:%M:%SZ"), "type": "ORDER_FILL"}

            r = TransactionList(accountID=self.account_id, params=params)
            resp = self.client.request(r)
            transactions = resp.get("transactions", [])

            if not transactions:
                return []

            with open(filename, "a", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(["Time", "Instrument", "Units", "Side", "Price", "P/L"])
                for tx in transactions:
                    side = "BUY" if int(tx["units"]) > 0 else "SELL"
                    writer.writerow([tx["time"], tx["instrument"], tx["units"], side, tx["price"], tx["pl"]])

            return transactions
        except Exception as e:
            print(f"Error exporting trades: {e}")
            return []

    def run(self, num_orders=5):
        print("Starting BreakoutTrader for XAU_USD...")
        self.stream.start()

        while not self.stream.current_price:
            time.sleep(0.5)
        bid, ask = self.stream.current_price["bid"], self.stream.current_price["ask"]

        self.place_ladder_orders(bid, ask, -num_orders, num_orders)

        fetch_flag = 0
        try:
            while True:
                if not self.stream.current_price:
                    continue
                bid, ask = self.stream.current_price["bid"], self.stream.current_price["ask"]

                print(f"[{self.stream.current_price['time']}] Bid: {bid} | Ask: {ask}")

                self.monitor_fills(bid, ask)

                if fetch_flag >= 300:
                    self.export_recent_trades(5)
                    fetch_flag = 0
                fetch_flag += 1

                time.sleep(1)
        except KeyboardInterrupt:
            print("Stopped by user.")
            self.stream.stop()

if __name__ == "__main__":
    trader = BreakoutTrader(
        account_id=ACCOUNT_ID,
        token=ACCESS_TOKEN,
        instrument="XAU_USD",
        units=1,
        pip_size=0.01
    )
    trader.run(num_orders=5)