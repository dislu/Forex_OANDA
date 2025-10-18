import oandapyV20
import oandapyV20.endpoints.pricing as pricing
import oandapyV20.endpoints.instruments as instruments
from oandapyV20.endpoints.orders import OrderCreate, OrderCancel, OrderList
from oandapyV20.endpoints.transactions import TransactionList
from config import ACCOUNT_ID, ACCESS_TOKEN
from datetime import datetime, timezone, timedelta
from dateutil import parser
import pytz
import time
import threading
import numpy as np
import csv


class PriceStreamHandler:
    """Handles OANDA price streaming in a background thread."""

    def __init__(self, client, account_id, instrument):
        self.client = client
        self.account_id = account_id
        self.instrument = instrument
        self.current_price = {}
        self._running = False

    def _stream(self):
        ps = pricing.PricingStream(
            accountID=self.account_id, params={"instruments": self.instrument}
        )
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
    """Breakout MIT ladder trading strategy with ATR-based TP/SL."""

    def __init__(self, account_id, token, instrument="XAU_USD", units=1, pip_size=0.01):
        self.client = oandapyV20.API(access_token=token)
        self.account_id = account_id
        self.instrument = instrument
        self.units = units
        self.pip_size = pip_size

        self.stream = PriceStreamHandler(self.client, self.account_id, self.instrument)

    # ---------------- ATR Calculation ----------------
    def get_atr(self, period=14, granularity="M5"):
        """Fetch candles and calculate ATR."""
        params = {"count": period+1, "granularity": granularity, "price": "M"}
        r = instruments.InstrumentsCandles(instrument=self.instrument, params=params)
        candles = self.client.request(r)["candles"]

        highs = [float(c["mid"]["h"]) for c in candles if c["complete"]]
        lows = [float(c["mid"]["l"]) for c in candles if c["complete"]]
        closes = [float(c["mid"]["c"]) for c in candles if c["complete"]]

        trs = []
        for i in range(1, len(closes)):
            high, low, prev_close = highs[i], lows[i], closes[i-1]
            tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
            trs.append(tr)

        return np.mean(trs[-period:]) if trs else 0.0

    # ---------------- Order Management ----------------
    def place_order(self, price, direction, tp, sl):
        order_type = "MARKET_IF_TOUCHED"
        units = self.units if direction == "BUY" else -self.units

        order_data = {
            "order": {
                "instrument": self.instrument,
                "units": str(units),
                "type": order_type,
                "price": str(round(price, 5)),
                "timeInForce": "GFD",
                "triggerCondition": "BID" if direction == "BUY" else "ASK",
                "takeProfitOnFill": {"price": str(round(tp, 5))},
                "stopLossOnFill": {"price": str(round(sl, 5))},
            }
        }

        try:
            order_request = OrderCreate(accountID=self.account_id, data=order_data)
            response = self.client.request(order_request)
            if "orderCreateTransaction" in response:
                oid = response["orderCreateTransaction"]["id"]
                print(f"Placed {direction} order {oid} at {price} TP={tp} SL={sl}")
                return oid
        except Exception as e:
            print(f"Failed to place {direction} order at {price}: {e}")
        return None

    def cancel_all_orders(self):
        """Cancel all pending orders in account."""
        try:
            r = OrderList(accountID=self.account_id)
            open_orders = self.client.request(r).get("orders", [])
            for o in open_orders:
                order_id = o["id"]
                cancel = OrderCancel(accountID=self.account_id, orderID=order_id)
                self.client.request(cancel)
                print(f"Cancelled order {order_id}")
        except Exception as e:
            print(f"Error cancelling orders: {e}")

    # ---------------- Ladder Placement ----------------
    def place_ladder_orders_with_atr(self, bid, ask, num_orders=5, step_pips=3, tp_mult=1.0, sl_mult=1.5):
        atr = self.get_atr()
        if atr == 0:
            print("ATR not available.")
            return

        step = (step_pips +2) * self.pip_size
        for i in range(1, num_orders + 1):
            # Buy ladder above ask
            buy_price = ask + i * step
            tp_buy = buy_price + atr * tp_mult
            sl_buy = buy_price - atr * sl_mult
            self.place_order(buy_price, "BUY", tp_buy, sl_buy)

            # Sell ladder below bid
            sell_price = bid - i * step
            tp_sell = sell_price - atr * tp_mult
            sl_sell = sell_price + atr * sl_mult
            self.place_order(sell_price, "SELL", tp_sell, sl_sell)

    # ---------------- Trade Export ----------------
    def export_cycle_trades(self, minutes=2, filename="cycle_trades.csv"):
        """Export all fills from the last `minutes` minutes to CSV."""
        try:
            now = datetime.now(timezone.utc)
            since_time = now - timedelta(minutes=minutes)
            params = {"from": since_time.strftime("%Y-%m-%dT%H:%M:%SZ"), "type": "ORDER_FILL"}

            r = TransactionList(accountID=self.account_id, params=params)
            resp = self.client.request(r)
            transactions = resp.get("transactions", [])

            if not transactions:
                print("No trades in this cycle.")
                return []

            with open(filename, "a", newline="") as f:
                writer = csv.writer(f)
                # Write header only if file is empty
                if f.tell() == 0:
                    writer.writerow(["Time", "Instrument", "Side", "Units", "Price", "P/L"])

                for tx in transactions:
                    side = "BUY" if int(tx["units"]) > 0 else "SELL"
                    writer.writerow([
                        tx["time"], tx["instrument"], side, tx["units"], tx["price"], tx["pl"]
                    ])
            print(f"Saved {len(transactions)} trades to {filename}")
            return transactions

        except Exception as e:
            print(f"Error exporting trades: {e}")
            return []

    # ---------------- Run Cycle ----------------
    def run_cycle(self, runtime=120, num_orders=5, step_pips=3, tp_mult=1.0, sl_mult=1.5):
        """Run one cycle: recalc ATR, place ladder orders, wait runtime sec, cancel all, export trades."""
        # Step 1: Recalculate ATR
        atr = self.get_atr()
        if atr == 0:
            print("ATR not available, skipping cycle.")
            return
        print(f"ATR = {atr:.5f}")

        # Step 2: Ensure we have current price
        while not self.stream.current_price:
            time.sleep(0.5)
        bid, ask = self.stream.current_price["bid"], self.stream.current_price["ask"]

        # Step 3: Place ladder
        self.place_ladder_orders_with_atr(
            bid, ask, num_orders=num_orders,
            step_pips=step_pips, tp_mult=tp_mult, sl_mult=sl_mult
        )

        # Step 4: Let orders run
        print(f"Running ladder for {runtime} seconds...")
        time.sleep(runtime)

        # Step 5: Cancel all
        print("Cancelling all pending orders...")
        self.cancel_all_orders()

        # Step 6: Export executed trades from this cycle
        self.export_cycle_trades(minutes=runtime/60, filename="cycle_trades.csv")


if __name__ == "__main__":
    trader = BreakoutTrader(
        ACCOUNT_ID, ACCESS_TOKEN,
        instrument="XAU_USD", units=1, pip_size=0.01
    )
    trader.stream.start()

    try:
        while True:
            trader.run_cycle(
                runtime=120,   # 2 minutes
                num_orders=5,  # 5 buys + 5 sells
                step_pips=3,   # spacing in pips
                tp_mult=1.0,   # TP = 1 x ATR
                sl_mult=1.5    # SL = 1.5 x ATR
            )
            print("Cycle complete. Restarting...\n")
    except KeyboardInterrupt:
        print("Stopped by user.")
        trader.stream.stop()
