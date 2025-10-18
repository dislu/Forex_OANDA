import oandapyV20
import oandapyV20.endpoints.pricing as pricing
import oandapyV20.endpoints.instruments as instruments
from oandapyV20.endpoints.orders import OrderCreate, OrderCancel, OrderList
from oandapyV20.endpoints.transactions import TransactionList
from oandapyV20.endpoints.trades import TradesList

from config import ACCOUNT_ID, ACCESS_TOKEN
from datetime import datetime, timezone, timedelta
from dateutil import parser
import pytz
import time
import threading
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
                # prices come as strings
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
    """Serial (one active order at a time) breakout MIT ladder with ATR TP/SL and cycle export."""

    def __init__(
        self, account_id, token, instrument="EUR_USD", units=1000, pip_size=0.0001
    ):
        self.client = oandapyV20.API(access_token=token)
        self.account_id = account_id
        self.instrument = instrument
        self.units = units
        self.pip_size = pip_size

        self.stream = PriceStreamHandler(self.client, self.account_id, self.instrument)

    # ---------------- ATR Calculation ----------------
    def get_atr(self, period=14, granularity="M5"):
        """
        Calculate ATR using 'period' completed candles at given granularity.
        Returns ATR as price units (not pips).
        """
        params = {"count": period + 1, "granularity": granularity, "price": "M"}
        r = instruments.InstrumentsCandles(instrument=self.instrument, params=params)
        resp = self.client.request(r)
        candles = resp.get("candles", [])

        highs = [float(c["mid"]["h"]) for c in candles if c.get("complete")]
        lows = [float(c["mid"]["l"]) for c in candles if c.get("complete")]
        closes = [float(c["mid"]["c"]) for c in candles if c.get("complete")]

        trs = []
        for i in range(1, len(closes)):
            high, low, prev_close = highs[i], lows[i], closes[i - 1]
            tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
            trs.append(tr)

        if not trs:
            return 0.0
        # simple moving average ATR
        return sum(trs[-period:]) / min(len(trs[-period:]), period)

    # ---------------- Order Management ----------------
    def place_order(self, price, direction, tp, sl):
        """
        Place a MARKET_IF_TOUCHED order (MIT). Returns order_id on success or None.
        """
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
                print(f"Placed {direction} MIT order {oid} @ {price:.5f} TP={tp:.5f} SL={sl:.5f}")
                return oid
            # There are other response shapes; try to find an id conservatively
            for k in ("orderCreateTransaction", "orderFillTransaction"):
                if k in response and "id" in response[k]:
                    return response[k]["id"]
        except Exception as e:
            print(f"Failed to place {direction} order at {price}: {e}")
        return None

    def cancel_order(self, order_id):
        try:
            cancel = OrderCancel(accountID=self.account_id, orderID=order_id)
            self.client.request(cancel)
            print(f"Cancelled order {order_id}")
        except Exception as e:
            print(f"Failed to cancel order {order_id}: {e}")

    def cancel_all_orders(self):
        """Cancel all pending orders for the account."""
        try:
            r = OrderList(accountID=self.account_id)
            open_orders = self.client.request(r).get("orders", [])
            for o in open_orders:
                oid = o.get("id")
                if oid:
                    self.cancel_order(oid)
        except Exception as e:
            print(f"Error cancelling all orders: {e}")

    # ----------------- Helpers to wait for lifecycle ----------------
    def _fetch_transactions_since(self, since_dt):
        """Helper: return transactions since 'since_dt' (UTC)."""
        params = {"from": since_dt.strftime("%Y-%m-%dT%H:%M:%SZ")}
        r = TransactionList(accountID=self.account_id, params=params)
        resp = self.client.request(r)
        return resp.get("transactions", [])

    def wait_for_fill_and_close(self, order_id, start_time, timeout):
        """
        Wait for an ORDER_FILL for order_id, then wait until the resulting trade is closed.
        Returns (filled, closed, trade_id or None).
        - filled: bool (whether order was filled before timeout)
        - closed: bool (whether resulting trade was closed before timeout after fill)
        - trade_id: trade id if one was opened, else None
        """
        deadline = time.time() + timeout
        since = start_time - timedelta(seconds=5)
        filled = False
        trade_id = None

        # 1) wait for ORDER_FILL for this order_id
        while time.time() < deadline:
            try:
                txs = self._fetch_transactions_since(since)
                for tx in txs:
                    if tx.get("type") == "ORDER_FILL" and tx.get("orderID") == order_id:
                        filled = True
                        # try to capture opened trade id if present
                        trade_opened = tx.get("tradeOpened") or tx.get("tradesOpened")
                        if trade_opened:
                            # tradeOpened may be an object or list; handle common shape
                            if isinstance(trade_opened, dict) and "tradeID" in trade_opened:
                                trade_id = trade_opened["tradeID"]
                            elif isinstance(trade_opened, list) and len(trade_opened) > 0:
                                trade_id = trade_opened[0].get("tradeID")
                        # If no tradeOpened present, the fill may have been immediately closed (rare)
                        print(f"Order {order_id} filled. trade_id={trade_id}")
                        break
                if filled:
                    break
            except Exception as e:
                print(f"Error while polling for ORDER_FILL: {e}")
            time.sleep(1)

        if not filled:
            return False, False, None

        # 2) If a trade was opened, wait until it is no longer in open trades
        if trade_id:
            while time.time() < deadline:
                try:
                    r = TradeList(accountID=self.account_id)
                    trades = self.client.request(r).get("trades", [])
                    open_ids = [t.get("id") for t in trades]
                    if trade_id not in open_ids:
                        # trade closed
                        print(f"Trade {trade_id} closed.")
                        return True, True, trade_id
                except Exception as e:
                    print(f"Error while polling open trades: {e}")
                time.sleep(1)
            # timeout while waiting for trade to close
            print(f"Timeout waiting for trade {trade_id} to close.")
            return True, False, trade_id

        # 3) No trade_id : treat as filled-and-closed immediately
        return True, True, None

    # ---------------- Ladder Serial Execution ----------------
    def run_serial_ladder_within_cycle(
        self,
        cycle_start,
        runtime=120,
        num_orders=5,
        step_pips=3,
        tp_mult=1.0,
        sl_mult=1.5,
    ):
        """
        Execute ladder serially (only one active order at a time) for buys first, then sells.
        If runtime expires while waiting for an order, the function will cancel that order and stop.
        """
        # Ensure we have price
        while not self.stream.current_price:
            time.sleep(0.5)
        bid = self.stream.current_price["bid"]
        ask = self.stream.current_price["ask"]
        step = step_pips * self.pip_size

        deadline = time.time() + runtime

        # ---- BUY ladder (closest to farthest) ----
        for i in range(1, num_orders + 1):
            if time.time() >= deadline:
                print("Runtime expired before placing next BUY level.")
                break
            buy_price = ask + i * step
            # TP rule: if buy_price < ask (buy below ask) then TP = ask, else TP = buy_price + ATR*tp_mult
            atr = self.get_atr()
            if atr == 0:
                print("ATR unavailable, skipping remaining orders.")
                break
            if buy_price < ask:
                tp_buy = ask
            else:
                tp_buy = buy_price + atr * tp_mult
            sl_buy = buy_price - atr * sl_mult

            order_id = self.place_order(buy_price, "BUY", tp_buy, sl_buy)
            if not order_id:
                continue

            # Wait for fill and close with remaining time
            remaining = max(0, int(deadline - time.time()))
            if remaining == 0:
                print("No time left to wait for BUY fill; cancelling it.")
                self.cancel_order(order_id)
                break

            filled, closed, trade_id = self.wait_for_fill_and_close(
                order_id, datetime.now(timezone.utc), remaining
            )
            if not filled:
                # didn't fill within remaining time -> cancel and stop serial execution
                print(f"BUY order {order_id} did not fill in time -> cancelling and ending buy ladder.")
                self.cancel_order(order_id)
                break
            # if filled but not closed (hit timeout waiting for close) we won't place next order
            if filled and not closed:
                print(f"BUY order {order_id} filled but not closed before cycle end; stopping buy ladder.")
                # we leave trade open (it will be closed by TP/SL or user later); do not place next
                break
            # else filled and closed -> proceed to next level

        # ---- SELL ladder (closest to farthest) ----
        for i in range(1, num_orders + 1):
            if time.time() >= deadline:
                print("Runtime expired before placing next SELL level.")
                break
            # refresh prices before placing each sell
            if self.stream.current_price:
                bid = self.stream.current_price["bid"]
                ask = self.stream.current_price["ask"]
            sell_price = bid - i * step
            atr = self.get_atr()
            if atr == 0:
                print("ATR unavailable, skipping remaining orders.")
                break
            # TP rule: if sell_price > bid (sell above bid) then TP = bid, else TP = sell_price - ATR*tp_mult
            if sell_price > bid:
                tp_sell = bid
            else:
                tp_sell = sell_price - atr * tp_mult
            sl_sell = sell_price + atr * sl_mult

            order_id = self.place_order(sell_price, "SELL", tp_sell, sl_sell)
            if not order_id:
                continue

            remaining = max(0, int(deadline - time.time()))
            if remaining == 0:
                print("No time left to wait for SELL fill; cancelling it.")
                self.cancel_order(order_id)
                break

            filled, closed, trade_id = self.wait_for_fill_and_close(
                order_id, datetime.now(timezone.utc), remaining
            )
            if not filled:
                print(f"SELL order {order_id} did not fill in time -> cancelling and ending sell ladder.")
                self.cancel_order(order_id)
                break
            if filled and not closed:
                print(f"SELL order {order_id} filled but not closed before cycle end; stopping sell ladder.")
                break

    # ---------------- Trade Export ----------------
    def export_cycle_trades(self, since_dt, filename="cycle_trades.csv"):
        """
        Export all ORDER_FILL and trade-close related transactions since 'since_dt' (UTC).
        Appends to CSV with header if file empty.
        """
        try:
            params = {"from": since_dt.strftime("%Y-%m-%dT%H:%M:%SZ")}
            r = TransactionList(accountID=self.account_id, params=params)
            resp = self.client.request(r)
            transactions = resp.get("transactions", [])
            # filter ORDER_FILL and TRADE_CLOSE transactions (they include price and pl)
            relevant = []
            for tx in transactions:
                if tx.get("type") in ("ORDER_FILL", "ORDER_CANCEL", "TRADE_CLOSE"):
                    relevant.append(tx)

            if not relevant:
                print("No relevant transactions to export for this cycle.")
                return []

            file_exists = False
            try:
                with open(filename, "r", newline="") as _:
                    file_exists = True
            except FileNotFoundError:
                file_exists = False

            with open(filename, "a", newline="") as f:
                writer = csv.writer(f)
                if not file_exists:
                    # header
                    writer.writerow(
                        [
                            "Cycle_Time",
                            "Tx_Time",
                            "Type",
                            "Instrument",
                            "Side",
                            "Units",
                            "Price",
                            "P/L",
                            "Details",
                        ]
                    )
                cycle_time = since_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
                for tx in relevant:
                    tx_type = tx.get("type")
                    tx_time = tx.get("time")
                    instr = tx.get("instrument", self.instrument)
                    # units and side
                    units = tx.get("units", "")
                    side = ""
                    if units:
                        try:
                            side = "BUY" if int(units) > 0 else "SELL"
                        except Exception:
                            side = ""
                    price = tx.get("price", "")
                    pl = tx.get("pl", "")
                    details = tx.get("reason", "") or ""
                    writer.writerow([cycle_time, tx_time, tx_type, instr, side, units, price, pl, details])

            print(f"Exported {len(relevant)} transactions to {filename}")
            return relevant

        except Exception as e:
            print(f"Error exporting trades: {e}")
            return []

    # ---------------- Run Cycle ----------------
    def run_cycle(
        self,
        runtime=120,
        num_orders=5,
        step_pips=3,
        tp_mult=1.0,
        sl_mult=1.5,
        atr_period=14,
        atr_granularity="M5",
    ):
        """
        Run one cycle:
          - recalc ATR
          - serially execute buy ladder then sell ladder (only one active order at a time)
          - stop when runtime expires
          - cancel any remaining pending orders
          - export transactions since cycle start into CSV
        """
        cycle_start = datetime.now(timezone.utc)
        print(f"Cycle start: {cycle_start.isoformat()}")

        # recalc ATR (and check availability)
        atr = self.get_atr(period=atr_period, granularity=atr_granularity)
        if atr == 0:
            print("ATR not available, skipping cycle.")
            return
        print(f"ATR ({atr_period},{atr_granularity}) = {atr:.6f}")

        # run serial ladder using remaining parameters
        self.run_serial_ladder_within_cycle(
            cycle_start,
            runtime=runtime,
            num_orders=num_orders,
            step_pips=step_pips,
            tp_mult=tp_mult,
            sl_mult=sl_mult,
        )

        # cancel pending orders at cycle end
        print("Cycle ended or time expired — cancelling any remaining pending orders...")
        self.cancel_all_orders()

        # small wait to ensure transactions are recorded by OANDA
        time.sleep(1.5)

        # export transactions since cycle_start
        self.export_cycle_trades(since_dt=cycle_start, filename="cycle_trades.csv")


if __name__ == "__main__":
    trader = BreakoutTrader(ACCOUNT_ID, ACCESS_TOKEN, instrument="EUR_USD", units=1000, pip_size=0.0001)
    trader.stream.start()
    try:
        while True:
            trader.run_cycle(
                runtime=120,
                num_orders=5,
                step_pips=3,
                tp_mult=1.0,
                sl_mult=1.5,
                atr_period=14,
                atr_granularity="M5",
            )
            print("Cycle complete. Restarting...\n")
    except KeyboardInterrupt:
        print("Stopped by user.")
        trader.stream.stop()
