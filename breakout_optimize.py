# breakout.py
import oandapyV20
import oandapyV20.endpoints.pricing as pricing
from oandapyV20.endpoints.orders import OrderCreate, OrderCancel
from oandapyV20.endpoints.transactions import TransactionList
from datetime import datetime, timedelta, timezone
from dateutil import parser
import pytz
import time
import pandas as pd
from config import ACCOUNT_ID, ACCESS_TOKEN

UNITS = 1000
INSTRUMENT = "EUR_USD"
NUM_ORDERS = 5
k_SL = 1.5  # SL multiplier
m_SPACING = 0.5  # Ladder spacing multiplier for volatility
client = oandapyV20.API(access_token=ACCESS_TOKEN)

# Order books
pending_trades = {}
buy_orders = []
sell_orders = []
filled_orders = []

def get_latest_price():
    """Fetch latest bid/ask price"""
    params = {"instruments": INSTRUMENT}
    r = pricing.PricingInfo(accountID=ACCOUNT_ID, params=params)
    resp = client.request(r)
    bids = float(resp['prices'][0]['bids'][0]['price'])
    asks = float(resp['prices'][0]['asks'][0]['price'])
    utc_time = parser.parse(resp['prices'][0]['time'])
    sgt_time = utc_time.astimezone(pytz.timezone("Asia/Singapore"))
    formatted_time = sgt_time.strftime("%Y-%m-%d %H:%M:%S")
    return bids, asks, formatted_time

def fetch_recent_candles(n=15):
    """Fetch last n 1-min candles to calculate ATR"""
    import requests
    url = f"https://api-fxpractice.oanda.com/v3/instruments/{INSTRUMENT}/candles"
    headers = {"Authorization": f"Bearer {ACCESS_TOKEN}"}
    params = {"count": n, "granularity": "M1", "price": "M"}
    r = requests.get(url, headers=headers, params=params).json()
    data = []
    for c in r['candles']:
        if c['complete']:
            data.append([float(c['mid']['h']), float(c['mid']['l']), float(c['mid']['c'])])
    df = pd.DataFrame(data, columns=['high','low','close'])
    return df

def calculate_atr(df, period=14):
    high_low = df['high'] - df['low']
    high_close = abs(df['high'] - df['close'].shift())
    low_close = abs(df['low'] - df['close'].shift())
    tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
    atr = tr.rolling(period).mean().iloc[-1]
    return atr

def place_order(price, direction, sl, tp):
    units = UNITS if direction == "BUY" else -UNITS
    trigger = "BID" if direction=="BUY" else "ASK"
    data = {
        "order": {
            "instrument": INSTRUMENT,
            "units": str(units),
            "type": "MARKET_IF_TOUCHED",
            "price": str(round(price,5)),
            "timeInForce":"GFD",
            "triggerCondition": trigger,
            "stopLossOnFill":{"price":str(round(sl,5))},
            "takeProfitOnFill":{"price":str(round(tp,5))}
        }
    }
    try:
        req = OrderCreate(accountID=ACCOUNT_ID, data=data)
        resp = client.request(req)
        order_id = resp['orderCreateTransaction']['id']
        pending_trades[round(price,5)] = order_id
        if direction=="BUY":
            buy_orders.append(round(price,5))
        else:
            sell_orders.append(round(price,5))
        return order_id
    except Exception as e:
        print(f"Error placing {direction} order at {price}: {e}")
        return None

def place_grid_orders(current_bid, current_ask):
    df_candles = fetch_recent_candles()
    atr = calculate_atr(df_candles)
    spacing = m_SPACING * atr

    # Place Buy MIT above current price
    for i in range(1, NUM_ORDERS+1):
        price = current_ask + i * spacing
        sl = price - k_SL*atr
        tp = price + spacing  # TP = 1 spacing above
        place_order(price, "BUY", sl, tp)

    # Place Sell MIT below current price
    for i in range(1, NUM_ORDERS+1):
        price = current_bid - i * spacing
        sl = price + k_SL*atr
        tp = price - spacing
        place_order(price, "SELL", sl, tp)

def monitor_orders():
    while True:
        bid, ask, ts = get_latest_price()
        print(f"{ts} | Bid: {bid} | Ask: {ask}")
        # Check filled Buy orders
        for price in buy_orders[:]:
            if bid >= price:
                print(f"Buy MIT at {price} filled")
                buy_orders.remove(price)
                filled_orders.append(price)
        # Check filled Sell orders
        for price in sell_orders[:]:
            if ask <= price:
                print(f"Sell MIT at {price} filled")
                sell_orders.remove(price)
                filled_orders.append(price)
        time.sleep(1)

def main():
    bid, ask, ts = get_latest_price()
    print(f"Initial price: Bid={bid}, Ask={ask} | {ts}")
    place_grid_orders(bid, ask)
    monitor_orders()

if __name__=="__main__":
    main()
