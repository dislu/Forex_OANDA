from trading_ig import IGService
from trading_ig import IGStreamService
import time
import json

# ------------------------------------
# IG ACCOUNT CONFIGURATION
# ------------------------------------
USERNAME = "arvindt8198"
PASSWORD = "TEVm8s!kYTmt3Qu"
API_KEY = "4522d68e54a7f825a3792f9740e184f7b4e913a5"
ACC_TYPE = "demo"  # or "live"

# ------------------------------------
# INIT IG SERVICE
# ------------------------------------
ig_service = IGService(USERNAME, PASSWORD, API_KEY, ACC_TYPE)
ig_service.create_session(encryption=True)

# ------------------------------------
# FIND SPOT GOLD KO MARKET
# ------------------------------------

def find_ko_market(search_term="Knock-Out"):
    df = ig_service.search_markets(search_term)
    print("🧪 Columns returned:")
    print(df.columns.tolist())
    print("🔍 Full DataFrame:")
    print(df.head(10))  # Show first 10 rows

    # Adjust filtering once we know the correct column names
# ------------------------------------
# PLACE KO ORDER
# ------------------------------------
def place_ko_order(epic, direction="BUY", size=1):
    resp = ig_service.create_open_position(
        currency_code=None,
        direction=direction,
        epic=epic,
        expiry="-",
        force_open=True,
        guaranteed_stop=False,
        level=None,
        limit_distance=None,
        limit_level=None,
        order_type="MARKET",
        quote_id=None,
        size=size,
        stop_distance=None,
        stop_level=None,
        time_in_force="FILL_OR_KILL"
    )
    print(json.dumps(resp, indent=2))
    return resp["dealReference"]

# ------------------------------------
# CLOSE POSITION
# ------------------------------------
def close_position(deal_id, direction, size):
    opposite = "SELL" if direction == "BUY" else "BUY"
    resp = ig_service.close_open_position(
        deal_id=deal_id,
        direction=opposite,
        order_type="MARKET",
        size=size
    )
    print("✅ Position closed successfully")
    return resp

# ------------------------------------
# MONITOR & AUTO-CLOSE
# ------------------------------------
def monitor_auto_close(profit_target=10):
    print("📊 Monitoring positions with auto-close...")
    while True:
        positions = ig_service.fetch_open_positions()["positions"]
        if not positions:
            print("No open KO positions found.")
            break

        for pos in positions:
            market = pos["market"]["instrumentName"]
            pnl = pos["position"]["profitLoss"]
            direction = pos["position"]["direction"]
            size = pos["position"]["size"]
            deal_id = pos["position"]["dealId"]

            print(f"{market}: {size} units | P/L: {pnl}")

            if pnl >= profit_target:
                print(f"🎯 Profit target reached! Closing {market} with +{pnl}")
                close_position(deal_id, direction, size)
                return

        time.sleep(5)

# ------------------------------------
# MAIN EXECUTION
# ------------------------------------
if __name__ == "__main__":
    epic = find_ko_market()
    deal_ref = place_ko_order(epic, direction="BUY", size=1)
    monitor_auto_close(profit_target=10)