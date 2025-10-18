🔹 Key Notes

Initial Breakout Ladder:

place_initial_orders_2_breakout() places Buy above, Sell below the current price.

Continuous Monitoring:

monitor_and_update_orders_2_breakout() handles filled orders and replenishes ladder dynamically.

Logging:

Filled orders logged to CSV (filled_orders_2_log.csv).

Periodic Transaction Fetch:

fetch_recent_closed_transactions(5) retrieves recent closed trades every 5 minutes for P/L tracking.

Debugging:

Every 60 iterations prints pending and filled orders.# Forex_OANDA
# Forex_OANDA
