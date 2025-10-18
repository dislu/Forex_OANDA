import pandas as pd
import matplotlib.pyplot as plt

# Load the CSV file
df = pd.read_csv('transactions.csv')

# Adjust these column names to match your CSV
time_col = 'TRANSACTION DATE'  # e.g., 'timestamp' or 'date'
price_col ='PRICE'  # e.g., 'price'
pnl_col = 'PL'  # e.g., 'profit_loss' or 'PnL'

# Convert time column to datetime if needed
df[time_col] = pd.to_datetime(df[time_col])

fig, ax1 = plt.subplots(figsize=(12, 6))

# Plot price
ax1.set_xlabel('Time')
ax1.set_ylabel('Price', color='tab:blue')
ax1.plot(df[time_col], df[price_col], color='tab:blue', label='Price')
ax1.tick_params(axis='y', labelcolor='tab:blue')

# Plot PnL on a secondary y-axis
ax2 = ax1.twinx()
ax2.set_ylabel('Profit/Loss', color='tab:red')
ax2.plot(df[time_col], df[pnl_col], color='tab:red', label='PnL')
ax2.tick_params(axis='y', labelcolor='tab:red')

fig.tight_layout()
plt.title('Price and Profit/Loss Over Time')
plt.show()