import asyncio
from alpaca.trading.client import TradingClient
from alpaca.trading.requests import MarketOrderRequest
from alpaca.trading.enums import OrderSide, TimeInForce
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest, StockLatestQuoteRequest
from alpaca.data.timeframe import TimeFrame
from datetime import datetime, timedelta
import pandas as pd
import alpaca_trade_api as tradeapi

# Initialize Alpaca clients with your provided API keys
api_key = 'PKPICLW1ZL125SU9DXPX'
api_secret = 'XQXgbYtG91FztvxMgbMnW1rHfiZPpAwxVEUyhI44'
trading_client = TradingClient(api_key, api_secret, paper=True)
dataclient = StockHistoricalDataClient(api_key, api_secret)
wss_client = tradeapi.Stream(api_key, api_secret)

# Trade limit configuration
MAX_TRADES_PER_MINUTE = 10000
trade_times = []

# Function to fetch historical data
def fetch_historical_data():
    try:
        request_params = StockBarsRequest(
            symbol_or_symbols=["SPY"],
            timeframe=TimeFrame.Day,
            start=datetime(2023, 3, 1),
            end=datetime.now()
        )
        bars = dataclient.get_stock_bars(request_params)
        df = bars.df
        df['moving_avg'] = df['close'].rolling(window=50).mean()
        df['moving_std'] = df['close'].rolling(window=50).std()
        df['sell_high'] = df['moving_avg'] + (df['moving_std'] * 4) # 8 is very conservative
        df['buy_low'] = df['moving_avg'] - (df['moving_std'] * 4)
        df['signal'] = 0
        df.loc[df['close'] < df['buy_low'], 'signal'] = 1  # Buy
        df.loc[df['close'] > df['sell_high'], 'signal'] = -1  # Sell
        df['position'] = df['signal'].shift()
        return df
    except Exception as err:
        print(f"Error occurred: {err}")
        return pd.DataFrame()  # Return an empty DataFrame on error

# Initialize with historical data
SPY_Historical_Data = fetch_historical_data()

# Function to handle live data updates
async def data_handler(data):
    global SPY_Historical_Data
    
    # Fetch latest quote data
    try:
        request_params = StockLatestQuoteRequest(symbol_or_symbols=["SPY"])
        quote = dataclient.get_stock_latest_quote(request_params)
        new_data = pd.DataFrame([{
            'timestamp': datetime.now(),
            'close': quote['SPY'].ask_price,  # Use ask_price as proxy for close price
            'volume': quote['SPY'].ask_size  # Use ask_size as proxy for volume
        }])
    except Exception as e:
        print(f"Failed to get latest quote: {e}")
        return

    # Append new data to the existing DataFrame
    SPY_Historical_Data = pd.concat([SPY_Historical_Data, new_data]).reset_index(drop=True)

    # Calculate rolling mean and standard deviation with new data
    SPY_Historical_Data['moving_avg'] = SPY_Historical_Data['close'].rolling(window=50).mean()
    SPY_Historical_Data['moving_std'] = SPY_Historical_Data['close'].rolling(window=50).std()
    SPY_Historical_Data['sell_high'] = SPY_Historical_Data['moving_avg'] + (SPY_Historical_Data['moving_std'] * 4) #smaller the number the smaller the diff between buy signal and call signal
    SPY_Historical_Data['buy_low'] = SPY_Historical_Data['moving_avg'] - (SPY_Historical_Data['moving_std'] * 4)
    SPY_Historical_Data['sell'] = SPY_Historical_Data['moving_avg'] + (SPY_Historical_Data['moving_std'] * 8) #smaller the number the smaller the diff between buy signal and call signal
    SPY_Historical_Data['buy'] = SPY_Historical_Data['moving_avg'] - (SPY_Historical_Data['moving_std'] * 8)

    # Update signals
    SPY_Historical_Data['signal'] = 0
    SPY_Historical_Data.loc[SPY_Historical_Data['close'] < SPY_Historical_Data['buy_low'], 'signal'] = 1  # Buy
    SPY_Historical_Data.loc[SPY_Historical_Data['close'] > SPY_Historical_Data['sell_high'], 'signal'] = -1  # Sell
    SPY_Historical_Data.loc[SPY_Historical_Data['close'] < SPY_Historical_Data['buy_low'], 'signal'] = 2  # Sell Call price high
    SPY_Historical_Data.loc[SPY_Historical_Data['close'] > SPY_Historical_Data['sell_high'], 'signal'] = -2  # Sell Put price low
    SPY_Historical_Data['position'] = SPY_Historical_Data['signal'].shift()

    # Print the updated DataFrame
    print(SPY_Historical_Data.tail())

    # Determine the latest position
    latest_position = SPY_Historical_Data['position'].iloc[-1]

    # Execute trades based on the latest position
    if latest_position == 1:
        execute_trade("SPY240814C00542000", OrderSide.BUY, 1)  # Buy one call option - adjust number for price of contract
    elif latest_position == -1:
        execute_trade("SPY240814P00544000", OrderSide.BUY, 1)  # Buy one put option
    if latest_position == 2:
        execute_trade("SPY240814C00542000", OrderSide.SELL, 1)  # Buy one call option - adjust number for price of contract
    elif latest_position == -2:
        execute_trade("SPY240814P00544000", OrderSide.SELL, 1)  # Buy one put option
   

# Function to execute trades
def execute_trade(option_symbol, side, qty):
    global trade_times

    current_time = datetime.now()

    # Remove trades that are older than one minute
    trade_times = [t for t in trade_times if t > current_time - timedelta(minutes=1)]

    if len(trade_times) >= MAX_TRADES_PER_MINUTE:
        print("Trade limit reached. Try again later.")
        return

    try:
        order = MarketOrderRequest(
            symbol=option_symbol,
            qty=qty,
            side=side,
            time_in_force=TimeInForce.DAY  # Change from GTC to DAY
        )
        trading_client.submit_order(order)
        trade_times.append(current_time)  # Record the trade time
        print(f"Submitted {side.name} order for {qty} contracts of {option_symbol}.")
    except Exception as e:
        print(f"Failed to execute trade: {e}")

# Main function to run the data handler
async def start_websocket_client():
    try:
        # Subscribe to live price updates for SPY
        wss_client.subscribe_quotes(data_handler, "SPY")
        
        # Run the WebSocket client
        while True:
            await wss_client._run_forever()
    except Exception as e:
        print(f"An error occurred: {e}")

# Main function to run the WebSocket client
async def main():
    await start_websocket_client()

# Run the main function
if __name__ == "__main__":
    asyncio.run(main())
