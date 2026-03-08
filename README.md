# Demo Trading Bot

A Python-based trading bot for Binance Futures that implements Support & Resistance (S&R) fractal-based signals with orderflow analysis. This is a **demo/testnet version** designed for backtesting and live testing on Binance's testnet environment.

## Features

- **S&R Fractal Detection**: Identifies support and resistance levels using fractal patterns.
- **Orderflow Analysis**: Analyzes aggregated trade data (aggTrades) for delta-based signals.
- **Risk Management**: Position sizing based on risk percentage, with configurable stop-loss and take-profit.
- **Native TP/SL Orders**: Uses Binance's Algo Service for conditional take-profit and stop-loss orders.
- **WebSocket Integration**: Real-time data streaming for candles and trades.
- **Flask Diagnostics**: Built-in HTTP endpoints for monitoring bot status, signals, and diagnostics.
- **In-Memory Logging**: Signals and fills are tracked in memory (no persistent CSV by default).
- **Testnet Safe**: Defaults to Binance demo/testnet for safe experimentation.

## Requirements

- Python 3.8+
- Required packages: `requests`, `python-dotenv`, `pandas`, `numpy`, `websockets`, `flask`
- Binance Futures API credentials (testnet or live)

## Installation

1. Clone or download the repository.
2. Install dependencies:
   ```bash
   pip install requests python-dotenv pandas numpy websockets flask
   ```
3. Set up your `.env` file (see Configuration below).

## Configuration

Create a `.env` file in the same directory as `demo_bot.py` with the following variables:

```env
# Binance API Credentials
BINANCE_API_KEY=your_testnet_api_key
BINANCE_API_SECRET=your_testnet_api_secret

# Testnet Mode (1 for testnet, 0 for live)
BINANCE_TESTNET=1

# Trading Symbol
SYMBOL=BTC/USDT

# Timeframe (e.g., '1h', '15m', '5m')
TIMEFRAME=1h

# Hours per candle (supports '5m' for 5 minutes, '2h' for 2 hours, or numeric like '1.5')
TF_HOURS=1h

# Leverage
LEVERAGE=10

# Risk & Sizing
INITIAL_EQUITY=1000.0
RISK_PER_TRADE=0.02
MIN_QTY=0.0001
MAX_USD_PER_TRADE=5000.0

# Strategy Params
AGGRESSIVE_LIMIT_OFFSET=0.0003
LIMIT_TIMEOUT_S=2
TP_MAX_POINTS=750.0
TP_R=1.5
DELTA_REL_MULTIPLIER=0.35
CONFIRM_MULT=0.65

# Monitoring
MONITOR_POLL_SEC=6
WATCHDOG_POLL_SEC=60
LISTENKEY_KEEPALIVE_SEC=1500

# Logging
VERBOSE_LOGGING=1

# Diagnostics Port
DIAG_PORT=5000
```

## Running the Bot

### Via Python
```bash
python demo_bot.py
```

### Via PowerShell Script
```powershell
.\run_demo_bot.ps1
```

The bot will:
- Connect to Binance WebSockets for real-time data.
- Start background threads for signal detection, order placement, and monitoring.
- Launch a Flask server on `http://127.0.0.1:5000` for diagnostics.

## Flask Endpoints

The bot exposes HTTP endpoints for monitoring:

- **GET /health**: Basic health check.
  ```bash
  curl http://127.0.0.1:5000/health
  ```

- **GET /diag**: Detailed diagnostics (threads, signals, balance, etc.).
  ```bash
  curl http://127.0.0.1:5000/diag
  ```

- **GET /sr**: Current support and resistance zones.
  ```bash
  curl http://127.0.0.1:5000/sr
  ```

Use PowerShell for easy JSON parsing:
```powershell
Invoke-RestMethod http://127.0.0.1:5000/diag | ConvertTo-Json -Depth 6
```

## How It Works

1. **Data Collection**: Fetches historical klines and streams real-time aggTrades.
2. **Signal Detection**: Analyzes candle patterns and delta thresholds to identify long/short signals at S&R levels.
3. **Order Placement**: Places market orders with native TP/SL via Binance Algo Service.
4. **Monitoring**: Tracks fills, cancels counterpart orders, and logs performance.

## Troubleshooting

- **Memory Errors**: If you see `MemoryError` in WebSocket, the bot may be accumulating too much data. Restart the bot or reduce `TARGET_CANDLES`.
- **API Errors**: Check your API keys and testnet mode. Use `VERBOSE_LOGGING=1` for detailed logs.
- **No Signals**: Adjust `DELTA_REL_MULTIPLIER` or check WebSocket connections.
- **Flask Not Starting**: Ensure port 5000 is free; change `DIAG_PORT` if needed.

## Safety Notes

- **Testnet Only**: This is designed for testnet. Do not use live keys without thorough testing.
- **Risk Warning**: Trading involves risk. This bot is for educational/demo purposes.
- **No Guarantees**: Performance is not guaranteed; backtest thoroughly.

## License

This project is for personal use. Modify and distribute at your own risk.
