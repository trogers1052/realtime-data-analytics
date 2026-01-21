# Analytics Service

**Language:** Python
**Status:** Not Started
**Priority:** Phase 2 (After Market Data Ingestion)

## Purpose

Calculates technical indicators (RSI, MACD, SMA, Bollinger Bands, ATR) from price data and publishes them to Kafka for the alert service.

## Responsibilities

- Consume price events from `stock.quotes.realtime` Kafka topic
- Calculate technical indicators using pandas and ta-lib
- Publish indicator events to `stock.indicators` Kafka topic
- Store calculated indicators in PostgreSQL `technical_indicators` table
- Support multiple timeframes (daily, hourly)

## Kafka Topics

**Consumes:**
- `stock.quotes.realtime` - Real-time price updates

**Produces:**
- `stock.indicators` - Calculated technical indicators

## Technical Indicators

| Indicator | Description | Use Case |
|-----------|-------------|----------|
| RSI_14 | Relative Strength Index (14-period) | Overbought/oversold detection |
| MACD | Moving Average Convergence Divergence | Trend direction |
| MACD_SIGNAL | MACD Signal line | Crossover signals |
| SMA_20 | Simple Moving Average (20-day) | Short-term trend |
| SMA_50 | Simple Moving Average (50-day) | Medium-term trend |
| SMA_200 | Simple Moving Average (200-day) | Long-term trend |
| BB_UPPER | Bollinger Band Upper | Resistance levels |
| BB_LOWER | Bollinger Band Lower | Support levels |
| ATR_14 | Average True Range (14-period) | Volatility measure |

## Configuration

```env
KAFKA_BROKERS=localhost:19092
KAFKA_CONSUMER_GROUP=analytics-service
KAFKA_INPUT_TOPIC=stock.quotes.realtime
KAFKA_OUTPUT_TOPIC=stock.indicators
DB_HOST=localhost
DB_PORT=5432
DB_USER=trader
DB_PASSWORD=trader5
DB_NAME=trading_platform
```

## Data Flow

```
Kafka (stock.quotes.realtime)
         ↓
   Analytics Service
   - Consume price events
   - Load historical data from DB
   - Calculate indicators
         ↓
   ┌─────┴─────┐
   ↓           ↓
Kafka      PostgreSQL
(stock.    (technical_
indicators) indicators)
```

## IndicatorEvent Schema

```json
{
  "symbol": "AAPL",
  "date": "2026-01-17",
  "indicators": {
    "RSI_14": 45.5,
    "MACD": 2.35,
    "MACD_SIGNAL": 1.89,
    "SMA_20": 175.00,
    "SMA_50": 170.00
  },
  "timestamp": "2026-01-17T16:00:00Z"
}
```

## Build & Run

### Prerequisites

1. **Create Kafka Topics:**
   ```bash
   ./scripts/create-kafka-topics.sh
   ```

2. **Set up Environment:**
   ```bash
   cp .env.example .env
   # Edit .env with your configuration
   ```

### Local Development

```bash
# Install dependencies
pip install -r requirements.txt

# Run
python -m analytics.main
```

### Docker

```bash
# Build
docker build -t analytics-service .

# Run
docker run --rm \
  --network trading-network \
  -e KAFKA_BROKERS=trading-redpanda:9092 \
  -e DB_HOST=trading-db \
  analytics-service
```

## Architecture

```
Kafka (stock.quotes.realtime)
    │
    ▼
Analytics Service
    │
    ├─► Maintain price buffer (in-memory)
    │   └─► Calculate indicators when enough data
    │
    ├─► Kafka (stock.indicators)
    │   └─► Publish indicator events
    │
    └─► PostgreSQL (technical_indicators)
        └─► Store calculated indicators
```

## Configuration

See `.env.example` for all configuration options.

Key settings:
- `MIN_BARS_FOR_CALCULATION`: Minimum bars needed before calculating indicators (default: 200)
- `SMA_PERIODS`: Comma-separated list of SMA periods (default: "20,50,200")
- `ENABLE_POSTGRES_STORAGE`: Whether to store indicators in database (default: true)
- `LOAD_HISTORICAL_DATA`: Load historical bars on startup (default: true)
- `HISTORICAL_BARS_LIMIT`: Number of historical bars to load per symbol (default: 500)

### Historical Data Loading

The service can pre-load historical 1-minute bars from the `market_data` database on startup. This allows indicators to be calculated immediately instead of waiting for 200+ real-time events.

**Configuration:**
```bash
# Market Data Database (for loading historical bars)
MARKET_DATA_DB_HOST=localhost
MARKET_DATA_DB_PORT=5432
MARKET_DATA_DB_USER=trader
MARKET_DATA_DB_PASSWORD=trader123
MARKET_DATA_DB_NAME=market_data

# Historical data loading
LOAD_HISTORICAL_DATA=true
HISTORICAL_BARS_LIMIT=500
```

**How it works:**
1. On startup, connects to `market_data` database
2. Fetches list of monitored symbols from `monitored_symbols` table
3. Loads last N bars (default: 500) for each symbol from `ohlcv_1min` table
4. Pre-populates price buffer with historical data
5. Calculates indicators immediately if enough data is available
6. Then continues with real-time processing from Kafka

**Benefits:**
- ✅ Indicators available immediately on startup
- ✅ No waiting for 200+ real-time events
- ✅ Seamless transition from historical to real-time data

## Indicators Calculated

- **RSI_14**: Relative Strength Index (14-period)
- **MACD**: Moving Average Convergence Divergence
- **MACD_SIGNAL**: MACD Signal line
- **MACD_HISTOGRAM**: MACD Histogram
- **SMA_20, SMA_50, SMA_200**: Simple Moving Averages
- **BB_UPPER, BB_MIDDLE, BB_LOWER**: Bollinger Bands
- **ATR_14**: Average True Range (14-period)

## Testing

```bash
# Verify Kafka consumer is working
# (Make sure market-data-ingestion is publishing events)

# Check indicators in database
psql -h localhost -U trader -d trading_platform -c \
  "SELECT * FROM technical_indicators ORDER BY date DESC LIMIT 10;"

# Consume indicator events from Kafka
docker exec trading-redpanda rpk topic consume stock.indicators \
  --brokers localhost:19092 --num 10
```

## Status

✅ **Complete:**
- Kafka consumer for quote events
- Indicator calculations using pandas-ta
- Kafka producer for indicator events
- PostgreSQL storage
- **Historical data loading on startup** ⭐ NEW
- Graceful shutdown
- Configuration management

🔄 **Future Enhancements:**
- Health check endpoint
- Multiple timeframe support (5min, 1hour, daily)
- Performance metrics and monitoring
