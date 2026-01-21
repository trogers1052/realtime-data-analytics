# Historical Data Loading Feature

## Overview

The Analytics Service now supports loading historical 1-minute bars on startup from the `market_data` database. This allows indicators to be calculated immediately instead of waiting for 200+ real-time quote events to accumulate.

## How It Works

### Startup Sequence

1. **Connect to Market Data Database**
   - Connects to `market_data` database (TimescaleDB)
   - Uses separate connection pool from indicator storage database

2. **Fetch Monitored Symbols**
   - Queries `monitored_symbols` table for enabled symbols
   - Gets list of symbols to load historical data for

3. **Load Historical Bars**
   - For each symbol, fetches last N bars (default: 500) from `ohlcv_1min` table
   - Bars are loaded in chronological order
   - Pre-populates the in-memory price buffer

4. **Calculate Indicators**
   - If enough historical data is available (≥200 bars), calculates indicators immediately
   - Publishes indicator events to Kafka
   - Stores indicators in PostgreSQL

5. **Continue with Real-Time Processing**
   - Starts consuming quote events from Kafka
   - New quotes are appended to the historical buffer
   - Indicators recalculated as new data arrives

### Data Flow

```
Startup:
┌─────────────────────┐
│ market_data DB      │
│ (TimescaleDB)       │
│                     │
│ ohlcv_1min table    │
└──────────┬──────────┘
           │ Load last 500 bars
           ▼
┌─────────────────────┐
│ Analytics Service   │
│                     │
│ Price Buffer        │
│ [historical bars]   │
└──────────┬──────────┘
           │ Calculate indicators
           ▼
┌─────────────────────┐
│ Kafka + PostgreSQL  │
│ (indicator events)  │
└─────────────────────┘

Runtime:
┌─────────────────────┐
│ Kafka               │
│ stock.quotes.       │
│ realtime            │
└──────────┬──────────┘
           │ Real-time quotes
           ▼
┌─────────────────────┐
│ Analytics Service   │
│                     │
│ Price Buffer        │
│ [historical + new]  │
└──────────┬──────────┘
           │ Recalculate indicators
           ▼
┌─────────────────────┐
│ Kafka + PostgreSQL  │
└─────────────────────┘
```

## Configuration

### Environment Variables

```bash
# Market Data Database Connection
MARKET_DATA_DB_HOST=localhost
MARKET_DATA_DB_PORT=5432
MARKET_DATA_DB_USER=trader
MARKET_DATA_DB_PASSWORD=trader123
MARKET_DATA_DB_NAME=market_data

# Historical Data Loading
LOAD_HISTORICAL_DATA=true          # Enable/disable feature
HISTORICAL_BARS_LIMIT=500          # Number of bars to load per symbol
```

### Defaults

- `LOAD_HISTORICAL_DATA`: `true` (enabled by default)
- `HISTORICAL_BARS_LIMIT`: `500` bars per symbol
- Market data DB credentials match market-data-ingestion defaults

## Benefits

### ✅ Immediate Indicator Availability
- Indicators calculated on startup if enough historical data exists
- No waiting period for real-time data to accumulate
- Service is "ready" immediately

### ✅ Seamless Transition
- Historical data seamlessly merges with real-time data
- No gaps or duplicate calculations
- Timestamp-based deduplication prevents duplicates

### ✅ Better User Experience
- Alert service can start working immediately
- Dashboard shows indicators right away
- No cold start delay

## Implementation Details

### Database Connections

The service maintains **two separate database connections**:

1. **Trading Platform DB** (`trading_platform`)
   - Used for storing calculated indicators
   - Table: `technical_indicators`

2. **Market Data DB** (`market_data`)
   - Used for loading historical bars
   - Table: `ohlcv_1min` (TimescaleDB hypertable)
   - Read-only access

### Deduplication

The service prevents duplicate bars by:
- Checking timestamp of incoming quote events
- Skipping quotes that are older than the latest bar in buffer
- Maintaining chronological order

### Buffer Management

- Maximum buffer size: `2 × MIN_BARS_FOR_CALCULATION` (default: 400 bars)
- Oldest bars are trimmed when buffer exceeds limit
- Keeps most recent data for indicator calculations

## Example Log Output

```
INFO - Connecting to market data database
INFO - Successfully connected to market data database
INFO - Loading historical data for monitored symbols...
INFO - Found 3 monitored symbols: ['AAPL', 'MSFT', 'GOOGL']
INFO - Loaded 500 historical bars for AAPL
INFO - Calculating indicators for AAPL from historical data...
INFO - Calculated and published indicators for AAPL: 11 indicators
INFO - Loaded 500 historical bars for MSFT
INFO - Calculating indicators for MSFT from historical data...
INFO - Calculated and published indicators for MSFT: 11 indicators
INFO - Loaded 500 historical bars for GOOGL
INFO - Calculating indicators for GOOGL from historical data...
INFO - Calculated and published indicators for GOOGL: 11 indicators
INFO - Historical data loading complete: 1500 total bars loaded
INFO - Starting analytics service...
```

## Troubleshooting

### No Historical Data Loaded

**Symptoms:**
- Service starts but no "Loaded X historical bars" messages
- Indicators not calculated on startup

**Possible Causes:**
1. `LOAD_HISTORICAL_DATA=false` in environment
2. Cannot connect to market_data database
3. No monitored symbols in `monitored_symbols` table
4. No data in `ohlcv_1min` table for monitored symbols

**Solutions:**
- Check environment variables
- Verify database connection: `psql -h localhost -U trader -d market_data`
- Check monitored symbols: `SELECT * FROM monitored_symbols WHERE enabled = true;`
- Verify data exists: `SELECT COUNT(*) FROM ohlcv_1min WHERE symbol = 'AAPL';`

### Duplicate Indicators

**Symptoms:**
- Same indicators calculated multiple times
- Duplicate entries in `technical_indicators` table

**Solution:**
- Database uses `ON CONFLICT` to handle duplicates
- Service checks timestamps to prevent duplicate quotes
- Should not occur, but database handles it gracefully

### Performance Considerations

**Memory Usage:**
- Each bar: ~100 bytes
- 500 bars × 10 symbols = ~500KB
- Negligible memory impact

**Startup Time:**
- Database query: ~100-200ms per symbol
- Indicator calculation: ~50-100ms per symbol
- Total startup time: ~1-2 seconds for 10 symbols

**Database Load:**
- Read-only queries
- Uses connection pooling
- Minimal impact on market-data-ingestion service

## Future Enhancements

- [ ] Configurable time range (e.g., "last 24 hours" instead of "last 500 bars")
- [ ] Parallel loading for multiple symbols
- [ ] Cache historical data in Redis for faster restarts
- [ ] Support for multiple timeframes (5min, 1hour, daily)

---

**Status: ✅ Implemented and Ready**
