-- Migration: Create technical_indicators table
-- This table stores calculated technical indicators from the analytics service
-- Database: trading_platform

-- Create the technical_indicators table
CREATE TABLE IF NOT EXISTS technical_indicators (
    id SERIAL,
    symbol VARCHAR(20) NOT NULL,
    date TIMESTAMPTZ NOT NULL,
    indicator_type VARCHAR(50) NOT NULL,
    value NUMERIC(20, 8) NOT NULL,
    timeframe VARCHAR(10) NOT NULL DEFAULT '1min',
    created_at TIMESTAMPTZ DEFAULT NOW(),

    -- Composite primary key for upsert support
    PRIMARY KEY (symbol, date, indicator_type, timeframe)
);

-- Create indexes for common query patterns
-- Index for querying by symbol and date range
CREATE INDEX IF NOT EXISTS idx_technical_indicators_symbol_date
    ON technical_indicators (symbol, date DESC);

-- Index for querying by indicator type
CREATE INDEX IF NOT EXISTS idx_technical_indicators_type
    ON technical_indicators (indicator_type, symbol, date DESC);

-- Index for querying latest indicators per symbol
CREATE INDEX IF NOT EXISTS idx_technical_indicators_latest
    ON technical_indicators (symbol, indicator_type, date DESC);

-- Add comments for documentation
COMMENT ON TABLE technical_indicators IS 'Stores calculated technical indicators (RSI, MACD, SMA, etc.) from the analytics service';
COMMENT ON COLUMN technical_indicators.symbol IS 'Stock ticker symbol (e.g., AAPL, SLV)';
COMMENT ON COLUMN technical_indicators.date IS 'Timestamp of the indicator calculation';
COMMENT ON COLUMN technical_indicators.indicator_type IS 'Type of indicator (RSI_14, MACD, SMA_20, BB_UPPER, ATR_14, etc.)';
COMMENT ON COLUMN technical_indicators.value IS 'Calculated indicator value';
COMMENT ON COLUMN technical_indicators.timeframe IS 'Data timeframe (1min, 5min, 1hour, 1day)';

-- Grant permissions (adjust as needed for your setup)
-- GRANT SELECT, INSERT, UPDATE ON technical_indicators TO trader;
-- GRANT USAGE, SELECT ON SEQUENCE technical_indicators_id_seq TO trader;
