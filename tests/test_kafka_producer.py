"""Tests for kafka_producer event construction format."""

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest
from analytics.kafka_producer import IndicatorProducer


class TestIndicatorProducerEventFormat:
    """Verify the event dict structure built by publish_indicator."""

    def _make_connected_producer(self):
        """Create a producer with a mocked KafkaProducer."""
        p = IndicatorProducer(brokers=["localhost:9092"], topic="stock.indicators")
        mock_kafka = MagicMock()
        future = MagicMock()
        future.get.return_value = MagicMock(topic="stock.indicators", partition=0, offset=42)
        mock_kafka.send.return_value = future
        p._producer = mock_kafka
        return p, mock_kafka

    def test_event_structure_no_data_quality(self):
        p, mock = self._make_connected_producer()
        ts = datetime(2026, 2, 20, 14, 30, 0, tzinfo=timezone.utc)
        indicators = {"RSI_14": 45.0, "close": 150.0}

        result = p.publish_indicator("AAPL", ts, indicators)
        assert result is True

        # Extract the event passed to send
        call_kwargs = mock.send.call_args
        event = call_kwargs.kwargs.get("value") or call_kwargs[1].get("value")

        assert event["event_type"] == "INDICATOR_UPDATE"
        assert event["source"] == "analytics-service"
        assert event["schema_version"] == "1.1"
        assert "Z" in event["timestamp"]
        assert event["data"]["symbol"] == "AAPL"
        assert event["data"]["indicators"]["RSI_14"] == 45.0
        assert "data_quality" not in event["data"]

    def test_event_includes_data_quality_when_provided(self):
        p, mock = self._make_connected_producer()
        ts = datetime(2026, 2, 20, 14, 30, 0)
        indicators = {"close": 150.0}
        quality = {"status": "current", "is_ready": True, "bar_count": 500}

        p.publish_indicator("GOOG", ts, indicators, data_quality=quality)

        event = mock.send.call_args.kwargs.get("value") or mock.send.call_args[1].get("value")
        assert event["data"]["data_quality"] == quality

    def test_symbol_used_as_kafka_key(self):
        p, mock = self._make_connected_producer()
        ts = datetime(2026, 2, 20, 14, 30, 0)
        p.publish_indicator("TSLA", ts, {"close": 200.0})

        key = mock.send.call_args.kwargs.get("key") or mock.send.call_args[1].get("key")
        assert key == "TSLA"

    def test_publish_to_configured_topic(self):
        p, mock = self._make_connected_producer()
        p.topic = "custom.topic"
        ts = datetime(2026, 2, 20, 14, 30, 0)
        p.publish_indicator("X", ts, {"close": 10.0})

        topic = mock.send.call_args.args[0] if mock.send.call_args.args else mock.send.call_args[0][0]
        assert topic == "custom.topic"


class TestIndicatorProducerErrors:
    def test_publish_raises_when_not_connected(self):
        p = IndicatorProducer(brokers=["localhost:9092"], topic="t")
        with pytest.raises(RuntimeError, match="not connected"):
            p.publish_indicator("X", datetime.now(), {})

    def test_close_noop_when_no_producer(self):
        p = IndicatorProducer(brokers=["localhost:9092"], topic="t")
        p.close()  # Should not raise
