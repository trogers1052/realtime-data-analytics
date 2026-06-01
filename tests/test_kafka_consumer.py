"""Tests for QuoteConsumer connect/consume/reconnect/close with mocked kafka."""

import json
from unittest.mock import MagicMock, patch

import pytest

from analytics.kafka_consumer import QuoteConsumer
from kafka.errors import KafkaError


def _make_consumer(handler=None):
    return QuoteConsumer(
        brokers=["localhost:9092"],
        topic="stock.quotes",
        consumer_group="analytics",
        message_handler=handler or MagicMock(),
    )


def _msg(payload):
    """Build a fake kafka message with a JSON-encoded value."""
    m = MagicMock()
    m.value = json.dumps(payload).encode("utf-8")
    return m


# ---------------------------------------------------------------------------
# connect
# ---------------------------------------------------------------------------

class TestConnect:
    def test_connect_success(self):
        c = _make_consumer()
        with patch("analytics.kafka_consumer.KafkaConsumer", return_value=MagicMock()):
            assert c.connect() is True
            assert c._consumer is not None

    def test_connect_failure(self):
        c = _make_consumer()
        with patch("analytics.kafka_consumer.KafkaConsumer", side_effect=KafkaError("down")):
            assert c.connect() is False


# ---------------------------------------------------------------------------
# _consume_loop
# ---------------------------------------------------------------------------

class TestConsumeLoop:
    def _poll_once_then_stop(self, c, first_result):
        """Make poll() return first_result once, then stop the loop."""
        state = {"n": 0}

        def _poll(*a, **kw):
            state["n"] += 1
            if state["n"] == 1:
                return first_result
            c._running = False  # exit the while loop
            return {}

        return _poll

    def test_processes_and_commits_good_batch(self):
        handler = MagicMock()
        c = _make_consumer(handler)
        consumer = MagicMock()
        consumer.poll.side_effect = self._poll_once_then_stop(
            c, {"tp0": [_msg({"event_type": "QUOTE_UPDATE"})]}
        )
        c._consumer = consumer
        c._running = True
        c._consume_loop()
        handler.assert_called_once()
        consumer.commit.assert_called_once()

    def test_empty_poll_continues(self):
        c = _make_consumer()
        consumer = MagicMock()
        consumer.poll.side_effect = self._poll_once_then_stop(c, {})
        c._consumer = consumer
        c._running = True
        c._consume_loop()
        consumer.commit.assert_not_called()

    def test_decode_error_skips_commit(self):
        c = _make_consumer()
        consumer = MagicMock()
        bad = MagicMock()
        bad.value = b"\xff\xfe not json"
        consumer.poll.side_effect = self._poll_once_then_stop(c, {"tp0": [bad]})
        c._consumer = consumer
        c._running = True
        c._consume_loop()
        consumer.commit.assert_not_called()

    def test_handler_exception_skips_commit(self):
        handler = MagicMock(side_effect=Exception("handler boom"))
        c = _make_consumer(handler)
        consumer = MagicMock()
        consumer.poll.side_effect = self._poll_once_then_stop(
            c, {"tp0": [_msg({"event_type": "QUOTE_UPDATE"})]}
        )
        c._consumer = consumer
        c._running = True
        c._consume_loop()
        consumer.commit.assert_not_called()

    def test_commit_error_is_swallowed(self):
        handler = MagicMock()
        c = _make_consumer(handler)
        consumer = MagicMock()
        consumer.commit.side_effect = KafkaError("commit failed")
        consumer.poll.side_effect = self._poll_once_then_stop(
            c, {"tp0": [_msg({"event_type": "QUOTE_UPDATE"})]}
        )
        c._consumer = consumer
        c._running = True
        # Should not raise despite commit error
        c._consume_loop()


# ---------------------------------------------------------------------------
# _reconnect
# ---------------------------------------------------------------------------

class TestReconnect:
    def test_reconnect_success_first_attempt(self):
        c = _make_consumer()
        c._running = True
        c._stop_event.clear()
        with patch.object(c, "connect", return_value=True), \
             patch.object(c._stop_event, "wait", return_value=False):
            assert c._reconnect() is True

    def test_reconnect_aborts_when_not_running(self):
        c = _make_consumer()
        c._running = False
        assert c._reconnect() is False

    def test_reconnect_aborts_on_shutdown_during_wait(self):
        c = _make_consumer()
        c._running = True
        with patch.object(c._stop_event, "wait", return_value=True):
            assert c._reconnect() is False

    def test_reconnect_exhausts_retries(self):
        c = _make_consumer()
        c._running = True
        c._consumer = MagicMock()
        with patch.object(c._stop_event, "wait", return_value=False), \
             patch.object(c, "connect", return_value=False):
            assert c._reconnect() is False


# ---------------------------------------------------------------------------
# start
# ---------------------------------------------------------------------------

class TestStart:
    def test_start_raises_when_not_connected(self):
        c = _make_consumer()
        with pytest.raises(RuntimeError, match="not connected"):
            c.start()

    def test_start_runs_consume_loop_then_stops(self):
        c = _make_consumer()
        c._consumer = MagicMock()

        def _loop():
            c._running = False  # stop after one iteration

        with patch.object(c, "_consume_loop", side_effect=_loop) as loop:
            c.start()
            loop.assert_called_once()

    def test_start_reconnects_on_error(self):
        c = _make_consumer()
        c._consumer = MagicMock()
        calls = {"n": 0}

        def _loop():
            calls["n"] += 1
            if calls["n"] == 1:
                raise RuntimeError("boom")
            c._running = False

        with patch.object(c, "_consume_loop", side_effect=_loop), \
             patch.object(c, "_reconnect", return_value=True) as reconnect:
            c.start()
            reconnect.assert_called_once()

    def test_start_gives_up_when_reconnect_fails(self):
        c = _make_consumer()
        c._consumer = MagicMock()

        with patch.object(c, "_consume_loop", side_effect=RuntimeError("boom")), \
             patch.object(c, "_reconnect", return_value=False):
            with pytest.raises(RuntimeError):
                c.start()


# ---------------------------------------------------------------------------
# close
# ---------------------------------------------------------------------------

class TestClose:
    def test_close_sets_flags_and_closes(self):
        c = _make_consumer()
        c._consumer = MagicMock()
        c._running = True
        c.close()
        assert c._running is False
        assert c._stop_event.is_set()
        c._consumer.close.assert_called_once()

    def test_close_swallows_errors(self):
        c = _make_consumer()
        c._consumer = MagicMock()
        c._consumer.close.side_effect = Exception("x")
        c.close()  # no raise

    def test_close_without_consumer(self):
        c = _make_consumer()
        c.close()  # no raise
