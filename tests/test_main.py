"""Tests for analytics.main entry point: signal handling, health server, main()."""

import sys
from unittest.mock import MagicMock, patch

import pytest

import analytics.main as main_mod


class TestSignalHandler:
    def test_closes_consumer_when_service_running(self):
        svc = MagicMock()
        with patch.object(main_mod, "_service", svc):
            main_mod.signal_handler(15, None)
        svc.consumer.close.assert_called_once()

    def test_exits_when_no_service(self):
        with patch.object(main_mod, "_service", None):
            with pytest.raises(SystemExit):
                main_mod.signal_handler(15, None)


class TestHealthServer:
    def test_starts_daemon_thread(self):
        with patch.object(main_mod, "HTTPServer") as Server, \
             patch.object(main_mod, "threading") as threading_mod:
            main_mod._start_health_server()
            Server.assert_called_once()
            threading_mod.Thread.assert_called_once()
            threading_mod.Thread.return_value.start.assert_called_once()


class TestMain:
    def test_main_initializes_and_starts(self):
        svc = MagicMock()
        svc.initialize.return_value = True
        with patch.object(main_mod, "load_dotenv"), \
             patch.object(main_mod, "_start_health_server"), \
             patch.object(main_mod, "start_metrics_server"), \
             patch.object(main_mod, "Settings"), \
             patch.object(main_mod, "AnalyticsService", return_value=svc), \
             patch.object(main_mod, "signal") as signal_mod:
            main_mod.main()
            svc.start.assert_called_once()
            svc.shutdown.assert_called_once()
            assert signal_mod.signal.call_count == 2

    def test_main_exits_when_init_fails(self):
        svc = MagicMock()
        svc.initialize.return_value = False
        with patch.object(main_mod, "load_dotenv"), \
             patch.object(main_mod, "_start_health_server"), \
             patch.object(main_mod, "start_metrics_server"), \
             patch.object(main_mod, "Settings"), \
             patch.object(main_mod, "AnalyticsService", return_value=svc):
            with pytest.raises(SystemExit):
                main_mod.main()
            svc.shutdown.assert_called_once()

    def test_main_fatal_error_exits(self):
        with patch.object(main_mod, "load_dotenv"), \
             patch.object(main_mod, "_start_health_server"), \
             patch.object(main_mod, "start_metrics_server"), \
             patch.object(main_mod, "Settings", side_effect=Exception("boom")):
            with pytest.raises(SystemExit):
                main_mod.main()

    def test_main_keyboard_interrupt_clean(self):
        svc = MagicMock()
        svc.initialize.return_value = True
        svc.start.side_effect = KeyboardInterrupt()
        with patch.object(main_mod, "load_dotenv"), \
             patch.object(main_mod, "_start_health_server"), \
             patch.object(main_mod, "start_metrics_server"), \
             patch.object(main_mod, "Settings"), \
             patch.object(main_mod, "AnalyticsService", return_value=svc), \
             patch.object(main_mod, "signal"):
            main_mod.main()  # no raise
            svc.shutdown.assert_called_once()
