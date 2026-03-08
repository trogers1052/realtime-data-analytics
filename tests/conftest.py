"""Shared fixtures for analytics-service tests."""

import os
import sys
from types import ModuleType
from unittest.mock import MagicMock

# Ensure required env vars are set for Settings construction in tests.
os.environ.setdefault("DB_PASSWORD", "test")
os.environ.setdefault("MARKET_DATA_DB_PASSWORD", "test")


def _stub_missing_modules():
    """Provide lightweight stubs for heavy third-party packages that may
    not be installed in a pure-test environment.  If the real package is
    present we never override it.
    """
    # psycopg2 stubs (need specific callables for `from ... import`)
    if "psycopg2" not in sys.modules:
        pg_stub = ModuleType("psycopg2")
        sys.modules["psycopg2"] = pg_stub
    if "psycopg2.extras" not in sys.modules:
        extras_stub = ModuleType("psycopg2.extras")
        extras_stub.execute_values = MagicMock()
        sys.modules["psycopg2.extras"] = extras_stub
    if "psycopg2.pool" not in sys.modules:
        pool_stub = ModuleType("psycopg2.pool")
        pool_stub.SimpleConnectionPool = MagicMock()
        sys.modules["psycopg2.pool"] = pool_stub

    # kafka stubs
    if "kafka" not in sys.modules:
        kafka_stub = ModuleType("kafka")
        kafka_stub.KafkaProducer = type("KafkaProducer", (), {})
        kafka_stub.KafkaConsumer = type("KafkaConsumer", (), {})
        sys.modules["kafka"] = kafka_stub
    if "kafka.errors" not in sys.modules:
        ke_stub = ModuleType("kafka.errors")
        ke_stub.KafkaError = type("KafkaError", (Exception,), {})
        sys.modules["kafka.errors"] = ke_stub

    # redis stubs
    if "redis" not in sys.modules:
        redis_stub = ModuleType("redis")
        redis_stub.Redis = type("Redis", (), {})
        redis_stub.RedisError = type("RedisError", (Exception,), {})
        sys.modules["redis"] = redis_stub

    # NOTE: pandas and pandas_ta are NOT stubbed here.  Modules that need
    # the real pandas (test_indicators.py) use skipUnless guards.  Modules
    # that don't need real pandas (test_service.py) do their own local
    # stubbing before importing analytics.service.


_stub_missing_modules()
