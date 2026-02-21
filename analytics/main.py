"""
Analytics Service - Main Entry Point

Consumes price events from Kafka, calculates technical indicators,
and publishes indicator events back to Kafka.
"""

import logging
import os
import signal
import sys
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

from dotenv import load_dotenv

from .config import Settings
from .service import AnalyticsService

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)

# Global service instance for signal handler
_service: AnalyticsService = None


def signal_handler(signum, frame):
    """Handle shutdown signals."""
    logger.info(f"Received signal {signum}, shutting down...")
    if _service:
        _service.shutdown()
    sys.exit(0)


def _start_health_server() -> None:
    """Start a minimal HTTP health server on a daemon thread."""
    port = int(os.environ.get("HEALTH_PORT", "8080"))

    class _Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path == "/health":
                self.send_response(200)
                self.end_headers()
                self.wfile.write(b"ok")
            else:
                self.send_response(404)
                self.end_headers()

        def log_message(self, *args):
            pass  # suppress HTTP access logs

    server = HTTPServer(("", port), _Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    logger.info(f"Health server listening on :{port}/health")


def main():
    """Main entry point for the analytics service."""
    global _service

    # Load environment variables
    load_dotenv()

    _start_health_server()

    logger.info("=" * 60)
    logger.info("Analytics Service")
    logger.info("=" * 60)

    try:
        # Load settings
        settings = Settings()
        logger.info(f"Kafka brokers: {settings.kafka_brokers}")
        logger.info(f"Input topic: {settings.kafka_input_topic}")
        logger.info(f"Output topic: {settings.kafka_output_topic}")
        logger.info(f"Consumer group: {settings.kafka_consumer_group}")
        logger.info(f"Database: {settings.db_host}:{settings.db_port}/{settings.db_name}")
        logger.info("=" * 60)

        # Create service
        _service = AnalyticsService(settings)

        # Initialize connections
        if not _service.initialize():
            logger.error("Failed to initialize service")
            sys.exit(1)

        # Set up signal handlers
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

        # Start service
        _service.start()

    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)
    finally:
        if _service:
            _service.shutdown()


if __name__ == "__main__":
    main()
