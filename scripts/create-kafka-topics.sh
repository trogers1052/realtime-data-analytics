#!/bin/bash
# Create Kafka topics for analytics service

set -e

KAFKA_BROKER="${KAFKA_BROKERS:-localhost:19092}"

echo "Creating Kafka topics for analytics service..."
echo "Broker: $KAFKA_BROKER"

# Create stock.indicators topic
echo ""
echo "Creating topic: stock.indicators"
docker exec trading-redpanda rpk topic create stock.indicators \
  --brokers "$KAFKA_BROKER" \
  --partitions 3 \
  --replication-factor 1 \
  --topic-config retention.ms=3600000 || echo "Topic may already exist"

echo ""
echo "Topics created successfully!"
echo ""
echo "To verify, run:"
echo "  docker exec trading-redpanda rpk topic list --brokers $KAFKA_BROKER"
echo ""
echo "To view topic details:"
echo "  docker exec trading-redpanda rpk topic describe stock.indicators --brokers $KAFKA_BROKER"
