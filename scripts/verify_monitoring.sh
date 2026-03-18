#!/bin/bash
echo "Verifying Monitoring Stack..."

# Prometheus
echo -n "Prometheus (9090): "
curl -s -o /dev/null -w "%{http_code}" http://localhost:9090/-/healthy
echo ""

# Grafana
echo -n "Grafana (3001): "
curl -s -o /dev/null -w "%{http_code}" http://localhost:3001/api/health
echo ""

# Redis Exporter
echo -n "Redis Exporter (9121): "
curl -s -o /dev/null -w "%{http_code}" http://localhost:9121/metrics | head -c 3
echo ""

echo "Done."
