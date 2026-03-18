Write-Host "Verifying Monitoring Stack..."

# Prometheus
Write-Host -NoNewline "Prometheus (9090): "
try {
    $response = Invoke-WebRequest -Uri "http://localhost:9090/-/healthy" -UseBasicParsing
    Write-Host $response.StatusCode
}
catch {
    Write-Host "Failed: $_"
}

# Grafana
Write-Host -NoNewline "Grafana (3001): "
try {
    $response = Invoke-WebRequest -Uri "http://localhost:3001/api/health" -UseBasicParsing
    Write-Host $response.StatusCode
}
catch {
    Write-Host "Failed: $_"
}

# Redis Exporter
Write-Host -NoNewline "Redis Exporter (9121): "
try {
    $response = Invoke-WebRequest -Uri "http://localhost:9121/metrics" -UseBasicParsing
    Write-Host $response.StatusCode
}
catch {
    Write-Host "Failed: $_"
}

Write-Host "Done."
