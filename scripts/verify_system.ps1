Write-Host "=== Fraud Detection System Verification ===" -ForegroundColor Cyan

# 1. Check Docker Services
Write-Host "`n[1] Checking Docker services..." -ForegroundColor Yellow
docker-compose ps

# 2. Check Kafka
Write-Host "`n[2] Checking Kafka messages (Wait 10s)..." -ForegroundColor Yellow
# Using a slightly different approach for Windows timeout equivalent
$kafkaJob = Start-Job -ScriptBlock { docker exec kafka kafka-console-consumer --bootstrap-server localhost:29092 --topic transactions --max-messages 3 }
Wait-Job $kafkaJob -Timeout 15
Stop-Job $kafkaJob
Receive-Job $kafkaJob

# 3. Check Spark
Write-Host "`n[3] Verifying Spark processing..." -ForegroundColor Yellow
docker-compose logs spark-inference | Select-String "Processing batch" -Context 0,1 | Select-Object -Last 5

# 4. Check Output File
Write-Host "`n[4] Checking output file..." -ForegroundColor Yellow
docker exec spark-inference ls -lh /app/data/recent.json

# 5. Test APIs
Write-Host "`n[5] Testing API endpoints..." -ForegroundColor Yellow

$endpoints = @(
    @{Url="http://localhost:3000/api/live"; Name="Live Data"},
    @{Url="http://localhost:3000/api/fraud"; Name="Fraud Data"},
    @{Url="http://localhost:3000/api/producer/status"; Name="Producer Status"},
    @{Url="http://localhost:3000/api/alerts/high_priority"; Name="High Priority Alerts"}
)

foreach ($ep in $endpoints) {
    try {
        $response = Invoke-RestMethod -Uri $ep.Url -Method Get
        $count = if ($response -is [array]) { $response.Count } else { 1 }
        Write-Host "$($ep.Name): OK (Count: $count)" -ForegroundColor Green
    } catch {
        Write-Host "$($ep.Name): FAILED ($($_))" -ForegroundColor Red
    }
}

Write-Host "`n=== Verification Complete ===" -ForegroundColor Cyan
