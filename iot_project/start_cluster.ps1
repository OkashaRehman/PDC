# Script to start cluster in separate visible terminals
# Usage: .\start_cluster.ps1

Write-Host "Starting Fault-Tolerant IoT Cluster..." -ForegroundColor Cyan
Write-Host "Starting Dashboard..." -ForegroundColor Green

# Start Dashboard in new window
Start-Process powershell -ArgumentList "-NoExit -Command `"cd 'e:\ppddcc\iot_project'; Write-Host 'Starting Dashboard...' -ForegroundColor Cyan; python dashboard.py`""

# Wait for dashboard to initialize
Start-Sleep -Seconds 2

Write-Host "`nTo add nodes to the cluster:" -ForegroundColor Yellow
Write-Host "1. Click '+ Add Node' in the dashboard (http://127.0.0.1:8000)" -ForegroundColor White
Write-Host "2. Each server will start in a NEW TERMINAL" -ForegroundColor White
Write-Host "3. Watch the live logs showing data chunks being processed in parallel" -ForegroundColor White
Write-Host "`nServers will show:" -ForegroundColor Yellow
Write-Host "  - [LEADER] Splitting data and sending chunks" -ForegroundColor Cyan
Write-Host "  - [WORKER] Receiving chunk, local calculation results" -ForegroundColor Green
Write-Host "  - [LEADER] Aggregating worker results" -ForegroundColor Cyan
Write-Host "`nThen click '▶ Start Sensors' to begin data flow" -ForegroundColor Yellow
