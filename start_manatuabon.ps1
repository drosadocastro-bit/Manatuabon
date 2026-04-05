Write-Host "===================================================" -ForegroundColor Cyan
Write-Host "    MANATUABON HYBRID INTELLIGENCE STARTUP" -ForegroundColor Cyan
Write-Host "===================================================" -ForegroundColor Cyan
Write-Host ""

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location -Path $ScriptDir

$PythonExe = Join-Path $ScriptDir ".venv\Scripts\python.exe"
if (-not (Test-Path $PythonExe)) {
	$PythonExe = "python"
}

Write-Host "[1/8] Migrating database schema..." -ForegroundColor Yellow
& $PythonExe db_init.py

Write-Host "[2/8] Starting UI Server (Port 8765)..." -ForegroundColor Green
Start-Process -FilePath $PythonExe -ArgumentList "-m http.server 8765" -Wait:$false

Write-Host "[3/8] Starting Agent Brain (Port 7777)..." -ForegroundColor Green
$agentArgs = "set MANATUABON_COUNCIL_GRAPH_MODE=primary && `"$PythonExe`" manatuabon_agent.py --watch `"$ScriptDir\renders`" --inbox `"$ScriptDir\inbox`" --port 7777 --consolidate-every 30"
Start-Process -FilePath "cmd.exe" -ArgumentList "/c", $agentArgs -Wait:$false
Write-Host "      Council graph mode: PRIMARY" -ForegroundColor DarkCyan

Write-Host "[4/8] Securing STScI API Worker (MAST JWST Archive)..." -ForegroundColor Green
Start-Process -FilePath $PythonExe -ArgumentList "mast_worker.py" -Wait:$false

Write-Host "[5/8] Securing ALMA/SETI Radio Worker..." -ForegroundColor Green
Start-Process -FilePath $PythonExe -ArgumentList "radio_worker.py" -Wait:$false

Write-Host "[6/8] Launching Observatory Dashboard (Port 8766)..." -ForegroundColor Magenta
Start-Process -FilePath $PythonExe -ArgumentList "-m http.server 8766" -Wait:$false

Write-Host "[7/8] Starting High-Energy Transient Worker (Swift/MAXI)..." -ForegroundColor Green
Start-Process -FilePath $PythonExe -ArgumentList "transient_worker.py" -Wait:$false

Write-Host "[8/8] Starting Artemis II Mission Tracker..." -ForegroundColor Green
Start-Process -FilePath $PythonExe -ArgumentList "mission_worker.py" -Wait:$false

Write-Host ""
Write-Host "The intelligence loop is awake." -ForegroundColor Cyan
Write-Host ""
Write-Host "  Main UI:        http://localhost:8765/manatuabon_v5.html" -ForegroundColor White
Write-Host "  Observatory:    http://localhost:8766/manatuabon_observatory.html" -ForegroundColor Magenta
Write-Host "  Agent Bridge:   http://localhost:7777/status" -ForegroundColor Green
Write-Host ""
Write-Host "You can close the new PowerShell windows to stop the servers." -ForegroundColor Gray
Write-Host ""
