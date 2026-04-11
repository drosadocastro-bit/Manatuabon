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

Write-Host "[1/14] Migrating database schema..." -ForegroundColor Yellow
& $PythonExe db_init.py

Write-Host "[2/14] Starting UI Server (Port 8765)..." -ForegroundColor Green
Start-Process -FilePath $PythonExe -ArgumentList "-m http.server 8765" -Wait:$false

Write-Host "[3/14] Starting Agent Brain (Port 7777)..." -ForegroundColor Green
$agentArgs = "set MANATUABON_COUNCIL_GRAPH_MODE=primary && `"$PythonExe`" manatuabon_agent.py --watch `"$ScriptDir\renders`" --inbox `"$ScriptDir\inbox`" --port 7777 --consolidate-every 30"
Start-Process -FilePath "cmd.exe" -ArgumentList "/c", $agentArgs -Wait:$false
Write-Host "      Council graph mode: PRIMARY" -ForegroundColor DarkCyan

Write-Host "[4/14] Securing STScI API Worker (MAST JWST Archive)..." -ForegroundColor Green
Start-Process -FilePath $PythonExe -ArgumentList "mast_worker.py" -Wait:$false

Write-Host "[5/14] Securing ALMA/SETI Radio Worker..." -ForegroundColor Green
Start-Process -FilePath $PythonExe -ArgumentList "radio_worker.py" -Wait:$false

Write-Host "[6/14] Launching Observatory Dashboard (Port 8766)..." -ForegroundColor Magenta
Start-Process -FilePath $PythonExe -ArgumentList "-m http.server 8766" -Wait:$false

Write-Host "[7/14] Starting High-Energy Transient Worker (Swift/MAXI)..." -ForegroundColor Green
Start-Process -FilePath $PythonExe -ArgumentList "transient_worker.py" -Wait:$false

Write-Host "[8/14] Starting Artemis II Mission Tracker..." -ForegroundColor Green
Start-Process -FilePath $PythonExe -ArgumentList "mission_worker.py" -Wait:$false

Write-Host "[9/14] Starting MCP Server (stdio + SSE port 8808)..." -ForegroundColor Yellow
Start-Process -FilePath $PythonExe -ArgumentList "mcp_server.py --sse 8808" -Wait:$false
Write-Host "       MCP tools: search_memories, list_hypotheses, observatory_stats, ..." -ForegroundColor DarkYellow

Write-Host "[10/14] Starting Simulation Worker (orbital, accretion, pulsar, Bayesian)..." -ForegroundColor Cyan
Start-Process -FilePath $PythonExe -ArgumentList "simulation_worker.py --interval 60" -Wait:$false
Write-Host "        Engines: orbital_confinement · accretion_physics · pulsar_glitch_stress · bayesian_update" -ForegroundColor DarkCyan

Write-Host "[11/14] Starting Hypothesis Revision Loop..." -ForegroundColor Cyan
Start-Process -FilePath $PythonExe -ArgumentList "hypothesis_revision_loop.py --interval 120" -Wait:$false
Write-Host "        Re-submits needs_revision hypotheses to council after new evidence is gathered" -ForegroundColor DarkCyan

Write-Host "[12/14] Starting Confidence Decay Worker..." -ForegroundColor Cyan
Start-Process -FilePath $PythonExe -ArgumentList "confidence_decay.py --interval 86400" -Wait:$false
Write-Host "        Applies 3%/30d confidence decay to hypotheses with no recent evidence" -ForegroundColor DarkCyan

Write-Host "[13/14] Starting Galactic Center Monitor + EHT Ingest..." -ForegroundColor Cyan
Start-Process -FilePath $PythonExe -ArgumentList "galactic_center_monitor.py --interval 3600" -Wait:$false
Write-Host "        Polls arXiv, Zenodo EHT, ATel every hour - auto-ingests GC/EHT evidence" -ForegroundColor DarkCyan

Write-Host "[14/14] Starting Vela Pulsar Glitch Watch..." -ForegroundColor Cyan
Start-Process -FilePath $PythonExe -ArgumentList "vela_glitch_watch.py --interval 3600" -Wait:$false
Write-Host "        Monitors arXiv/ATel for Vela glitch reports - validates pulsar engine prediction" -ForegroundColor DarkCyan

Write-Host ""
Write-Host "The intelligence loop is awake." -ForegroundColor Cyan
Write-Host ""
Write-Host "  Main UI:        http://localhost:8765/manatuabon_v5.html" -ForegroundColor White
Write-Host "  Hypothesis Wiki:http://localhost:8765/manatuabon_wiki.html" -ForegroundColor Cyan
Write-Host "  Observatory:    http://localhost:8766/manatuabon_observatory.html" -ForegroundColor Magenta
Write-Host "  Agent Bridge:   http://localhost:7777/status" -ForegroundColor Green
Write-Host "  MCP Server:     http://localhost:8808/sse  (Claude Desktop: stdio)" -ForegroundColor Yellow
Write-Host "  Sim Worker:     polling every 60s   - physics engines on demand" -ForegroundColor Cyan
Write-Host "  Revision Loop:  polling every 120s  - heals needs_revision hypotheses" -ForegroundColor Cyan
Write-Host "  Decay Worker:   polling every 24h   - confidence decay for stale hypotheses" -ForegroundColor Cyan
Write-Host "  GC Monitor:     polling every 1h    - arXiv/Zenodo EHT/ATel evidence ingest" -ForegroundColor Cyan
Write-Host "  Vela Watch:     polling every 1h    - glitch confirmation vs engine prediction" -ForegroundColor Cyan
Write-Host ""
Write-Host "You can close the new PowerShell windows to stop the servers." -ForegroundColor Gray
Write-Host ""
