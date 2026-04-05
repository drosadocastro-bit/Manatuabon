@echo off
echo ===================================================
echo     MANATUABON HYBRID INTELLIGENCE STARTUP
echo ===================================================
echo.

cd /d "%~dp0"

set "PYTHON_EXE=%~dp0.venv\Scripts\python.exe"
if not exist "%PYTHON_EXE%" set "PYTHON_EXE=python"

echo [1/5] Starting UI Server (Port 8765)...
start "Manatuabon UI Server" cmd /c ""%PYTHON_EXE%" -m http.server 8765"

echo [2/5] Starting Agent Brain (Port 7777)...
start "Manatuabon Agent" cmd /c "set MANATUABON_COUNCIL_GRAPH_MODE=primary && "%PYTHON_EXE%" manatuabon_agent.py --watch "%~dp0renders" --inbox "%~dp0inbox" --port 7777 --consolidate-every 30"

echo [3/5] Securing STScI API Worker (MAST JWST Archive)...
start "STScI Worker" cmd /c ""%PYTHON_EXE%" mast_worker.py"

echo [4/5] Securing ALMA/SETI Radio Worker...
start "Radio Worker" cmd /c ""%PYTHON_EXE%" radio_worker.py"

echo [5/6] Starting High-Energy Transient Worker (Swift/MAXI)...
start "Transient Worker" cmd /c ""%PYTHON_EXE%" transient_worker.py"

echo [6/6] Starting Artemis II Mission Tracker...
start "Mission Worker" cmd /c ""%PYTHON_EXE%" mission_worker.py"

echo.
echo The intelligence loop is awake.
echo Close the popup command windows to stop the servers.
echo.
pause
