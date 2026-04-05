# Manatuabon Setup

This document captures the runtime assumptions that were verified in the workspace on April 3, 2026.

## Python

- Recommended version: Python 3.13
- Verified environment: virtual environment at `.venv`

Create and populate the environment:

```powershell
py -3.13 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

## Required local assets

- LM Studio running locally with OpenAI-compatible API enabled at `http://127.0.0.1:1234`
- A Nemotron-compatible model loaded in LM Studio
- Local embedding model already present at `models/all-MiniLM-L6-v2`

## Optional cloud configuration

Create `.env` in the workspace root if you want cloud judge fallback or Anthropic escalation:

```env
ANTHROPIC_API_KEY=your_key_here
```

## Database initialization

Initialize schema and migrate legacy JSON-backed state into SQLite:

```powershell
.\.venv\Scripts\python.exe .\db_init.py
```

## Start the stack

Preferred startup path:

```powershell
.\start_manatuabon.ps1
```

This now initializes the database before starting the UI, bridge, and workers.

## Main endpoints

- Main UI: `http://localhost:8765/manatuabon_v5.html`
- Observatory: `http://localhost:8766/manatuabon_observatory.html`
- Bridge status: `http://127.0.0.1:7777/status`

## What was validated

- Bridge startup and `/status`
- `/api/chat` persistence
- `/query` with LM Studio active
- `/ingest` with LM Studio active
- Hypothesis council initialization with local MiniLM embeddings
- Worker import readiness for MAST, ALMA, and HEASARC via `astroquery`

## Known runtime expectations

- If LM Studio is not running, Nemotron-backed query and ingest paths will degrade or fail.
- The council can initialize locally, but cloud judge fallback still requires `ANTHROPIC_API_KEY`.
- `start_manatuabon.ps1` launches long-running processes in separate windows; closing those windows stops the services.

## Manual commands

Run the agent directly:

```powershell
.\.venv\Scripts\python.exe .\manatuabon_agent.py --watch "d:\Manatuabon\renders" --inbox "d:\Manatuabon\inbox" --port 7777 --consolidate-every 30
```

Run the radio worker directly:

```powershell
.\.venv\Scripts\python.exe .\radio_worker.py --db "d:\Manatuabon\manatuabon.db" --inbox "d:\Manatuabon\inbox" --poll-interval 15
```

Run the MAST worker directly:

```powershell
.\.venv\Scripts\python.exe .\mast_worker.py
```