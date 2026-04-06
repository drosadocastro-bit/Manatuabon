# Manatuabon Walkthrough

A step-by-step guide to using Manatuabon as a governed astrophysics reasoning workspace.

---

## Table of Contents

1. [What Is Manatuabon?](#what-is-manatuabon)
2. [Installation](#installation)
3. [Starting the Stack](#starting-the-stack)
4. [The Main UI](#the-main-ui)
5. [Workflow 1: Ingesting Evidence](#workflow-1-ingesting-evidence)
6. [Workflow 2: Cross-Matching Surveys](#workflow-2-cross-matching-surveys)
7. [Workflow 3: Reviewing Hypotheses](#workflow-3-reviewing-hypotheses)
8. [Workflow 4: Replaying and Exporting](#workflow-4-replaying-and-exporting)
9. [Workflow 5: Synthetic Benchmarks](#workflow-5-synthetic-benchmarks)
10. [The Bridge API](#the-bridge-api)
11. [Database and Memory](#database-and-memory)
12. [Governance Model](#governance-model)
13. [Tips and Troubleshooting](#tips-and-troubleshooting)

---

## What Is Manatuabon?

Manatuabon is a **local-first, human-governed astrophysics workspace**. It does three things:

1. **Fetches and freezes** real observational data from public archives (Gaia, SDSS, Pan-STARRS, ZTF, MAST, arXiv, GraceDB, GWOSC) into auditable structured bundles.
2. **Cross-matches and triages** those bundles through deterministic anomaly workers that score candidates but never claim discoveries autonomously.
3. **Reviews hypotheses** through a governed council of specialized review agents (skeptic, archivist, judge, evidence reviewer, quant reviewer, reflection agent) that produce auditable decisions.

**Key principle:** Manatuabon is a decision-support tool for a human analyst. It surfaces evidence and follow-up cues, but the human always has final authority.

---

## Installation

### Prerequisites

- **Python 3.13** (required)
- **Windows** (primary development platform; Linux/macOS should work for the offline tools)
- **LM Studio** (optional — needed only for live Nemotron-backed query and chat paths)
- **Git** (for cloning)

### Step-by-step

```powershell
# 1. Clone the repository
git clone https://github.com/drosadocastro-bit/Manatuabon.git
cd Manatuabon

# 2. Create a virtual environment
py -3.13 -m venv .venv
.\.venv\Scripts\Activate.ps1

# 3. Install dependencies
python -m pip install --upgrade pip
python -m pip install -r requirements.txt

# 4. Initialize the database
python db_init.py
```

This creates `manatuabon.db` with all required tables and seeds the five founding hypotheses (H14–H18).

### Optional: Cloud reasoning

If you want Claude-backed deep review or cloud judge escalation, create a `.env` file:

```env
ANTHROPIC_API_KEY=your_key_here
```

### Optional: Accurate extinction maps

For accurate per-sightline Galactic extinction (instead of the analytical fallback):

```powershell
pip install dustmaps
python -c "from dustmaps.sfd import fetch; fetch()"
```

This downloads ~300 MB of SFD98 dust map data. Without it, the system uses a conservative csc(|b|) approximation that is adequate for flagging but not for publication-grade photometry.

---

## Starting the Stack

### Full stack (UI + bridge + workers)

```powershell
.\start_manatuabon.ps1
```

This launches 8 components in separate windows:

| Step | Component | Port | Purpose |
|------|-----------|------|---------|
| 1 | Database migration | — | Ensures schema is current |
| 2 | UI Server | 8765 | Serves the main web interface |
| 3 | Agent Brain (Bridge) | 7777 | HTTP API for frontend, memory, hypotheses |
| 4 | MAST Worker | — | JWST/HST archive polling |
| 5 | Radio Worker | — | ALMA/SETI radio archive polling |
| 6 | Observatory Dashboard | 8766 | Separate visualization surface |
| 7 | Transient Worker | — | Swift/BAT, Fermi/LAT, MAXI monitoring |
| 8 | Mission Tracker | — | Artemis II and other NASA missions |

### Verify it is running

Open your browser to:

- **Main UI:** http://localhost:8765/manatuabon_v5.html
- **Observatory:** http://localhost:8766/manatuabon_observatory.html
- **Bridge health:** http://localhost:7777/status

### Minimal mode (offline tools only)

You do not need the full stack to use the importers, workers, or analysis tools. They all work as standalone CLI scripts against `manatuabon.db`.

---

## The Main UI

The web interface at `http://localhost:8765/manatuabon_v5.html` provides:

- **Chat panel**: conversational interface to the agent (requires LM Studio for Nemotron-backed responses)
- **Memory viewer**: browse all stored memories with timestamps, tags, and hypothesis links
- **Hypothesis dashboard**: view all hypotheses, their council decisions, evidence tiers, and review priority
- **Timeline visualization**: chronological view of ingested evidence and events
- **Sky map**: coordinate-based visualization of ingested observations
- **Network graph**: memory-to-hypothesis linking topology

The observatory at port 8766 shows transient events, mission telemetry, and real-time monitoring panels.

---

## Workflow 1: Ingesting Evidence

This is the most common workflow. You fetch data from a public archive and store it as a governed evidence bundle.

### The pattern

Every importer follows the same three-step cycle:

```
Fetch from archive → Write raw snapshot + structured bundle → Optionally ingest into DB
```

### Step 1: Fetch a Gaia snapshot

Pick a sky position (e.g., Sgr A* at RA=266.42, Dec=−29.01):

```powershell
python gaia_snapshot_importer.py `
  --ra-center 266.4168 `
  --dec-center -29.0078 `
  --radius-deg 0.5 `
  --max-results 100 `
  --inbox D:/Manatuabon/data `
  --db D:/Manatuabon/manatuabon.db `
  --agent-log D:/Manatuabon/agent_log.json `
  --ingest --evidence-only
```

**What happens:**
1. The importer queries Gaia DR3 via TAP for stars within 0.5° of Sgr A*.
2. It writes a raw JSON snapshot in `data/` (the exact API response, frozen for audit).
3. It writes a `structured_ingest_v1` bundle with normalized stellar parameters.
4. `--ingest` tells it to also load the bundle into `manatuabon.db`.
5. `--evidence-only` stores it as provenance in memory without auto-generating a new hypothesis.

### Step 2: Fetch matching SDSS data for the same region

```powershell
python sdss_snapshot_importer.py `
  --ra-center 266.4168 `
  --dec-center -29.0078 `
  --radius-arcmin 60 `
  --max-results 50 `
  --object-type galaxy `
  --inbox D:/Manatuabon/data `
  --db D:/Manatuabon/manatuabon.db `
  --agent-log D:/Manatuabon/agent_log.json `
  --ingest --evidence-only
```

The SDSS importer has a defensive fallback chain: SQL query → relaxed SQL (drop type filter) → spectroscopic join → radial search. You do not need to worry about which path it takes — the bundle records which query mode was used.

### Step 3: Fetch Pan-STARRS for the same field

```powershell
python panstarrs_snapshot_importer.py `
  --ra-center 266.4168 `
  --dec-center -29.0078 `
  --radius-deg 0.05 `
  --max-results 50 `
  --min-detections 1 `
  --inbox D:/Manatuabon/data `
  --db D:/Manatuabon/manatuabon.db `
  --agent-log D:/Manatuabon/agent_log.json `
  --ingest --evidence-only
```

### Other importers

| Importer | Archive | Example target |
|----------|---------|----------------|
| `ztf_snapshot_importer.py` | ZTF / IRSA | Time-domain frames near a transient |
| `mast_snapshot_importer.py` | MAST (JWST, HST) | `--target M87 --collections JWST,HST` |
| `arxiv_snapshot_importer.py` | arXiv | Keyword search for recent papers |
| `gracedb_snapshot_importer.py` | GraceDB | `--superevent-id S190425z` |
| `gwosc_snapshot_importer.py` | GWOSC | `--event-version GW241110_124123-v1` |

### What `--evidence-only` does

- **Without it:** The bundle is ingested and an `AUTO-*` hypothesis is auto-generated from its content.
- **With it:** The bundle is stored as memory with a link to an existing supporting hypothesis (if specified via `--supports-hypothesis`), but no new hypothesis is created. Use this for routine archive pulls where you do not want hypothesis noise.

### What extinction enrichment does

SDSS and Pan-STARRS importers automatically look up the Galactic extinction E(B-V) for every catalogued object's coordinates. Each row in the bundle gets:

- `extinction_ebv`: the reddening value
- `extinction_method`: `"sfd"` (if dustmaps installed) or `"analytical_csc_b"` (lightweight fallback)

This is used downstream by the Gaia × Pan-STARRS anomaly worker to compute dereddened colors for scoring.

---

## Workflow 2: Cross-Matching Surveys

Once you have bundles from two different archives for the same sky region, you can cross-match them to find anomaly candidates.

### Gaia × Pan-STARRS (color-aware triage)

```powershell
python gaia_panstarrs_anomaly_worker.py `
  --gaia-bundle D:/Manatuabon/data/gaia_snapshot_bundle_sgra_20260406.json `
  --panstarrs-bundle D:/Manatuabon/data/panstarrs_snapshot_bundle_sgra_20260406.json `
  --max-sep-arcsec 5 `
  --pm-threshold-masyr 10 `
  --min-detections 3 `
  --inbox D:/Manatuabon/data `
  --db D:/Manatuabon/manatuabon.db `
  --agent-log D:/Manatuabon/agent_log.json `
  --ingest
```

**What it produces:**

An anomaly profile with ranked candidates. Each candidate carries:

| Field | Meaning |
|-------|---------|
| `separation_arcsec` | Angular distance between Gaia and Pan-STARRS positions |
| `proper_motion_total_masyr` | Total proper motion from Gaia |
| `pm_total_error_masyr` | Propagated proper-motion uncertainty |
| `parallax`, `parallax_error`, `parallax_snr` | Distance indicators |
| `g_r_color`, `r_i_color` | Raw Pan-STARRS colors |
| `g_r_color_dereddened`, `r_i_color_dereddened` | Extinction-corrected colors |
| `extinction_ebv` | E(B-V) at this sightline |
| `candidate_score` | Composite triage score (0–1) |
| `review_priority` | `high`, `medium`, or `low` |
| `foreground_likely` | Whether parallax suggests a foreground star |

### Gaia × SDSS (redshift-aware triage)

```powershell
python gaia_sdss_anomaly_worker.py `
  --gaia-bundle D:/Manatuabon/data/gaia_snapshot_bundle_sgra_20260406.json `
  --sdss-bundle D:/Manatuabon/data/sdss_snapshot_bundle_sgra_20260406.json `
  --max-sep-arcsec 30 `
  --pm-threshold-masyr 10 `
  --redshift-threshold 0.05 `
  --inbox D:/Manatuabon/data `
  --db D:/Manatuabon/manatuabon.db `
  --agent-log D:/Manatuabon/agent_log.json `
  --ingest
```

### Gaia × ZTF (time-domain triage)

```powershell
python gaia_ztf_anomaly_worker.py `
  --gaia-bundle D:/Manatuabon/data/gaia_snapshot_bundle_sgra_20260406.json `
  --ztf-bundle D:/Manatuabon/data/ztf_snapshot_bundle_sgra_20260406.json `
  --max-sep-arcsec 30 `
  --pm-threshold-masyr 10 `
  --seeing-threshold 2.5 `
  --inbox D:/Manatuabon/data `
  --db D:/Manatuabon/manatuabon.db `
  --agent-log D:/Manatuabon/agent_log.json `
  --ingest
```

### How scoring works

All three workers score candidates with a weighted composite:

| Component | Weight | What it rewards |
|-----------|--------|-----------------|
| Proximity | 0.40 | Closer angular separations |
| Proper motion | 0.25 | Higher Gaia proper motion |
| Detection persistence | 0.15 | More Pan-STARRS detections / better ZTF seeing |
| Color presence | 0.10 | Usable multiband photometry (Pan-STARRS) |
| Parallax SNR | 0.05 | Higher parallax signal-to-noise |
| RUWE quality | 0.05 | Good Gaia astrometric quality (RUWE ≤ 1.4) |
| Quality flag penalty | −0.20 | Non-zero survey quality flags |

**Important:** A high score means "this is worth a human look," not "this is an anomaly." The workers deliberately surface follow-up cues rather than claiming discoveries.

---

## Workflow 3: Reviewing Hypotheses

Hypotheses enter the system in two ways:

1. **Auto-generated** during bundle ingest (when `--evidence-only` is not used).
2. **Manually created** through the UI or API.

### The council review pipeline

Every hypothesis goes through a multi-agent review:

```
Hypothesis → Normalizer → Auto-reject gate → Skeptic → Archivist → Evidence Reviewer
    → Quant Reviewer → Judge → Reflection → Final Decision
```

| Agent | Role | Output |
|-------|------|--------|
| **Normalizer** | Enforces strict schema on the hypothesis | Cleaned hypothesis |
| **Auto-reject gate** | Hard-rule rejection (no predictions, low confidence, empty claim) | Pass/reject |
| **Skeptic** | Challenges the claim with objections | `weak` / `plausible` / `strong` |
| **Archivist** | Checks for duplicates against existing hypotheses | `unique` / `partial_overlap` / `duplicate` |
| **Evidence Reviewer** | Classifies supporting evidence into tiers | Tier A (direct) / B (contextual) / C (speculative) |
| **Quant Reviewer** | Checks numerical claims and physics constraints | Score contributions |
| **Judge** | Weighs all reviews and decides | `accepted` / `rejected` / `needs_revision` / `held` / `merged` |
| **Reflection** | If borderline, proposes concrete revisions or evidence requests | Revisions or follow-up tasks |

### Evidence tiers and decision caps

The evidence policy enforces safety rails:

- **Only Tier C (speculative) evidence?** → Decision capped at *held*.
- **No Tier A (direct) evidence?** → Decision capped at *needs_revision*.
- **Tier A evidence present?** → Full acceptance is possible.

### Triggering review manually

Via the bridge API:

```powershell
# Reprocess an existing hypothesis
Invoke-RestMethod -Uri "http://localhost:7777/api/council/reprocess" `
  -Method POST `
  -ContentType "application/json" `
  -Body '{"hypothesis_id": "H14"}'
```

Via the UI: the hypothesis dashboard has a "Reprocess" button for each hypothesis.

### Evidence requests

When the council holds a hypothesis, it generates structured evidence requests specifying what kind of Tier A or B evidence would resolve the hold. These appear in the UI and in the `/api/evidence-requests` endpoint.

---

## Workflow 4: Replaying and Exporting

### Replay: reproduce analysis from saved bundles

Create a manifest JSON listing the bundle paths and worker configs:

```json
{
  "description": "Sgr A* field triage replay",
  "steps": [
    {
      "worker": "gaia_panstarrs",
      "gaia_bundle": "data/gaia_snapshot_bundle_sgra_20260406.json",
      "panstarrs_bundle": "data/panstarrs_snapshot_bundle_sgra_20260406.json",
      "max_sep_arcsec": 5.0,
      "pm_threshold_masyr": 10.0,
      "min_detections": 3
    }
  ]
}
```

Run it:

```powershell
python replay_manifest.py `
  --manifest D:/Manatuabon/my_replay.json `
  --out-dir D:/Manatuabon/replay_output `
  --ingest
```

This produces a timestamped `replay_report_*.json` with all worker outputs. Because the workers are deterministic, the same inputs always produce the same outputs.

### Export: CSV and markdown for papers

```powershell
python analysis_export.py `
  --profiles D:/Manatuabon/data/gaia_panstarrs_anomaly_profile_*.json `
  --out-dir D:/Manatuabon/exports
```

This produces:
- A flat **CSV** with one row per candidate (all scoring and uncertainty fields)
- A **markdown summary table** suitable for paper supplementary material
- A **summary report** with aggregate statistics

---

## Workflow 5: Synthetic Benchmarks

Synthetic data stays on a separate path from real observations so it never contaminates council evidence tiers.

### Step 1: Ingest a synthetic dataset manifest

```powershell
python openuniverse_snapshot_importer.py `
  --dataset openuniverse2024 `
  --inbox D:/Manatuabon/data `
  --db D:/Manatuabon/manatuabon.db `
  --agent-log D:/Manatuabon/agent_log.json `
  --ingest
```

### Step 2: Score benchmark readiness

```powershell
python anomaly_benchmark_worker.py `
  --bundle D:/Manatuabon/data/openuniverse_snapshot_bundle_openuniverse2024_*.json `
  --inbox D:/Manatuabon/data `
  --db D:/Manatuabon/manatuabon.db `
  --agent-log D:/Manatuabon/agent_log.json `
  --ingest
```

### Step 3: Normalize and cross-match synthetic catalogs

If you have actual extracted catalog files (Roman, Rubin, Truth), run the full pipeline:

```powershell
python openuniverse_catalog_pipeline.py `
  --roman D:/Manatuabon/data/roman_export.json `
  --rubin D:/Manatuabon/data/rubin_export.json `
  --truth D:/Manatuabon/data/truth_export.json `
  --out-dir D:/Manatuabon/data `
  --normalized-format both `
  --max-sep-arcsec 1.0 `
  --db D:/Manatuabon/manatuabon.db `
  --agent-log D:/Manatuabon/agent_log.json `
  --ingest
```

This normalizes all three catalogs, cross-matches them, computes positional and flux residuals, and writes an anomaly bundle — all in one command.

---

## The Bridge API

The agent bridge at `http://localhost:7777` exposes a REST API. Here are the most useful endpoints:

### Health and status

```powershell
# Check if the bridge is alive
Invoke-RestMethod http://localhost:7777/status
```

### Memory and chat

```powershell
# Get all memories
Invoke-RestMethod http://localhost:7777/memories

# Chat with the agent (requires LM Studio)
Invoke-RestMethod -Uri http://localhost:7777/api/chat -Method POST `
  -ContentType "application/json" `
  -Body '{"message": "What do we know about pulsars?"}'

# Get chat history
Invoke-RestMethod http://localhost:7777/api/chat
```

### Hypotheses

```powershell
# List all hypotheses
Invoke-RestMethod http://localhost:7777/api/hypotheses/all

# Get council decisions
Invoke-RestMethod http://localhost:7777/api/council/decisions

# Get full review audit trail
Invoke-RestMethod http://localhost:7777/api/council/reviews
```

### Manual ingest

```powershell
# Ingest text as a new memory (requires LM Studio for hypothesis extraction)
Invoke-RestMethod -Uri http://localhost:7777/ingest -Method POST `
  -ContentType "application/json" `
  -Body '{"text": "Observation: unusual proper motion detected in field X", "source": "manual"}'
```

### Visualizations

```powershell
Invoke-RestMethod http://localhost:7777/api/viz/timeline    # Event timeline
Invoke-RestMethod http://localhost:7777/api/viz/skymap      # Sky coordinates
Invoke-RestMethod http://localhost:7777/api/viz/hypotheses  # Hypothesis network
Invoke-RestMethod http://localhost:7777/api/viz/network     # Memory link topology
Invoke-RestMethod http://localhost:7777/api/viz/transients  # Swift/Fermi/MAXI events
Invoke-RestMethod http://localhost:7777/api/viz/missions    # Space mission tracking
```

### Evidence requests and governance

```powershell
# Get pending evidence requests
Invoke-RestMethod http://localhost:7777/api/evidence-requests

# Get memory link proposals for human review
Invoke-RestMethod http://localhost:7777/api/memory-link-proposals

# Override a hypothesis decision (requires rationale)
Invoke-RestMethod -Uri http://localhost:7777/api/council/override -Method POST `
  -ContentType "application/json" `
  -Body '{"hypothesis_id": "H14", "new_status": "held", "rationale": "Need Tier A evidence before proceeding"}'
```

---

## Database and Memory

### Where data lives

| File | Contents |
|------|----------|
| `manatuabon.db` | SQLite database — all memories, hypotheses, decisions, reviews, evidence requests |
| `agent_log.json` | Append-only activity log for the agent |
| `data/` | Raw snapshots, structured bundles, worker outputs, exported files |
| `inbox/` | Drop zone for file watcher (auto-ingest on file arrival) |
| `models/all-MiniLM-L6-v2/` | Local embedding model for similarity search |

### Key database tables

| Table | What it stores |
|-------|----------------|
| `hypotheses` | Every hypothesis with title, evidence, status, origin, confidence |
| `memories` | Agent observations linked to hypotheses by concept/domain tags |
| `hypothesis_decisions` | Final council verdicts with score breakdowns |
| `hypothesis_reviews` | Individual agent review records (skeptic, archivist, judge, etc.) |
| `confidence_history` | Confidence score evolution over time |
| `evidence_requests` | Follow-up tasks generated when hypotheses are held |
| `memory_link_proposals` | Human-reviewed memory-to-hypothesis link suggestions |
| `hypothesis_overrides` | Manual governance overrides with rationale and audit trail |

### The founding hypotheses

The database seeds five hypotheses on first initialization:

| ID | Title |
|----|-------|
| H14 | The Pulsar Timing Web |
| H15 | Atmospheric Disequilibrium Worlds |
| H16 | Quasar Flicker Cartography |
| H17 | The Late-Time Expansion Tension |
| H18 | The Cosmic Web as an Engine |

These provide anchor points for evidence linking from the start.

---

## Governance Model

Manatuabon enforces a strict governance model:

### What the system will NOT do

- Make autonomous scientific claims
- Silently merge hypotheses
- Auto-execute real-world actions
- Bypass evidence tier requirements
- Generate hypotheses from synthetic data

### What it WILL do

- Freeze raw upstream responses before any processing
- Record every decision's full audit trail
- Require human review for link proposals
- Cap decisions based on evidence quality
- Separate synthetic benchmarks from real evidence

### The evidence-first hierarchy

```
Tier A: Direct observational evidence (observations, measurements)
    ↓ allows acceptance
Tier B: Contextual evidence (papers, catalog cross-references)
    ↓ allows needs_revision
Tier C: Speculative evidence (theoretical predictions, simulations)
    ↓ caps at held
```

### Council graph modes

| Mode | Behavior |
|------|----------|
| `primary` | Full review pipeline with database persistence |
| `shadow` | Runs the same pipeline without writing to DB (for comparison) |

Set the mode via environment variable before starting the agent:

```powershell
$env:MANATUABON_COUNCIL_GRAPH_MODE = "primary"
```

---

## Tips and Troubleshooting

### "LM Studio is not running"

The chat, query, and ingest-with-hypothesis-extraction paths require LM Studio with a Nemotron-compatible model. The snapshot importers, cross-match workers, analysis tools, and council review do **not** need LM Studio — they work entirely offline.

### Sparse fields returning no data

The SDSS and Pan-STARRS importers have multi-stage fallback chains specifically for this. If your target region is genuinely empty in a catalog, the bundle will still be written (with zero rows and appropriate review flags) so you have an auditable record of the attempt.

### Extinction shows `analytical_csc_b`

This means dustmaps SFD98 map files are not installed. The analytical fallback is adequate for flagging but not for publication-grade work. Install dustmaps and fetch the maps for accurate per-sightline values.

### Running tests

```powershell
# Run the full 27-test suite (92 test functions)
$tests = @(
  'test_anomaly_benchmark_worker.py'
  'test_cross_survey_catalog_anomaly_worker.py'
  'test_gaia_panstarrs_anomaly_worker.py'
  'test_gaia_snapshot_importer.py'
  'test_gaia_sdss_anomaly_worker.py'
  'test_gaia_ztf_anomaly_worker.py'
  'test_mast_snapshot_importer.py'
  'test_panstarrs_snapshot_importer.py'
  'test_sdss_snapshot_importer.py'
  'test_openuniverse_catalog_normalizer.py'
  'test_openuniverse_catalog_pipeline.py'
  'test_ztf_snapshot_importer.py'
  'test_arxiv_snapshot_importer.py'
  'test_gracedb_snapshot_importer.py'
  'test_gwosc_snapshot_importer.py'
  'test_openuniverse_snapshot_importer.py'
  'test_pulsar_glitch_importer.py'
  'test_pulsar_recovery_paper_importer.py'
  'test_council_evidence_policy.py'
  'test_council_evidence_requests.py'
  'test_council_quant_reviewer.py'
  'test_council_reflection.py'
  'test_council_graph.py'
  'test_watcher_handler.py'
  'test_replay_manifest.py'
  'test_analysis_export.py'
  'test_extinction_lookup.py'
)
foreach ($t in $tests) { python $t }
```

### Recommended scientific-use pattern

1. **Fetch once** and preserve the raw snapshot plus structured bundle in `data/`.
2. **Run workers** against those saved bundles, not against fresh live queries.
3. **Use `replay_manifest.py`** for reproducible analysis packages.
4. **Use `analysis_export.py`** for CSV + markdown exports.
5. **Treat outputs as decision support**, then do image-, calibration-, and uncertainty-aware follow-up before claiming a result.

---

*Manatuabon — Truth > Fluency · Refusal > Hallucination · Evidence > Intuition*
