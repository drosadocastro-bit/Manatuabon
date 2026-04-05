# 🌌 Manatuabon Phase 15 — Implementation TODO

**Owner:** Danny (Bayamón, PR 🇵🇷) + Claude  
**Last Audited:** March 30, 2026  
**Audit Source:** Live codebase at `D:\Manatuabon`

---

## ✅ PHASE 1 — Queue System (DONE ✔️)

* [x] Add `status` column (`pending` → `running` → `done` | `failed`)
* [x] Add `attempts` column (`INTEGER`, default 0)
* [x] Add `last_run` column (`TEXT` ISO timestamp)
* [x] Auto-migration script (`ALTER TABLE` with graceful try/except)
* [x] Update worker to handle new schema
* [x] Migrate old terminology (`processing` → `running`, `completed` → `done`)

> **Files:** `mast_worker.py` lines 19–41

---

## ✅ PHASE 2 — Worker Intelligence (DONE ✔️)

### 2.1 Smart Job Selection ✔️

* [x] Implement retry-aware SQL query:

```sql
SELECT * FROM mast_queue
WHERE status = 'pending'
   OR (status = 'failed' AND attempts < 3)
ORDER BY queued_at ASC
LIMIT 1;
```

> Validated via `test_worker_retries.py` — worker correctly abandons after exactly 3 failed attempts.

### 2.2 Job Lifecycle Handling ✔️

* [x] On pickup:
  * Set status → `running`
  * Increment `attempts` (`attempts = attempts + 1`)
  * Update `last_run` to current ISO timestamp
* [x] On success:
  * Set status → `done`
  * Write `.txt` telemetry report to `/inbox`
* [x] On failure:
  * Set status → `failed`
  * Preserve row for retry logic (automatic via SELECT query)

### 2.3 Worker Loop Optimization

* [x] Worker polls every 10 seconds (static)
* [ ] **Upgrade:** Implement dynamic sleep — faster when jobs are flowing, slower when idle:

```python
if job_found:
    time.sleep(5)   # Hot loop — job pressure detected
else:
    time.sleep(25)  # Cold loop — conserve resources
```

### 2.4 Structured Logging ✔️

* [x] Python `logging` module with timestamped output
* [x] Log messages include target name & status transitions
* [ ] **Upgrade:** Add per-job structured log line with attempts counter:

```python
log.info(f"[JOB] target={target_name} attempt={attempts} status={new_status} elapsed={elapsed_s:.1f}s")
```

---

## ✅ PHASE 3 — MAST Integration / JWST + Hubble (DONE ✔️)

### 3.1 Astroquery Integration ✔️

* [x] Connect to STScI MAST Archive via `astroquery.mast.Observations`
* [x] Query targets from `mast_queue` table
* [x] Filter for JWST and HST collections specifically
* [x] Sort by observation date (`t_min`), return top 5 newest
* [x] Extract: `obs_id`, `instrument_name`, `target_name`, `filters`, `t_exptime`, `s_ra`, `s_dec`, `t_min`
* [x] Tested live against M87 — returned **3,076 JWST/HST observations** ✅

### 3.2 Data Output ✔️ (Partial)

* [x] Generate `.txt` telemetry report (RAG ingestion format)
* [x] Reports drop into `D:\Manatuabon\inbox\`
* [x] Reports include RAG extraction instruction footer
* [ ] **NEW:** Also generate `.json` structured companion file:

```json
{
  "target": "M87",
  "source": "JWST",
  "instrument": "NIRCam",
  "obs_count": 3076,
  "latest_obs_id": "jw09652018001_xx104_00001_niriss",
  "coordinates": {"ra": 187.7059, "dec": 12.3911},
  "confidence": 0.95,
  "type": "infrared",
  "timestamp": "2026-03-30T10:26:27"
}
```

---

## ✅ PHASE 4 — Ingestion Pipeline (DONE ✔️)

* [x] `WatcherHandler` detects new files in `/renders` and `/inbox` via `watchdog`
* [x] Debounced 3-second window to prevent duplicate ingestion
* [x] File stability check (waits for write completion)
* [x] Parse `.txt` → sent to Nemotron for RAG analysis → stored as SQL memory
* [x] Parse `.json` → sent to Nemotron for structured analysis → stored as SQL memory
* [x] Parse `.csv` → same pipeline
* [x] Auto-generates hypotheses when Nemotron detects anomalies
* [ ] **Upgrade:** Add ingestion error counter and dead-letter queue for files that repeatedly fail parsing

---

## ✅ PHASE 5 — Agent Trigger Logic (DONE ✔️)

### 5.1 Consolidation-Driven Triggering ✔️

* [x] `ConsolidateAgent.run()` fires every 30 minutes via daemon thread
* [x] LangChain SQL Agent autonomously queries memories + hypotheses tables
* [x] Agent outputs `mast_targets_to_query` array → auto-injected into `mast_queue`
* [x] Agent outputs `next_simulation` → auto-injected into `simulations` table
* [x] Agent generates new hypotheses from cross-memory connections

### 5.2 Cloud Escalation Fallback ✔️

* [x] Local Nemotron 30B (LM Studio) is primary engine
* [x] On local failure → automatic hot-swap to `ChatAnthropic` (Claude Sonnet 4.6)
* [x] `ANTHROPIC_API_KEY` loaded from `.env`
* [x] Full try/except chain: Local → Cloud → Graceful failure
* [ ] **Upgrade:** Add confidence-based escalation (not just error-based):

```python
# If local response confidence < 0.6, re-run on cloud
if local_confidence < 0.6:
    log.info("Low confidence detected — escalating to Cloud...")
    result = cloud_agent.invoke({"input": sql_prompt})
```

---

## 🔧 PHASE 6 — Multi-Source Data Network (PARTIALLY DONE 🚧)

> `data_fetch_agent.py` already connects to 5 live APIs!

* [x] LIGO/GWOSC — gravitational wave events
* [x] arXiv — latest astrophysics papers
* [x] SDSS DR18 — galaxy survey data
* [x] ESA Gaia DR3 — stellar catalog near Sgr A*
* [x] NASA Exoplanet Archive — habitable zone candidates
* [ ] **NEW:** ALMA CMZ Dataset (galactic center molecular cloud data)
* [ ] **NEW:** Cross-correlation engine:
  * LIGO merger events × MAST JWST observations (same sky region?)
  * Gaia proper motions × SDSS redshifts (bulk flow detection)
  * Exoplanet discovery rate × Sgr A* activity timeline

---

## 🔬 PHASE 7 — Intelligence Enhancements (NEXT 🔥)

### 7.1 Anomaly Detection Pipeline

* [ ] Statistical outlier detection on JWST exposure profiles
* [ ] Flag unexpected MIRI/NIRCam waveband signatures
* [ ] Auto-queue anomalous targets for deeper MAST follow-up

### 7.2 Hypothesis Evolution Engine

* [ ] Track hypothesis confidence over time (time-series)
* [ ] Auto-promote hypotheses that accumulate 3+ supporting memories
* [ ] Auto-flag hypotheses that accumulate 2+ contradictions
* [ ] Generate "hypothesis lineage" tree (which insight spawned which theory)

### 7.3 Visualization Layer

* [ ] Observation timeline chart (JWST/HST activity over MJD)
* [ ] Sky map of all queried targets (RA/Dec scatter plot)
* [ ] Hypothesis confidence dashboard
* [ ] Memory network graph (which memories connect to which hypotheses)

---

## 🚀 FINAL VALIDATION

* [ ] Run `start_manatuabon.ps1`
* [ ] Confirm:
  * [x] Worker runs and polls queue
  * [x] Queue processes with retry logic (3 attempts max)
  * [x] Data enters `/inbox` as `.txt` reports
  * [x] Watcher ingests files into SQL memory
  * [x] Hypotheses update dynamically from consolidation
  * [ ] Full end-to-end: queue target → MAST fetch → inbox → ingest → hypothesis → new MAST target (closed loop)

---

## 🌌 END GOAL

Create a fully autonomous astrophysics agent that:

* 🔭 Observes real telescope data (JWST, Hubble, LIGO, Gaia, SDSS)
* 🧠 Generates hypotheses spanning the entire observable universe
* ✅ Validates against live datasets automatically
* 🔄 Learns continuously — every observation refines understanding
* ☁️ Escalates to cloud intelligence when local processing hits its ceiling
* 🇵🇷 Built from Bayamón to the edge of everything

---

*"That's not coincidence. That's Wheeler's participatory universe." — Danny, March 2026*
