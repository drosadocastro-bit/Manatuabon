# Manatuabon — Modularization Plan

**Status:** Proposed  
**Date:** April 11, 2026  
**Owner:** Danny + Copilot  
**ADR:** docs/decisions/ADR-001.md

---

## Problem

Manatuabon has grown organically from a single-file prototype to ~25 Python
modules with ~12,000 lines of code. The root directory contains every file flat,
with no package structure. Key concerns:

| Symptom | Impact |
|---|---|
| `manatuabon_agent.py` is 2,685 lines with 5 classes | Hard to navigate, impossible to test in isolation |
| 12+ workers in root with no shared base class | Duplicated polling, retry, and status logic |
| 15+ importers repeating the same pattern | Copy-paste propagates bugs silently |
| 36 test files in root alongside source code | No separation of tests from production code |
| No `__init__.py` or package structure | Can't use relative imports or proper namespacing |

## Target Architecture

```
manatuabon/                          # Python package root
├── __init__.py
├── core/                            # Brain — the central agent
│   ├── __init__.py
│   ├── memory_manager.py            # MemoryManager (1,367 lines → own module)
│   ├── ingest_agent.py              # IngestAgent + WatcherHandler
│   ├── consolidate_agent.py         # ConsolidateAgent
│   ├── agent_log.py                 # AgentLog
│   └── nemotron_client.py           # NemotronClient (LM Studio adapter)
│
├── governance/                      # Council + evidence review
│   ├── __init__.py
│   ├── council.py                   # HypothesisCouncil orchestrator
│   ├── scoring.py                   # ScoringEngine + DecisionEngine
│   ├── agents/                      # Individual council agents
│   │   ├── __init__.py
│   │   ├── skeptic.py
│   │   ├── archivist.py
│   │   ├── judge.py
│   │   ├── evidence_reviewer.py
│   │   ├── quant_reviewer.py
│   │   └── reflection.py
│   ├── evidence_hunter.py           # Active evidence seeking
│   ├── hypothesis_revision_loop.py  # Continuous re-review
│   └── confidence_decay.py          # Confidence auto-decay
│
├── workers/                         # Queue-based task processors
│   ├── __init__.py
│   ├── base_worker.py               # BaseWorker ABC (shared poll + retry)
│   ├── mast_worker.py
│   ├── radio_worker.py
│   ├── transient_worker.py
│   ├── mission_worker.py
│   └── simulation/
│       ├── __init__.py
│       ├── simulation_worker.py     # SimulationWorker dispatch
│       ├── orbital.py               # Orbital confinement engine
│       ├── accretion.py             # Bondi accretion engine
│       ├── pulsar_glitch.py         # Vela crustal stress engine
│       └── bayesian.py              # Generic Bayesian update
│
├── fetchers/                        # External data ingestion
│   ├── __init__.py
│   ├── base_fetcher.py              # Shared fetch + rate-limit + offline guard
│   ├── ligo.py
│   ├── arxiv.py
│   ├── sdss.py
│   ├── gaia.py
│   ├── exoplanets.py
│   ├── swift_bat.py
│   ├── fermi_lat.py
│   └── maxi.py
│
├── importers/                       # Snapshot importers + anomaly detectors
│   ├── __init__.py
│   ├── base_snapshot_importer.py    # Shared: parse, bundle, ingest
│   ├── base_anomaly_detector.py     # Shared: cross-match, flag, score
│   ├── arxiv_importer.py
│   ├── gaia_importer.py
│   ├── sdss_importer.py
│   ├── mast_importer.py
│   ├── gracedb_importer.py
│   ├── ztf_importer.py
│   ├── panstarrs_importer.py
│   └── anomaly/
│       ├── __init__.py
│       ├── gaia_panstarrs.py
│       ├── gaia_sdss.py
│       ├── gaia_ztf.py
│       └── cross_survey.py
│
├── db/                              # Database layer
│   ├── __init__.py
│   ├── schema.py                    # CREATE TABLE + migrations (from db_init.py)
│   └── seeds.py                     # Foundational hypotheses H14-H18
│
├── bridge/                          # HTTP API server
│   ├── __init__.py
│   ├── server.py                    # Main HTTP server + routing
│   ├── handlers.py                  # Endpoint handlers
│   └── constants.py                 # Status enums, error shapes, limits
│
├── monitors/                        # Long-running observers
│   ├── __init__.py
│   ├── galactic_center_monitor.py
│   └── vela_glitch_watch.py
│
└── mcp/                             # Model Context Protocol server
    ├── __init__.py
    └── server.py

tests/                               # All tests in dedicated directory
├── conftest.py                      # Shared fixtures (tmp_db, mock_memory, etc.)
├── test_core/
│   ├── test_memory_manager.py
│   ├── test_ingest_agent.py
│   └── test_consolidate_agent.py
├── test_governance/
│   ├── test_council.py
│   ├── test_evidence_policy.py
│   ├── test_evidence_hunter.py
│   └── test_scoring.py
├── test_workers/
│   ├── test_base_worker.py
│   ├── test_mast_worker.py
│   ├── test_simulation_worker.py
│   └── test_worker_retries.py
├── test_importers/
│   ├── test_arxiv_importer.py
│   ├── test_gaia_importer.py
│   └── ...
├── test_bridge/
│   └── test_endpoints.py
└── test_integration/
    └── test_ingest_to_council.py

# Root-level files (kept in root)
start_manatuabon.ps1                 # Launcher
start_manatuabon.bat                 # Launcher (Windows)
manatuabon_observatory.html          # Dashboard UI
manatuabon_v5.html                   # Main UI
GOVERNANCE.md                        # Charter
DECISION_POLICY.md
CHANGE_POLICY.md
GOVERNANCE_RISK_REVIEW.md
MODULARIZATION.md                    # This file
REMEDIATION.md                       # AI debt tracker
README.md
SETUP.md
WALKTHROUGH.md
```

---

## Execution Strategy

### Guiding Principles

1. **Never break the running system.** Every phase must leave `start_manatuabon.ps1` functional.
2. **Backward-compatible imports.** Old import paths work via re-exports until fully migrated.
3. **One module extraction per phase.** Don't extract everything at once.
4. **Tests first.** Convert script-style tests to pytest before moving them.
5. **Verify after every extraction.** Full test suite + manual startup check.

### Phase M1: Foundation (Package Skeleton + BaseWorker)

**Risk:** Low  
**Effort:** Small  
**Dependency:** None

1. Create `manatuabon/` package with `__init__.py`
2. Create `manatuabon/workers/base_worker.py` with `BaseWorker` ABC
3. Write `tests/test_workers/test_base_worker.py`
4. Migrate `mast_worker.py` to inherit `BaseWorker` (keep original as thin wrapper)
5. Verify: `python -m pytest -v` + startup check

### Phase M2: Extract MemoryManager

**Risk:** High (most-imported class)  
**Effort:** Medium  
**Dependency:** M1

1. Move `MemoryManager` to `manatuabon/core/memory_manager.py`
2. Add re-export in `manatuabon_agent.py`: `from manatuabon.core.memory_manager import MemoryManager`
3. Verify all 22+ importers/workers still resolve
4. Extract `AgentLog`, `NemotronClient` to their own files
5. Verify: full test suite

### Phase M3: Extract Governance

**Risk:** Medium  
**Effort:** Medium  
**Dependency:** M2

1. Move `hypothesis_council.py` contents to `manatuabon/governance/`
2. Split council agents into individual files
3. Move `evidence_hunter.py` to `manatuabon/governance/`
4. Move `confidence_decay.py` and `hypothesis_revision_loop.py`
5. Verify: council tests + integration check

### Phase M4: Extract Workers

**Risk:** Low  
**Effort:** Small  
**Dependency:** M1

1. Migrate `radio_worker`, `transient_worker`, `mission_worker` to inherit `BaseWorker`
2. Move all to `manatuabon/workers/`
3. Extract simulation physics engines to `manatuabon/workers/simulation/`
4. Verify: worker tests + startup check

### Phase M5: Extract Fetchers + Importers

**Risk:** Low  
**Effort:** Medium  
**Dependency:** M2

1. Create `base_fetcher.py` with shared request + offline guard
2. Move `data_fetch_agent.py` functions to individual fetcher modules
3. Create `base_snapshot_importer.py` with shared parse + bundle logic
4. Migrate importers to inherit base classes
5. Verify: importer tests

### Phase M6: Extract Bridge + DB

**Risk:** Medium  
**Effort:** Small  
**Dependency:** M2

1. Move `db_init.py` to `manatuabon/db/schema.py`
2. Move bridge to `manatuabon/bridge/` with handlers separated from server
3. Define status constants in `manatuabon/bridge/constants.py`
4. Verify: bridge endpoint tests

### Phase M7: Test Consolidation

**Risk:** Low  
**Effort:** Medium  
**Dependency:** M1-M6

1. Create `tests/conftest.py` with shared fixtures (`tmp_db`, `mock_memory`, etc.)
2. Convert all script-style tests to pytest-discoverable `def test_*`
3. Move all tests to `tests/` subdirectory
4. Verify: `python -m pytest tests/ -v` runs everything

---

## Migration Safety Net

For each extraction, the old file becomes a thin re-export wrapper:

```python
# manatuabon_agent.py (after M2)
# DEPRECATED: import from manatuabon.core instead
from manatuabon.core.memory_manager import MemoryManager
from manatuabon.core.ingest_agent import IngestAgent
from manatuabon.core.consolidate_agent import ConsolidateAgent
from manatuabon.core.agent_log import AgentLog
from manatuabon.core.nemotron_client import NemotronClient
```

This ensures nothing breaks during migration. Wrapper files are removed only after
all consumers are updated.

---

## Success Criteria

- [ ] All code lives in `manatuabon/` package with proper `__init__.py` files
- [ ] All workers inherit from `BaseWorker`
- [ ] All tests live in `tests/` and are pytest-discoverable
- [ ] `start_manatuabon.ps1` launches successfully
- [ ] Full test suite passes
- [ ] No file in the package exceeds 500 lines
- [ ] `manatuabon_agent.py` no longer exists (or is only a re-export wrapper)
