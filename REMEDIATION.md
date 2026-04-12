# Manatuabon — AI Debt Remediation Plan

**Status:** Active  
**Date:** April 11, 2026  
**Owner:** Danny + Copilot  
**Review Cadence:** Monthly sweep

---

## What Is AI Debt?

AI debt is technical debt created or amplified by AI-assisted development:
- Code generated faster than it can be reviewed
- Patterns duplicated across files without shared abstractions
- Decisions made in-context but never recorded
- Tests that look complete but miss real coverage
- Architecture that grew without a plan

Manatuabon is a personal research system treated as production-grade.
This plan tracks known debt and prioritizes remediation.

---

## Debt Inventory

### D1: Test Architecture Debt

**Severity:** HIGH  
**Status:** Open  
**Found:** April 11, 2026

| Issue | Detail |
|---|---|
| 11 of 36 test files use script-style `check()` | Invisible to pytest and CI |
| No shared test fixtures | Database setup duplicated across 22+ test files |
| No integration tests | Only unit/component — no end-to-end ingest→council flow |
| Encoding bugs on Windows | `open()` without `encoding="utf-8"` fails on CP1252 |

**Remediation:**

- [ ] R1.1: Convert all `check()` scripts to `def test_*()` (Phase M7)
- [ ] R1.2: Create `tests/conftest.py` with `tmp_db`, `mock_memory` fixtures
- [ ] R1.3: Add one integration test: ingest bundle → council review → decision
- [ ] R1.4: Audit all `open()` calls for explicit `encoding="utf-8"`
- [ ] R1.5: Add pytest to pre-commit hook

**Verification:** `python -m pytest -v` discovers and runs all tests.

---

### D2: Schema Drift Debt

**Severity:** HIGH  
**Status:** Partially Remediated  
**Found:** April 10, 2026 | Fixed: radio_queue + simulations aligned

| Issue | Detail |
|---|---|
| Workers built independently | Each worker assumed its own schema conventions |
| Status inconsistency | `completed` vs `done` across tables |
| Missing columns | `radio_queue` lacked `attempts`/`last_run` |
| No schema contract | Column names/types not enforced by a shared definition |

**Remediation (done):**

- [x] R2.1: Add `attempts`/`last_run` to radio_queue and simulations
- [x] R2.2: Normalize status values to `done` everywhere
- [x] R2.3: Add migration logic in `db_init.py`

**Remediation (remaining):**

- [ ] R2.4: Define schema contract as Python constants (table names, column names, status enums)
- [ ] R2.5: Schema validation test — assert all queue tables have `status`, `attempts`, `last_run`
- [ ] R2.6: Document table schemas in ADR-002

---

### D3: Decision Amnesia

**Severity:** MEDIUM  
**Status:** Open  
**Found:** April 11, 2026

| Issue | Detail |
|---|---|
| No ADRs | Zero architecture decision records |
| No record of "why" | Why exponential confidence decay? Why 3 council reviewers? Why FORCE_OFFLINE? |
| Re-debating decisions | Same questions arise across sessions |

**Remediation:**

- [ ] R3.1: Create `docs/decisions/` with initial ADRs (see Seed ADRs below)
- [ ] R3.2: ADR-001: Modularization decision
- [ ] R3.3: ADR-002: Queue schema contract
- [ ] R3.4: ADR-003: Offline-first architecture
- [ ] R3.5: ADR-004: Council reviewer count and composition
- [ ] R3.6: ADR-005: Confidence decay model (exponential + floor)
- [ ] R3.7: Going forward, write an ADR for any non-obvious decision

---

### D4: Worker Inconsistency

**Severity:** MEDIUM  
**Status:** Open  
**Found:** April 11, 2026

| Issue | Detail |
|---|---|
| 5+ workers with no shared base class | Duplicated polling, retry, status update logic |
| Different retry strategies | mast=3, radio=1, simulation=0, transient=N/A |
| Different logging formats | Inconsistent structured logging |
| Different poll intervals | Hardcoded magic numbers |

**Remediation:**

- [ ] R4.1: Create `BaseWorker` ABC with shared poll/retry/status contract
- [ ] R4.2: Migrate mast_worker as first adopter
- [ ] R4.3: Migrate remaining workers incrementally
- [ ] R4.4: Test base class independently
- [ ] R4.5: Document worker contract in `api-and-interface-design` skill

---

### D5: Monolith Debt

**Severity:** MEDIUM  
**Status:** Open  
**Found:** April 11, 2026

| Issue | Detail |
|---|---|
| `manatuabon_agent.py` = 2,685 lines | 5 classes, God-file |
| All files in root directory | No package structure |
| Circular dependency risk | Agent imports bridge, bridge imports agent |
| 15+ importers duplicating patterns | No base class or shared utilities |

**Remediation:**

- [ ] R5.1: Execute MODULARIZATION.md phases M1-M7
- [ ] R5.2: No root-level .py file exceeds 500 lines after split
- [ ] R5.3: Backward-compatible re-export wrappers during migration

---

### D6: Prompt/Instruction Drift

**Severity:** LOW  
**Status:** Open  
**Found:** April 11, 2026

| Issue | Detail |
|---|---|
| Copilot instructions are static | Don't reflect new constraints (chat validation, queue alignment) |
| No per-module instructions | No `.instructions.md` for specific areas |
| No skills until today | Agent had no structured workflows |

**Remediation:**

- [x] R6.1: Create 5 adapted agent skills in `skills/`
- [ ] R6.2: Update Copilot instructions to reference skills and current architecture
- [ ] R6.3: Review instructions quarterly or after major phases

---

### D7: Error Response Inconsistency

**Severity:** LOW  
**Status:** Open  
**Found:** April 11, 2026

| Issue | Detail |
|---|---|
| Bridge uses mixed error shapes | Some return `{"error": msg}`, others `{"status": "error"}` |
| No documented API contract | UI and MCP depend on undocumented response shapes |
| No HTTP status code consistency | Some errors return 200 with error body |

**Remediation:**

- [ ] R7.1: Define standard error response shape: `{"error": "<code>", "detail": "<msg>"}`
- [ ] R7.2: Audit all bridge handlers for consistent error returns
- [ ] R7.3: Document all endpoints in ADR or dedicated API doc

---

### D8: Importer Duplication

**Severity:** LOW  
**Status:** Open  
**Found:** April 11, 2026

| Issue | Detail |
|---|---|
| 15+ importers, ~400 lines each | All repeat: parse, validate, build bundle, ingest |
| Copy-paste from templates | Bug fixes must be applied to each copy separately |
| 3 anomaly detectors near-identical | gaia_panstarrs, gaia_sdss, gaia_ztf |

**Remediation:**

- [ ] R8.1: Create `base_snapshot_importer.py` with shared parse/bundle/ingest
- [ ] R8.2: Create `base_anomaly_detector.py` with shared cross-match/flag/score
- [ ] R8.3: Migrate importers incrementally (one per session)

---

## Priority Matrix

```
         HIGH SEVERITY              LOW SEVERITY
    ┌─────────────────────┬─────────────────────┐
    │                     │                     │
H   │  D1: Test Arch      │  D6: Prompt Drift   │
I   │  D2: Schema Drift   │                     │
G   │                     │                     │
H   ├─────────────────────┼─────────────────────┤
    │                     │                     │
L   │  D3: Decision Amnesia│  D7: Error Shapes   │
O   │  D4: Worker Incon.  │  D8: Importer Dup.  │
W   │  D5: Monolith       │                     │
    │                     │                     │
    └─────────────────────┴─────────────────────┘
         HIGH IMPACT              LOW IMPACT
```

## Recommended Execution Order

| Order | Debt | Why First |
|---|---|---|
| 1 | D1 (Test Architecture) | Need reliable tests before any refactoring |
| 2 | D3 (Decision Amnesia) | Seed ADRs — low effort, high future value |
| 3 | D4 (Worker Inconsistency) | BaseWorker unblocks modularization Phase M1 |
| 4 | D5 (Monolith) | MODULARIZATION.md phases M1-M7 |
| 5 | D2 (Schema Drift) | Schema contract + validation test |
| 6 | D8 (Importer Duplication) | Base classes during modularization Phase M5 |
| 7 | D7 (Error Response) | Standardize during bridge extraction Phase M6 |
| 8 | D6 (Prompt Drift) | Ongoing — review after each major phase |

---

## Monthly Sweep Checklist

Every ~30 days or after a major phase:

- [ ] Run full test suite — any new failures?
- [ ] Check for new script-style test files (should be zero)
- [ ] Review `schema.py` — any undocumented migrations?
- [ ] Check for files > 500 lines in the package
- [ ] Any decisions made without ADRs?
- [ ] Any new workers not inheriting BaseWorker?
- [ ] Update this remediation plan with new findings

---

## Governance Note

This remediation plan operates under GOVERNANCE.md §13 (Change Management):

> Routine UI and ergonomics work can proceed without formal governance review.
> Structural changes to hypothesis lifecycle, scoring, lineage, or authority
> boundaries should reference this charter.

Modularization is a structural change but does not alter governance logic.
It should proceed with tests as the safety net, not governance review gates.
