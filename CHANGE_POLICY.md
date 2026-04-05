# MANATUABON — Change Policy

**Status:** Active companion to [GOVERNANCE.md](GOVERNANCE.md)  
**Effective:** April 3, 2026  
**Purpose:** Define how sensitive changes should be made without over-administering the repo.

---

## 1. Why This Exists

Manatuabon is still a personal research system, so governance must stay lightweight.

At the same time, some changes alter the system's authority, auditability, or scientific behavior more than they appear to.

This policy separates:

- routine iteration
- sensitive architecture changes

---

## 2. Change Classes

### Class A — Routine changes

Examples:

- UI ergonomics
- modal layouts
- filter persistence
- non-structural logging improvements
- display-only analytics

Expectation:

- can move fast
- should still avoid unrelated regressions

### Class B — Behavioral changes

Examples:

- confidence heuristics
- prompt wording adjustments
- domain-selection heuristics
- ranking logic for proposal review
- routing logic between local and cloud

Expectation:

- should include rationale
- should include targeted validation
- should preserve current auditability

### Class C — Structural governance changes

Examples:

- status semantics
- council role changes
- reflection authority changes
- merge logic changes
- lineage schema changes
- automated reprocessing or backfill behavior
- override authority changes

Expectation:

- should reference [GOVERNANCE.md](GOVERNANCE.md)
- should reference [DECISION_POLICY.md](DECISION_POLICY.md) if statuses are affected
- should include explicit validation steps
- should preserve or improve audit history

---

## 3. Protected Change Areas In This Repo

The following areas are considered sensitive by default.

### 3.1 Hypothesis lifecycle

Hotspots:

- `MemoryManager.update_hypothesis_status(...)` in `manatuabon_agent.py`
- `MemoryManager.save_decision(...)` in `manatuabon_agent.py`
- `HypothesisCouncil._finalize_review_cycle(...)` in `hypothesis_council.py`

Risk:

- status meaning can drift faster than the UI and audit trail reveal

### 3.2 Confidence behavior

Hotspots:

- `record_confidence(...)` in `manatuabon_agent.py`
- council score computation in `hypothesis_council.py`
- confidence display and filters in `manatuabon_v5.html`

Risk:

- confidence inflation or misleading presentation

### 3.3 Memory-to-hypothesis linking

Hotspots:

- `generate_memory_link_proposals(...)` in `manatuabon_agent.py`
- `review_memory_link_proposal(...)` in `manatuabon_agent.py`
- `/api/memory-link-proposals/*` in `manatuabon_bridge.py`
- proposal triage UI in `manatuabon_v5.html`

Risk:

- ambiguous evidence becoming implicit support without sufficient scrutiny

### 3.4 Council authority boundaries

Hotspots:

- `JudgeAgent`
- `ReflectionAgent`
- reflection trigger logic in `hypothesis_council.py`
- `POST /api/council/override` in `manatuabon_bridge.py`

Risk:

- advisory mechanisms becoming de facto autonomous decision makers

### 3.5 Domain breadth and prompt context

Hotspots:

- hypothesis selection and founding-context logic in `manatuabon_agent.py`
- query routing in `manatuabon_bridge.py`

Risk:

- reintroducing hidden domain bias or Sgr A-centric drift

---

## 4. Minimum Requirements For Sensitive Changes

For Class B and Class C changes, include:

1. a short rationale  
2. focused validation or tests  
3. no silent removal of audit context  
4. no unrelated schema or behavior drift  
5. a note if the change affects governance boundaries

---

## 5. Repo-Specific Validation Expectations

Use the smallest useful validation set.

Examples:

- editor error checks for touched files
- targeted runtime API checks against `http://127.0.0.1:7777`
- deterministic Python regression scripts where possible
- DB round-trip validation for persistence changes

Avoid heavy process for simple UI polish.

---

## 6. Change Rules By Area

### Prompts

- prompt changes that affect verdict semantics should be treated as Class C
- prompt changes that only improve phrasing without changing authority are Class B

### Thresholds and weights

- any change to confidence or decision thresholds should record rationale
- threshold changes should be considered capable of reinterpreting historical hypotheses

### Backfills and reprocessing

- should be resumable where practical
- should not silently rewrite historical meaning without preserved trace

### Overrides

- human override remains allowed
- override pathways should stay explicit rather than hidden inside normal agent flows

### Local versus cloud routing

- route changes should not hide when cloud reasoning is being used
- route changes that alter groundedness or provenance expectations are Class B or Class C depending on scope

---

## 7. Current Default Governance-Safe Stance

Until explicitly changed later:

- reflection remains advisory only
- manual review remains the path for ambiguous evidence links
- council remains the main operational adjudication layer
- confidence remains a grounded signal, not a truth claim
- lineage and audit preservation outrank convenience mutation

---

## 8. Final Rule

If a change would alter who gets to decide, what counts as evidence, or how historical meaning is preserved, treat it as a governance-sensitive change even if the code diff looks small.
