# MANATUABON — Master TODO
## Full Hypothesis Evolution System

**Owner:** Danny (Bayamon, PR) + Copilot
**Created:** April 3, 2026
**Purpose:** Upgrade Manatuabon from hypothesis generator to auditable, self-evolving scientific hypothesis system.

---

## Core Mission
Transform Manatuabon from generator -> evolutionary scientific intelligence.

That requires four properties:

1. Stable hypothesis identity
2. Retroactive scoring across all hypotheses
3. Full auditability of every decision and mutation
4. Safe autonomous loops that never destroy historical context

---

## Reality Check

Parts of this already exist in the codebase:

- `hypothesis_reviews` table exists
- `hypothesis_decisions` table exists
- `confidence_history` table exists
- auto-generated hypotheses are already exposed through the bridge/UI
- council review flow already exists for new hypotheses

So the real work is not “start from zero”.
The real work is to unify the existing pieces into one coherent lifecycle.

---

## Design Rules

- Never overwrite prior hypothesis state without preserving lineage
- Treat accepted/rejected/held/needs_revision as evolutionary states, not terminal deletion events
- Confidence must be decomposable, reproducible, and backfillable
- Every merge/refine/review event must be recoverable from the audit trail
- UI must expose why a hypothesis is alive, downgraded, merged, or archived

## Governance Baseline

This roadmap now operates under [GOVERNANCE.md](GOVERNANCE.md).

Interpretation rules for future phases:

- council and reflection are review mechanisms, not truth authorities
- reflection remains advisory unless explicitly redesigned later
- ambiguous evidence links should stay reviewable rather than being silently promoted
- lineage, auditability, and breadth control outrank convenience automation

---

## PHASE 0 — Canonical Schema First
**Status:** Critical prerequisite

Before global loading or retroactive scoring, define the canonical model.

- [ ] Create canonical hypothesis schema
- [ ] Normalize fields across founding + auto-generated hypotheses
- [ ] Add explicit lineage fields where missing
- [ ] Add stable origin typing
- [ ] Add merge/refinement markers without destroying existing IDs

### Canonical Hypothesis Record

```json
{
  "id": "AUTO-128",
  "text": "Hypothesis body",
  "title": "Optional display title",
  "status": "accepted|rejected|needs_revision|held|merged|archived|proposed",
  "confidence": 0.0,
  "confidence_components": {
    "evidence_score": 0.0,
    "testability_score": 0.0,
    "coherence_score": 0.0,
    "novelty_score": 0.0
  },
  "origin": "founding|agent_auto|refined|merged|manual",
  "parent_id": null,
  "root_id": "AUTO-128",
  "merged_into": null,
  "created_at": "ISO timestamp",
  "updated_at": "ISO timestamp"
}
```

### Deliverables

- [ ] DB migration plan
- [ ] Canonical Python serializer/deserializer
- [ ] Bridge response normalization
- [ ] UI-safe schema contract

### Success Criteria

- every hypothesis, regardless of source, can be rendered through one schema
- no downstream engine needs source-specific branching for base fields

---

## PHASE 1 — Global Hypothesis Loader
**Depends on:** Phase 0

- [ ] Load ALL hypotheses from DB
- [ ] Include accepted, rejected, needs_revision, held, merged, archived, proposed
- [ ] Normalize to canonical schema on load
- [ ] Expose one internal loader used by bridge, council, and refinement engine

### Required Outputs

- [ ] `get_all_hypotheses(normalized=True)` in memory manager
- [ ] optional filters: `status`, `origin`, `root_id`, `active_only`
- [ ] reusable bridge endpoint for full hypothesis state

### Risk to Avoid

- Do not special-case “founding” and “auto” hypotheses forever. That split is legacy baggage.

---

## PHASE 2 — Unified Confidence Engine
**Depends on:** Phase 0, Phase 1

- [ ] Compute confidence for ALL hypotheses retroactively
- [ ] Define weighted scoring model
- [ ] Persist confidence component breakdown
- [ ] Backfill missing historical confidence rows

### Proposed Components

- [ ] `evidence_score`
- [ ] `testability_score`
- [ ] `coherence_score`
- [ ] `novelty_score`

### Weighted Formula

```text
final_confidence =
  0.35 * evidence_score +
  0.25 * testability_score +
  0.25 * coherence_score +
  0.15 * novelty_score
```

### Persistence

- [ ] persist final weighted confidence to canonical record
- [ ] persist component breakdown to `confidence_history` or companion table
- [ ] mark score source: `retroactive_engine`, `council`, `refinement_pass`, `merge_engine`

### Success Criteria

- every hypothesis has a reproducible current confidence
- historical confidence changes remain append-only

---

## PHASE 3 — Council Reprocessing
**Depends on:** Phase 1, Phase 2

- [ ] Run Skeptic / Archivist / Judge on ALL hypotheses
- [ ] Reprocess legacy hypotheses that predate council activation
- [ ] Store full reasoning logs and per-agent scoring
- [ ] Preserve prior decisions instead of overwriting them silently

### Required Outputs

- [ ] per-agent verdicts
- [ ] per-agent score contributions
- [ ] final decision snapshot
- [ ] traceable timestamped reprocessing run ID

### Safety Rule

- Reprocessing must be idempotent and resumable.
- A failed batch must not leave hypotheses in half-updated state.

---

## PHASE 4 — Auto-Refinement Engine
**Depends on:** Phase 0, Phase 1, Phase 2, Phase 3

- [ ] Refine hypotheses where `status == needs_revision`
- [ ] Produce child hypothesis instead of mutating original in place
- [ ] Preserve parent -> child lineage
- [ ] Carry forward confidence deltas and rationale

### Required Behavior

- [ ] original hypothesis remains immutable except status update
- [ ] refined child gets new ID
- [ ] child stores `parent_id`
- [ ] lineage root remains stable

### Success Criteria

- every refinement is traceable as evolution, not replacement

---

## PHASE 5 — Hypothesis Lineage System
**Depends on:** Phase 4

- [ ] Build full evolution tree
- [ ] Track parent/child versioning
- [ ] Track mutation reason
- [ ] Track merge ancestry separately from refinement ancestry

### Required Views

- [ ] root hypothesis
- [ ] descendants
- [ ] merge targets
- [ ] current champion version

### Data Rules

- refinement lineage != merge lineage
- a merged hypothesis should remain queryable historically

---

## PHASE 6 — Duplicate + Merge Intelligence
**Depends on:** Phase 1, Phase 5

- [ ] Use MiniLM embeddings across normalized hypothesis text
- [ ] Flag `merge_candidate` when cosine similarity > 0.80
- [ ] Require semantic guardrails before auto-merge
- [ ] Preserve both original records and create merge metadata

### Merge Guardrails

- [ ] similarity alone is not enough
- [ ] require compatible status or explicit judge approval
- [ ] require no strong contradiction in rationale

### Output

- [ ] `merged_into`
- [ ] merge reasoning
- [ ] merge timestamp
- [ ] source hypotheses retained in history

---

## PHASE 7 — Survival System
**Depends on:** Phase 2, Phase 3, Phase 6

- [ ] Rank by confidence, evidence density, novelty, and recency
- [ ] Define ACTIVE / HELD / ARCHIVED tiers
- [ ] Separate “interesting but weak” from “contradicted”

### Suggested Tier Logic

- [ ] `ACTIVE`: high confidence or high strategic value
- [ ] `HELD`: unresolved, not dead
- [ ] `ARCHIVED`: low strategic value or superseded
- [ ] `REJECTED`: contradicted or structurally invalid

### Success Criteria

- the system knows what to spend compute on next

---

## PHASE 8 — Observational Binding
**Depends on:** Phase 2, Phase 7

- [ ] Link hypotheses to JWST, ALMA, Gaia, SETI, LIGO, SDSS, transients
- [ ] Track support vs contradiction from observations
- [ ] Attach evidence references to hypothesis records

### Required Outputs

- [ ] supporting memory IDs
- [ ] contradicting memory IDs
- [ ] last observation touchpoint
- [ ] evidence delta after each ingest

### Strategic Goal

- move from “text hypothesis” to “observationally bound research object”

---

## PHASE 9 — UI Evolution
**Depends on:** Phase 5, Phase 7, Phase 8

- [ ] Evolution tree view
- [ ] Confidence heatmap
- [ ] Timeline view
- [ ] Filters for viable hypotheses only
- [ ] Show lineage, merges, and contradictions in one place

### UI Must Answer

- why is this hypothesis alive?
- what evidence supports it?
- what contradicted it?
- what version replaced it?
- what should be tested next?

---

## PHASE 10 — Audit + Transparency
**Depends on:** All prior phases

- [ ] Log why accepted/rejected/held/merged/refined
- [ ] Per-hypothesis audit trail
- [ ] Exportable review packet

### Required Audit Objects

- [ ] scoring snapshots
- [ ] council reasoning snapshots
- [ ] merge/refinement decisions
- [ ] observational supports/contradictions

---

## PHASE 11 — Autonomous Evolution Loop
**Depends on:** Phase 2 through 10

Generate -> Evaluate -> Reject -> Refine -> Evolve -> Re-test -> Repeat

- [ ] bounded batch processing
- [ ] no destructive autonomous rewrites
- [ ] human-review checkpoints for merge/refine thresholds
- [ ] resumable loop state

### Guardrails

- [ ] max hypotheses processed per cycle
- [ ] max refinements per root hypothesis per cycle
- [ ] no auto-merge without audit record
- [ ] no permanent archival without recoverable state

---

## Implementation Order I Recommend

Do **not** implement in the original order.
Use this order instead:

1. Phase 0 — Canonical Schema First
2. Phase 1 — Global Hypothesis Loader
3. Phase 2 — Unified Confidence Engine
4. Phase 3 — Council Reprocessing
5. Phase 10 — Audit + Transparency
6. Phase 4 — Auto-Refinement Engine
7. Phase 5 — Hypothesis Lineage System
8. Phase 6 — Duplicate + Merge Intelligence
9. Phase 7 — Survival System
10. Phase 8 — Observational Binding
11. Phase 9 — UI Evolution
12. Phase 11 — Autonomous Evolution Loop

Reason:

- schema before scoring
- scoring before reprocessing
- audit before autonomous mutation
- lineage before merge
- observation binding before UI glamor

---

## Immediate Next Sprint

If we start now, the correct first sprint is:

### Sprint A — Foundation
- [ ] add canonical schema adapter
- [ ] implement full hypothesis loader
- [ ] add migration plan for lineage fields
- [ ] create confidence component calculator stub
- [ ] backfill current confidence for all hypotheses without council reruns yet

### Sprint B — Retroactive Intelligence
- [ ] council reprocess legacy hypotheses
- [ ] store per-agent score traces
- [ ] add decisions history endpoint for all normalized hypotheses

### Sprint C — Evolution Mechanics
- [ ] refinement child creation
- [ ] lineage graph building
- [ ] merge candidate detection

---

## LangGraph Transition

The concrete LangGraph migration plan now lives in [TODO_LangGraphGovernedRoadmap.md](TODO_LangGraphGovernedRoadmap.md).

Use it as a control-flow roadmap, not as a reason to relax governance.

Priority interpretation:

1. keep current evidence and merge policy semantics stable
2. graph the council review cycle first
3. graph held re-review and evidence-request closure second
4. graph autonomous ingest/generation only after council parity is proven

---

## End Goal

Build a self-evolving scientific intelligence system that:

- generates hypotheses
- scores them consistently
- reviews them adversarially
- refines them without losing ancestry
- binds them to observations
- explains every decision it makes

This is the right direction.
The only thing I changed was making it buildable.