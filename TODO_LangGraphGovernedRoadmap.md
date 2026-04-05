# MANATUABON — Governed LangGraph Roadmap

**Owner:** Danny + Copilot  
**Created:** April 4, 2026  
**Purpose:** Translate recent RAG and agent-framework training into a concrete migration plan for Manatuabon without weakening governance, evidence policy, or auditability.

---

## Bottom Line

LangGraph should be used in Manatuabon as a **deterministic workflow layer**.

It should not be introduced as:

- an autonomous agent society
- a replacement for SQL memory or current review policy
- a shortcut around evidence gates
- a reason to hide state transitions behind framework magic

The right target is a set of explicit, replayable graphs for bounded workflows that already exist conceptually in the codebase.

---

## What This Certificate Maps To

The IBM certificate topics map cleanly onto the current repo.

### Strongest direct carryover

1. **RAG fundamentals**  
   Maps to: `MemoryManager`, SQL-backed memories, inbox ingestion, retrieval-conditioned hypothesis generation.

2. **Vector retrieval and advanced retrievers**  
   Maps to: embedding-based overlap detection, memory-link proposals, diversified evidence selection, future reranking.

3. **AI agent fundamentals**  
   Maps to: current worker/council separation, bounded review roles, tool-mediated orchestration.

4. **LangChain and LangGraph**  
   Maps to: formalizing multi-step state transitions that already exist informally in `hypothesis_council.py` and ingestion flows.

5. **Multi-agent patterns**  
   Maps to: skeptic, archivist, judge, evidence reviewer, quant reviewer, reflection.

### Skills to use carefully

1. **Autonomous collaboration**  
   Useful only when bounded by governance and explicit state checkpoints.

2. **Dynamic context switching**  
   Useful only when the switching criteria are inspectable and evidence-led.

3. **Self-improving loops**  
   Useful only if append-only, replayable, rate-limited, and human-reviewable.

---

## Non-Negotiable Constraints

Any LangGraph adoption must preserve the repo's existing governance posture from [GOVERNANCE.md](GOVERNANCE.md):

- evidence-gated decisions
- append-only audit trails
- human authority over structural state changes
- explicit override rationale
- no silent merge/refinement behavior
- no hidden online dependency assumptions
- conservative behavior under uncertainty

If a graph makes a workflow harder to audit than the current Python flow, it is the wrong graph.

---

## Current System To Preserve

These are already valuable and should become graph state, not disappear behind abstraction:

1. deterministic evidence tiering in `EvidenceReviewerAgent`
2. bounded advisory reflection
3. explicit evidence-request workflow for `held` outcomes
4. re-review only when new Tier A or Tier B evidence arrives
5. append-only `hypothesis_reviews`, `hypothesis_decisions`, `confidence_history`
6. manual review path for ambiguous evidence links
7. live governance diagnostics surfaced in `/status`

---

## Migration Principle

Do **not** rewrite Manatuabon into LangGraph all at once.

Do this instead:

1. define graph state around an already-stable workflow
2. run that graph in parallel with current logic where possible
3. compare outputs and audit traces
4. switch only after parity is demonstrated

LangGraph should formalize state transitions that are already correct, not mask workflows that are still under active policy design.

---

## Recommended Adoption Order

### Phase LG-0 — Freeze Governance Semantics First

**Status:** mostly in progress already

Before adding LangGraph runtime control, stabilize:

- decision meanings: `accepted`, `needs_revision`, `held`, `rejected`, `merged`
- evidence tier policy and caps
- merge guardrails
- evidence request generation semantics
- held re-review eligibility rules

**Exit criteria:**

- council regressions cover policy boundaries
- merge behavior does not depend on cosine similarity alone
- `held` means the same thing in UI, bridge, and persistence

### Phase LG-1 — Define Explicit Review State Schema

Introduce a serializable graph state object for one council review cycle.

Suggested state shape:

```python
{
  "hypothesis": {...},
  "existing_hypotheses": [...],
  "skeptic_review": None,
  "archivist_review": None,
  "evidence_review": None,
  "quant_review": None,
  "judge_review": None,
  "reflection_review": None,
  "final_decision": None,
  "decision_adjustments": [],
  "evidence_requests": [],
  "governance_flags": {},
  "audit_run_id": "...",
  "errors": []
}
```

**Goal:** the workflow becomes replayable without changing behavior.

### Phase LG-2 — LangGraph Council Graph

Convert the current council pipeline into a bounded graph.

Suggested node order:

1. `load_context`
2. `run_skeptic`
3. `run_archivist`
4. `run_evidence_review`
5. `run_quant_review`
6. `merge_gate`
7. `run_judge`
8. `apply_evidence_policy`
9. `run_reflection_if_needed`
10. `persist_decision`
11. `sync_evidence_requests`
12. `emit_audit_summary`

**Important:**

- `merge_gate` must require an explicit archivist duplicate verdict
- `apply_evidence_policy` must remain deterministic
- `run_reflection_if_needed` must remain advisory
- persistence must happen before downstream workflow syncing that depends on fresh status

**Code target:** new module such as `council_graph.py`, leaving `hypothesis_council.py` as orchestration shim during migration.

### Phase LG-3 — Held Re-Review Graph

Move `re_evaluate_held()` into a dedicated bounded graph.

Suggested nodes:

1. `load_held_candidates`
2. `check_material_evidence`
3. `skip_without_material_evidence`
4. `rebuild_review_payload`
5. `invoke_council_graph`
6. `record_rereview_outcome`

**Guardrails:**

- hard batch cap per cycle
- no re-review without new Tier A/B evidence
- preserve prior decision history
- surface skipped reasons in audit logs

### Phase LG-4 — Evidence Request Closure Graph

Create a small graph for converting new linked evidence into request updates.

Suggested nodes:

1. `load_pending_requests`
2. `inspect_new_linked_memories`
3. `classify_materiality`
4. `mark_request_satisfied_or_pending`
5. `flag_ready_for_rereview`

This is a strong LangGraph fit because the workflow is stateful, bounded, and benefits from explicit transitions.

### Phase LG-5 — Ingestion-To-Hypothesis Graph

After council flows are stable, formalize the ingest path.

Suggested path:

1. `detect_file`
2. `parse_payload`
3. `store_memory`
4. `propose_links`
5. `gate_ambiguous_links`
6. `generate_hypothesis_candidate`
7. `invoke_council_graph`
8. `queue_followup_tasks`

**Why this order:** you should graph the review system before graphing autonomous generation.

### Phase LG-6 — Human Checkpoint Nodes

Only after the above works should you add human interrupt points.

Candidate checkpoints:

- approve ambiguous evidence links
- approve sensitive status overrides
- review merge candidates near threshold
- reopen rejected hypotheses with new evidence

These should be implemented as explicit pause/resume states, not ad hoc UI side effects.

### Phase LG-7 — Replay, Audit, and Diff Tooling

Add operator tooling around the graphs.

Required capabilities:

- rerun a graph from stored state
- compare old council result vs graph result
- export node-by-node audit packets
- surface where policy downgraded or blocked a decision

Without this, LangGraph adds indirection but not trust.

---

## What Not To Put In LangGraph Yet

Do not move these first:

1. low-level SQL memory access
2. schema migrations
3. UI rendering behavior
4. direct worker polling loops unless their state model is already stable
5. cloud escalation policy until confidence semantics are tighter

The mistake would be turning every function call into a graph node before the policy is mature.

---

## Folder Structure I Recommend

When you start implementation, use a small explicit layout.

```text
D:\Manatuabon\
  council_graph.py
  graph_state.py
  graph_audit.py
  graph_registry.py
  tests\
    test_council_graph.py
    test_held_rereview_graph.py
    test_evidence_request_graph.py
```

Keep the graph code shallow. The point is state clarity, not framework sprawl.

---

## Acceptance Tests For Adoption

Before switching any live workflow fully to LangGraph, require:

1. decision parity on a legacy council regression set
2. identical or better audit detail than current implementation
3. no loss of evidence caps or merge guardrails
4. no hidden network assumptions
5. resumable failure behavior
6. clear operator-visible state for every paused or blocked workflow

---

## First Concrete Sprint

If you want to start now, the correct first LangGraph sprint is:

### Sprint LG-A — Council Graph Skeleton

- [ ] define serializable council graph state
- [ ] implement `load_context`, `run_skeptic`, `run_archivist`, `run_evidence_review`, `run_quant_review`
- [ ] add explicit `merge_gate` node
- [ ] keep final decision persistence in existing code for parity comparison
- [ ] add regression fixtures from known edge cases, especially `Silent Feast` vs `Silent Target`

### Sprint LG-B — Decision + Audit Parity

- [ ] add `run_judge`, `apply_evidence_policy`, `run_reflection_if_needed`
- [ ] compare graph output against existing `_finalize_review_cycle()`
- [ ] emit a graph audit packet for each run

### Sprint LG-C — Held Workflow Extraction

- [ ] move `re_evaluate_held()` into its own graph
- [ ] move evidence-request readiness updates into graph steps
- [ ] keep live batch limits and material-evidence gate unchanged

---

## Strategic Recommendation

For Manatuabon, **LangGraph is worth adopting** if you keep its role narrow:

- state machine for bounded review loops
- explicit checkpointing for governance-sensitive transitions
- replayable audit paths for hypothesis evolution

For this repo, **CrewAI, AutoGen, and BeeAI are lower priority** than LangGraph because the hard problem is not “more agents.”
The hard problem is controlled state evolution under evidence and audit constraints.

That is a LangGraph-shaped problem.
