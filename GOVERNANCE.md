# MANATUABON — Governance Charter

**Status:** Active repo guidance  
**Effective:** April 3, 2026  
**Owner:** Danny + Copilot  
**Scope:** Research governance for the Manatuabon workspace, including memory, hypotheses, council review, confidence scoring, lineage, reflection, and human override.

---

## 1. Purpose

Manatuabon is a human-on-the-loop scientific research system.

Its purpose is to:

- propose, compare, and refine scientific hypotheses
- preserve historical context and audit trails
- surface uncertainty instead of hiding it
- support human scientific judgment rather than replace it

Manatuabon is **not** an autonomous scientific authority.

---

## 2. Operating Posture

Manatuabon is governed as a **research system**, not a product platform and not an autonomous agent society.

The governing posture for this repo is:

- **Evidence-gated**: stronger claims require stronger provenance
- **Audit-preserving**: important decisions must remain reconstructible
- **Human-supervised**: the human owner remains final authority on structural changes
- **Reversible where practical**: status changes, links, and thresholds should be recoverable or reviewable
- **Conservative under uncertainty**: unresolved hypotheses remain unresolved rather than being promoted by fluency

---

## 3. Core Principles

1. **Evidence over fluency**  
   A well-worded output is not evidence.

2. **Proposal over fact**  
   Hypotheses are working scientific proposals, not settled truth.

3. **Audit over convenience**  
   Traceable reasoning is preferred over silent automation.

4. **Lineage over overwrite**  
   Refinement and merge operations must preserve ancestry.

5. **Human authority over agent consensus**  
   Agent agreement does not supersede human review for structural governance.

6. **Breadth over local bias**  
   The system must avoid collapsing into one favored target, theme, or domain without evidence.

---

## 4. Governance Boundaries

### 4.1 Human Authority

The human owner has final authority over:

- model routing policy
- confidence thresholds and scoring weights
- ontology changes and status semantics
- merge/refinement policy
- prompt and governance policy changes
- release decisions
- final override of hypothesis state

### 4.2 System Authority

The system may:

- ingest observations and preserve them as memories
- propose hypotheses
- score and route hypotheses through council review
- suggest merges, revisions, and evidence requests
- surface ambiguity for manual review
- maintain append-only decision history

The system may not, by default:

- silently redefine scientific truth
- silently erase prior state
- auto-promote ambiguous evidence links into strong support
- let reflection mutate final council decisions on its own

---

## 5. Roles And Responsibilities

### 5.1 Memory Layer

The memory layer records observations, summaries, and inferred links.

- High-confidence direct links may be written automatically
- Ambiguous links must remain proposals for manual review
- Contradictory or weak evidence should remain visible rather than discarded

### 5.2 Council

The council is a structured review mechanism, not a truth oracle.

- **Skeptic** challenges falsifiability and coherence
- **Archivist** checks redundancy and overlap
- **Judge** issues operational status routing
- **Reflection** advises on unresolved cases only

### 5.3 Reflection

Reflection is explicitly bounded.

- It is advisory
- It runs only on unresolved outcomes such as `held` or borderline `needs_revision`
- It may propose blockers, revisions, and evidence requests
- It does not overwrite the Judge decision by itself

### 5.4 Confidence Engine

Confidence is an operational signal, not a truth score.

- Confidence must remain decomposable
- Missing evidence should lower confidence rather than be filled with narrative certainty
- Confidence display should remain tied to sources and score components where available

---

## 6. Hypothesis State Policy

Hypothesis statuses have governance meaning.

- `proposed`: generated but not yet fully adjudicated
- `accepted`: operationally useful and sufficiently supported for active consideration, not proven true
- `needs_revision`: promising but structurally incomplete
- `held`: unresolved due to insufficient evidence, insufficient testability, or unresolved conflict
- `rejected`: currently not viable under available review and evidence
- `merged`: historically retained but superseded through explicit merge lineage
- `rejected_auto`: failed hard quality floors before full review

Status changes should be interpreted as workflow decisions, not declarations of scientific truth.

---

## 7. Evidence Policy

Evidence should be treated in tiers.

### Tier A — Direct support

- source-backed observations
- instrument outputs
- concrete literature-backed findings
- high-confidence memory links

### Tier B — Weak support

- indirect correlation
- partial contextual similarity
- plausible but incomplete retrieval support

### Tier C — Speculative synthesis

- analogy
- extrapolation
- narrative reasoning without direct corroboration

Governance rule:

- Tier C may inspire a hypothesis
- Tier C alone should not justify strong acceptance

---

## 8. Protected Operations

The following operations are governance-sensitive and should remain explicit, logged, and reviewable:

- changing scoring thresholds or weights
- changing status semantics
- changing prompt roles for the council
- changing cloud escalation policy
- mass backfill or reprocessing of hypotheses
- automated merge behavior
- schema changes that affect lineage, confidence, or audit tables
- domain-selection logic that changes what context is injected into generation

For this repo, any of the above should be accompanied by:

- a short rationale
- a test or validation step where feasible
- preserved audit context rather than destructive replacement

---

## 9. Bias And Domain Breadth Policy

Manatuabon must remain universe-wide unless the evidence for a given task is explicitly narrow.

Governance rules:

- do not hardcode one target domain as the default worldview
- do not inject unrelated founding context just to pad prompts
- prefer evidence-led domain selection
- preserve diversity of founding hypotheses across domains
- treat historical concentration in one topic as a potential bias signal, not as proof of importance

---

## 10. Auditability Requirements

The following should remain reconstructible from the repo state and runtime data where possible:

- what hypothesis was reviewed
- which agents reviewed it
- what decision was produced
- what score breakdown was used
- what evidence and context influenced the result
- whether reflection was triggered
- whether a human overrode any state

Append-only history is preferred for decisions, confidence changes, and review records.

---

## 11. Runtime Safety Rules

To avoid brittle or deceptive behavior, Manatuabon should operate under these rules:

- no silent deletion of historical hypotheses because a newer one is preferred
- no silent replacement of ambiguous links with asserted support
- no unlogged confidence inflation
- no hidden shift from local reasoning to cloud reasoning without surfacing that route
- no assumption that top retrieval or top similarity equals correctness

---

## 12. Human Review Triggers

Human review should be preferred when:

- a merge would change meaning, not just wording
- a hypothesis touches multiple domains with weak evidence
- contradiction and support are both substantial
- reflection repeatedly marks the same blocker without resolution
- confidence and decision appear materially inconsistent
- historical backfill would rewrite interpretation at scale

---

## 13. Change Management For This Repo

Balanced governance means not over-administering a personal research system.

Therefore this repo adopts a lightweight change policy:

- Routine UI and ergonomics work can proceed without formal governance review
- Structural changes to hypothesis lifecycle, scoring, lineage, or authority boundaries should reference this charter
- Governance should guide architecture, not freeze experimentation
- Where tradeoffs exist, preserve clarity and reversibility over maximum automation

---

## 14. Governance Application To Current Manatuabon

These are the recommended active defaults for the current repo.

1. The council remains the main operational review boundary.
2. Reflection remains advisory only.
3. Ambiguous memory-to-hypothesis links remain in proposal review, not auto-linked.
4. Confidence remains visible as a grounded operational signal, not as proof.
5. Canonical hypothesis lineage remains preserved rather than overwritten.
6. Domain context injection remains evidence-led and breadth-aware.
7. Human override remains available for meaningful state corrections.

These defaults align with the current architecture and do not require a framework rewrite.

---

## 15. Future Governance Artifacts

Companion documents for this charter now exist:

1. [DECISION_POLICY.md](DECISION_POLICY.md)
2. [CHANGE_POLICY.md](CHANGE_POLICY.md)
3. [GOVERNANCE_RISK_REVIEW.md](GOVERNANCE_RISK_REVIEW.md)

If the project grows further, the next governance additions should be:

1. a small rationale capture path for sensitive manual overrides
2. a domain breadth audit snapshot
3. a lightweight governance diagnostics surface in runtime status

---

## 16. Final Rule

Manatuabon may propose, rank, compare, and reflect.

It may not quietly promote its own outputs into truth without evidence, audit trail, and human authority.