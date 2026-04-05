# MANATUABON — Decision Policy

**Status:** Active companion to [GOVERNANCE.md](GOVERNANCE.md)  
**Effective:** April 3, 2026  
**Purpose:** Define what hypothesis states mean operationally inside Manatuabon.

---

## 1. Why This Exists

Manatuabon uses workflow states such as `accepted`, `held`, and `needs_revision`.

These states should not drift into vague labels.

This policy defines:

- what each state means
- what minimum evidence posture it implies
- what transitions are normal
- what transitions require higher care or human review

---

## 2. General Interpretation Rule

All Manatuabon statuses are **operational research states**, not declarations of scientific truth.

Short version:

- `accepted` means strong enough to actively work with
- `needs_revision` means promising but incomplete
- `held` means unresolved
- `rejected` means not currently viable
- `merged` means historically preserved but superseded

---

## 3. Status Definitions

### 3.1 `proposed`

Meaning:

- A hypothesis has been generated or inserted into the system
- It has not yet completed the full intended review lifecycle

Expected characteristics:

- may have initial confidence
- may have sparse evidence
- may still be awaiting council context

Normal next states:

- `accepted`
- `needs_revision`
- `held`
- `rejected`
- `rejected_auto`

### 3.2 `accepted`

Meaning:

- The hypothesis is operationally useful and sufficiently supported for active consideration
- It is fit to stay in the active hypothesis landscape

It does **not** mean:

- proven true
- final
- immune from future downgrade or merge

Expected characteristics:

- evidence is materially stronger than speculation
- testability is present
- coherence is not in obvious conflict with review findings
- redundancy is not severe

Human interpretation:

- treat as an active research candidate worth building on

### 3.3 `needs_revision`

Meaning:

- The core idea has potential, but the current form is not strong enough
- The system can state what should improve

Expected characteristics:

- some evidence or novelty exists
- one or more dimensions remain materially weak
- concrete revisions should be possible

Normal next states:

- `accepted`
- `held`
- `rejected`
- refined child hypothesis in future lineage work

Human interpretation:

- salvageable, but not ready

### 3.4 `held`

Meaning:

- The hypothesis remains unresolved because the evidence, testability, or conflict picture is too weak or too mixed

Expected characteristics:

- insufficient evidence density
- unresolved contradiction or ambiguity
- unclear path to immediate promotion

Human interpretation:

- neither endorse nor discard yet
- hold until stronger evidence or better framing appears

Governance note:

- `held` is not a soft rejection
- `held` preserves uncertainty explicitly

### 3.5 `rejected`

Meaning:

- Under current evidence and review, the hypothesis is not viable enough to keep active

Expected characteristics:

- low support
- poor coherence or testability
- severe mismatch with evidence or duplication logic

Human interpretation:

- not currently worth active pursuit
- historical record remains valuable

Governance note:

- rejection must preserve audit context
- rejection is reversible if future evidence changes the picture

### 3.6 `merged`

Meaning:

- The hypothesis is sufficiently overlapped by another hypothesis that it should no longer stand as an independent active node

Expected characteristics:

- explicit merge target
- preserved historical trace
- no silent deletion of the merged record

Human interpretation:

- the idea survives as ancestry, not as a separate live branch

### 3.7 `rejected_auto`

Meaning:

- The hypothesis failed hard quality floors before full review

Typical reasons:

- no falsifiable prediction
- confidence below minimum floor
- claim too short or structurally empty

Governance note:

- this is a quality gate, not a scientific verdict

---

## 4. Evidence Expectations By State

These are policy expectations, not rigid mathematics.

### `accepted`

- should have clear support stronger than pure synthesis
- should have meaningful testability
- should not rely only on Tier C speculative synthesis

### `needs_revision`

- can have partial evidence
- may still be structurally weak
- should have at least a plausible path to improvement

### `held`

- can contain mixed support and contradiction
- may be blocked by missing evidence rather than bad logic alone
- should remain queryable and auditable

### `rejected`

- evidence and review posture currently do not justify continued active status

---

## 5. Preferred Transitions

Normal transitions:

- `proposed` -> `accepted`
- `proposed` -> `needs_revision`
- `proposed` -> `held`
- `proposed` -> `rejected`
- `needs_revision` -> `accepted`
- `needs_revision` -> `held`
- `held` -> `needs_revision`
- `held` -> `accepted`
- `held` -> `rejected`
- `accepted` -> `merged`

Transitions that should be treated carefully:

- `accepted` -> `rejected`
- `accepted` -> `held`
- mass status changes across many hypotheses
- any change caused by backfill or threshold shifts rather than new evidence

---

## 6. Human Review Triggers For Decisions

Human review is preferred when:

- an `accepted` hypothesis would be downgraded
- a merge would materially change meaning
- status and confidence appear inconsistent
- a hypothesis spans multiple domains with thin evidence
- repeated reflection cycles keep surfacing the same blockers

---

## 7. Current Repo Application

This policy applies directly to the current architecture:

- council decisions persisted in `hypothesis_decisions`
- review trail persisted in `hypothesis_reviews`
- confidence updates persisted in `confidence_history`
- direct status mutation path in `MemoryManager.update_hypothesis_status(...)`
- manual override endpoint in `POST /api/council/override`

The practical implication is simple:

- state changes should be explainable
- state changes should not silently destroy prior interpretation

---

## 8. Final Rule

When in doubt:

- prefer `held` over false certainty
- prefer `needs_revision` over premature acceptance
- prefer preserved lineage over overwrite
