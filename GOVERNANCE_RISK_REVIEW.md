# MANATUABON — Governance Risk Review

**Status:** Active architecture review  
**Effective:** April 3, 2026  
**Purpose:** Identify current governance risks in the repo and the exact places where future controls matter most.

---

## 1. Executive Read

Manatuabon is already past the point where governance is optional.

It now has:

- memory persistence
- canonical hypothesis state
- confidence history
- multi-agent council review
- bounded reflection
- manual proposal review queues
- human override paths

That means the main risks are no longer about feature absence.
They are about **authority drift, hidden reinterpretation, and audit gaps**.

The repo is in a good position because the current architecture already trends conservative.
The right move is not more bureaucracy. The right move is to protect a few high-leverage boundaries.

---

## 2. Top Governance Risks

### Risk 1 — Confidence inflation

What can go wrong:

- scores begin to stand in for evidence
- status or UI presentation makes weak hypotheses feel more settled than they are

Primary hotspots:

- `record_confidence(...)` in `manatuabon_agent.py`
- `save_decision(...)` in `manatuabon_agent.py`
- scoring logic in `hypothesis_council.py`
- confidence filters and presentation in `manatuabon_v5.html`

Why it matters:

- the system already exposes confidence visually and stores it historically
- once visible, confidence can accumulate social authority inside the repo workflow

Future control worth adding:

- stronger distinction between evidence-backed confidence and workflow confidence

### Risk 2 — Council legitimacy drift

What can go wrong:

- the council is treated as a scientific authority rather than an operational review mechanism
- reflection advice starts being interpreted as a final decision layer

Primary hotspots:

- `JudgeAgent` in `hypothesis_council.py`
- `ReflectionAgent` and reflection trigger logic in `hypothesis_council.py`
- council review modal in `manatuabon_v5.html`
- `POST /api/council/override` in `manatuabon_bridge.py`

Why it matters:

- the council is structured, visible, and persistent, which makes it easy for users to over-trust its outputs

Future control worth adding:

- explicit UI distinction between operational verdict and scientific truth status

### Risk 3 — Historical bias lock-in

What can go wrong:

- earlier domains, older memories, or previous founding sets keep steering future outputs disproportionately
- anti-bias fixes degrade over time through later prompt or selection changes

Primary hotspots:

- founding-context and domain-selection logic in `manatuabon_agent.py`
- global hypothesis loading in `get_all_hypotheses(...)`
- prompt/context paths feeding proposal generation and query grounding

Why it matters:

- Manatuabon already had a real Sgr A bias problem once
- any system with memory and founding context can relapse into path dependence

Future control worth adding:

- periodic domain distribution audit across recent hypotheses and supporting memories

### Risk 4 — Silent ontology changes

What can go wrong:

- a merge, override, or status change changes the repo's practical worldview more than the code diff suggests
- human interpretation of hypothesis meaning shifts without an obvious lineage trail

Primary hotspots:

- `update_hypothesis_status(...)` in `manatuabon_agent.py`
- `save_decision(...)` in `manatuabon_agent.py`
- `POST /api/council/override` and `POST /api/council/reprocess` in `manatuabon_bridge.py`

Why it matters:

- these paths can materially change how a hypothesis is treated downstream

Future control worth adding:

- a lightweight status-change rationale field for sensitive manual changes

### Risk 5 — Ambiguous evidence promotion

What can go wrong:

- ambiguous memory links gradually become de facto evidence even if they were never strong enough
- review fatigue leads to casual approvals that later shape confidence and status

Primary hotspots:

- `generate_memory_link_proposals(...)` in `manatuabon_agent.py`
- `review_memory_link_proposal(...)` in `manatuabon_agent.py`
- `/api/memory-link-proposals/*` in `manatuabon_bridge.py`
- proposal filters, bulk actions, and note capture in `manatuabon_v5.html`

Why it matters:

- the system already uses support/challenge links in downstream confidence heuristics

Future control worth adding:

- periodic audit of approved proposal quality and note coverage

### Risk 6 — Hidden route authority

What can go wrong:

- cloud versus local path differences alter quality, provenance, or trust assumptions without being obvious
- users mistake a grounded local answer for a grounded cloud answer or vice versa

Primary hotspots:

- `GET/POST /query` in `manatuabon_bridge.py`
- `POST /cloud/query` in `manatuabon_bridge.py`
- route/source display in `manatuabon_v5.html`

Why it matters:

- route changes can influence evidence quality and confidence while appearing superficially similar in the UI

Future control worth adding:

- stricter route provenance labeling in chat history and review traces

### Risk 7 — Operational fragility masking governance risk

What can go wrong:

- direct startup failures make it harder to trust whether the intended runtime path is actually active
- a governance assumption may be invalid if the wrong startup path is being used in practice

Primary hotspots:

- direct `manatuabon_agent.py` startup path
- worker startup scripts
- runtime process management around the bridge on port 7777

Why it matters:

- governance depends on the actual runtime respecting the intended control boundaries

Future control worth adding:

- a startup diagnostics note or health surface that states which governance-sensitive services are active

---

## 3. Areas Already In Good Shape

The repo already has several governance strengths.

- append-oriented decision and review persistence
- explicit manual review path for ambiguous memory links
- visible council audit modal
- confidence history rather than a single hidden score
- evidence-led context selection rather than hardcoded Sgr A padding
- bounded reflection instead of autonomous self-revision

These are not small wins. They meaningfully reduce governance risk already.

---

## 4. Highest-Leverage Future Controls

If you later want to add more governance without overbuilding, these are the best candidates.

1. **Status rationale for sensitive overrides**  
   Best place: manual override and status mutation paths.

2. **Domain breadth audit snapshot**  
   Best place: canonical hypothesis inspector or a small bridge endpoint.

3. **Confidence provenance split**  
   Best place: confidence history and UI display.

4. **Approved-link quality audit**  
   Best place: proposal review analytics.

5. **Startup governance diagnostics**  
   Best place: `/status` payload and startup scripts.

---

## 5. Recommended Priority Order

If you ever operationalize more governance, do it in this order:

1. protect status and override meaning
2. protect confidence interpretation
3. protect domain breadth and anti-bias behavior
4. protect proposal-review quality
5. improve runtime diagnostics

That order gives the most governance value with the least disruption.

---

## 6. Bottom Line

Manatuabon does not currently need heavy governance machinery.

It does need disciplined protection around:

- who gets to change state
- what counts as support
- how confidence is interpreted
- how historical meaning is preserved
- how domain bias is kept from creeping back in

The current architecture is already compatible with that approach.
The main job now is to keep future changes from crossing those boundaries silently.
