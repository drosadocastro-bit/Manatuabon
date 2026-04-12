# ADR-003: Offline-First Architecture

## Status
Accepted

## Date
2026-04-11

## Context
Manatuabon is designed to operate in air-gapped and offline environments.
The `FORCE_OFFLINE` flag is mandatory. External API calls (arXiv, MAST, SDSS,
LIGO, Gaia) are optional enrichment, not required for core operation.

This is a foundational constraint, not a configuration option.

## Decision
Manatuabon operates offline-first with these rules:

1. **Core loop runs without network.** Ingestion, council review, confidence
   scoring, consolidation, evidence hunting (internal), and all governance
   logic work with only local data.

2. **External fetchers are opt-in.** `data_fetch_agent.py` and snapshot importers
   only activate when explicitly triggered and network is available.

3. **No implicit network dependencies.** No module may silently fall back to
   a cloud service. If a network call fails, log the failure and continue
   with local data.

4. **Local LLM is the default.** NemotronClient connects to LM Studio on
   localhost. Cloud escalation (Anthropic Claude) is explicit, logged, and
   only used by the Judge agent when local confidence is insufficient.

5. **Embedding model is local.** MiniLM-L6-v2 runs from `models/all-MiniLM-L6-v2/`
   with no download step.

6. **FORCE_OFFLINE must never be silently overridden.** If code needs to
   make a network call while FORCE_OFFLINE is set, it must refuse, not retry.

## Alternatives Considered

### Cloud-first with local cache
- Pros: Better models, always-current data
- Cons: Requires network, non-deterministic, privacy concerns
- Rejected: Violates the safety-critical, air-gapped design goal

### Hybrid with automatic fallback
- Pros: Best of both worlds
- Cons: Silent fallbacks are the exact failure mode we're preventing
- Rejected: Per GOVERNANCE.md, no hidden retries or implicit online dependencies

## Consequences
- All external data fetching is batch + manual trigger
- LLM quality is bounded by local model capability
- System remains functional during network outages
- Cloud escalation points are explicit and auditable
- New features must work offline before considering online enrichment

## Governance Impact
Core constraint. Referenced by GOVERNANCE.md §ARCHITECTURAL CONSTRAINTS and
the Copilot system prompt. Any change to offline-first behavior requires
human approval and this ADR to be superseded.
