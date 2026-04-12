# ADR-001: Modularize Manatuabon into a Python Package

## Status
Accepted

## Date
2026-04-11

## Context
Manatuabon has grown to ~25 Python modules with ~12,000 lines of code, all in
a flat root directory. Key pain points:

- `manatuabon_agent.py` is 2,685 lines containing 5 classes (MemoryManager,
  IngestAgent, ConsolidateAgent, AgentLog, NemotronClient)
- 5+ workers duplicate queue polling, retry, and status update logic
- 15+ importers duplicate parse/bundle/ingest patterns
- 36 test files live alongside source code in the root
- No package structure means no namespacing or relative imports

This creates maintenance burden, makes testing harder, and increases the risk
of circular import breakage as the codebase grows.

## Decision
Restructure Manatuabon into a proper Python package with submodules:

```
manatuabon/
├── core/           # MemoryManager, IngestAgent, ConsolidateAgent
├── governance/     # HypothesisCouncil, evidence_hunter, scoring
├── workers/        # BaseWorker + all queue workers
├── fetchers/       # External data ingestion per-archive
├── importers/      # Snapshot importers + anomaly detectors
├── db/             # Schema + seeds
├── bridge/         # HTTP API
├── monitors/       # Long-running observers
└── mcp/            # Model Context Protocol server
tests/              # All tests in dedicated directory
```

Execute incrementally over 7 phases (M1-M7) per MODULARIZATION.md.
Old files become thin re-export wrappers during migration.

## Alternatives Considered

### Keep flat structure, just split large files
- Pros: Minimal disruption
- Cons: Doesn't solve namespace pollution, test isolation, or worker duplication
- Rejected: Addresses symptoms, not root cause

### Full rewrite into a framework (FastAPI, etc.)
- Pros: Modern patterns, async, dependency injection
- Cons: Massive risk, no incremental path, rewrites fail
- Rejected: Violates incremental implementation principle; Manatuabon works today

### Monorepo with multiple packages
- Pros: Strong boundaries
- Cons: Over-engineering for a solo project, pip install complexity
- Rejected: Single package with submodules provides sufficient structure

## Consequences
- Each submodule can be tested in isolation
- `BaseWorker` eliminates duplicated queue polling across 5+ workers
- New workers/importers inherit from base classes instead of copy-pasting
- Import paths become explicit and namespaced
- Migration takes multiple sessions but each phase is independently verifiable
- `start_manatuabon.ps1` must be updated to use new entry points

## Governance Impact
None — modularization does not alter hypothesis lifecycle, confidence scoring,
or council logic. Per GOVERNANCE.md §13, structural changes that don't affect
governance can proceed with tests as the safety net.
