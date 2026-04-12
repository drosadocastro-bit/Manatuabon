---
name: incremental-implementation
applyTo: '**'
description: >
  Delivers changes incrementally. Use when implementing any feature or change
  that touches more than one file. Use when a task feels too big to land in one step.
---

# Incremental Implementation (Manatuabon)

## Overview

Build in thin vertical slices — implement one piece, test it, verify it, then
expand. Each increment should leave the system in a working, testable state.
This is the execution discipline that makes large features manageable and
prevents the "500 changed lines, something broke, good luck" problem.

## When to Use

- Implementing any multi-file change
- Building a new worker, importer, or feature
- Refactoring or extracting modules
- Any time you're tempted to write more than ~100 lines before testing

**When NOT to use:** Single-file, single-function changes where scope is minimal.

## The Increment Cycle

```
┌──────────────────────────────────────┐
│                                      │
│   Implement ──→ Test ──→ Verify ──┐  │
│       ▲                           │  │
│       └───── Commit ◄─────────────┘  │
│              │                       │
│              ▼                       │
│          Next slice                  │
│                                      │
└──────────────────────────────────────┘
```

For each slice:

1. **Implement** the smallest complete piece of functionality
2. **Test** — run the test suite (`python -m pytest -v --tb=short`)
3. **Verify** — confirm the slice works (tests pass, no new errors)
4. **Commit** — save progress with a descriptive message
5. **Move to the next slice**

## Slicing Strategies

### Vertical Slices (Preferred)

Build one complete path through the stack:

```
Slice 1: Extract BaseWorker class + test
    → Tests pass, base class exists and is importable

Slice 2: Migrate mast_worker to inherit BaseWorker
    → Tests pass, mast_worker uses shared polling logic

Slice 3: Migrate radio_worker to inherit BaseWorker
    → Tests pass, radio_worker uses shared polling logic

Slice 4: Migrate remaining workers
    → Tests pass, all workers use consistent pattern
```

### Risk-First Slicing

Tackle the riskiest or most uncertain piece first:

```
Slice 1: Extract MemoryManager from manatuabon_agent.py + verify imports
    → If this breaks circular dependencies, find out now

Slice 2: Update all internal imports to use new location
    → Tests pass with new module structure

Slice 3: Extract remaining classes
    → Lower risk since pattern is proven
```

## Implementation Rules

### Rule 0: Simplicity First

Before writing any code, ask: "What is the simplest thing that could work?"

```
SIMPLICITY CHECK:
✗ Generic WorkerFactory with plugin system for 5 workers
✓ Simple BaseWorker class with poll() override

✗ Abstract importer framework with configurable pipeline stages
✓ Shared utility functions that importers call directly

✗ Event-driven architecture for queue status changes
✓ Direct SQLite UPDATE in the worker loop
```

### Rule 1: Scope Discipline

Touch only what the task requires.

Do NOT:
- "Clean up" code adjacent to your change
- Refactor imports in files you're not modifying
- Add features not in the spec
- Remove comments you don't fully understand

If you notice something worth improving:
```
NOTICED BUT NOT TOUCHING:
- data_fetch_agent.py has duplicated error handling (separate task)
- extinction_lookup.py could use caching (not in scope)
→ Want me to add these to the remediation backlog?
```

### Rule 2: Keep It Runnable

After each increment, the full test suite must pass and `start_manatuabon.ps1`
must still launch without errors. Don't leave the system in a broken state.

### Rule 3: One Thing at a Time

Each increment changes one logical thing. Don't mix:
- Feature implementation with refactoring
- Bug fixes with improvements
- Schema changes with code changes

### Rule 4: Safe Defaults

New code defaults to safe, conservative behavior consistent with FORCE_OFFLINE
and the governance charter.

### Rule 5: Rollback-Friendly

Each increment should be independently revertable:
- Additive changes (new files, new functions) are easy to revert
- Database migrations must have corresponding rollback logic
- Avoid deleting and replacing in the same commit

## Commit Sizing

Target ~100 lines per commit. If a commit is larger:
- Can it be split into two independent changes?
- Are you mixing concerns?

## Common Rationalizations

| Rationalization | Reality |
|---|---|
| "I'll test it all at the end" | Bugs compound. A bug in Slice 1 makes Slices 2-5 wrong. Test each slice. |
| "It's faster to do it all at once" | It feels faster until something breaks and you can't find which of 500 changed lines caused it. |
| "These changes are too small to commit separately" | Small commits are free. Large commits hide bugs. |
| "This refactor is small enough to include" | Refactors mixed with features make both harder to review. Separate them. |

## Red Flags

- More than 100 lines written without running tests
- Multiple unrelated changes in a single increment
- "Let me just quickly add this too" scope expansion
- Skipping the test/verify step to move faster
- Large uncommitted changes accumulating
- Building abstractions before the third use case

## Verification

After completing all increments for a task:

- [ ] Each increment was individually tested and committed
- [ ] The full test suite passes
- [ ] `start_manatuabon.ps1` launches without errors
- [ ] No uncommitted changes remain
