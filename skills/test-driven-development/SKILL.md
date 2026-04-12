---
name: test-driven-development
applyTo: '**/*.py'
description: >
  Drives development with tests. Use when implementing any logic, fixing any bug,
  or changing any behavior. Use when you need to prove that code works.
---

# Test-Driven Development (Manatuabon)

## Overview

Write a failing test before writing the code that makes it pass. For bug fixes,
reproduce the bug with a test before attempting a fix. Tests are proof —
"seems right" is not done.

Manatuabon-specific: all tests MUST be pytest-discoverable (`def test_*`).
Script-style `check()` functions are legacy debt and must not be created.

## When to Use

- Implementing any new logic or behavior
- Fixing any bug (the Prove-It Pattern)
- Modifying existing functionality
- Adding edge case handling
- Any change that could break existing behavior

**When NOT to use:** Pure configuration changes, documentation updates, or
static content changes with no behavioral impact.

## The TDD Cycle

```
    RED                GREEN              REFACTOR
 Write a test    Write minimal code   Clean up the
 that fails  ──→  to make it pass  ──→  implementation  ──→ (repeat)
      │                  │                    │
      ▼                  ▼                    ▼
   Test FAILS        Test PASSES         Tests still PASS
```

### Step 1: RED — Write a Failing Test

Write the test first. It must fail. A test that passes immediately proves nothing.

```python
def test_confidence_decay_reduces_stale_hypothesis(tmp_path):
    """Hypothesis with no evidence for 30 days loses confidence."""
    db = setup_test_db(tmp_path)
    insert_hypothesis(db, id="H99", confidence=0.8, last_evidence_days_ago=30)

    decay_stale_hypotheses(db)

    result = get_hypothesis(db, "H99")
    assert result["confidence"] < 0.8
```

### Step 2: GREEN — Make It Pass

Write the minimum code to make the test pass. Don't over-engineer.

### Step 3: REFACTOR — Clean Up

With tests green, improve the code without changing behavior. Run tests after
every refactor step.

## The Prove-It Pattern (Bug Fixes)

```
Bug report arrives
       │
       ▼
  Write a test that demonstrates the bug
       │
       ▼
  Test FAILS (confirming the bug exists)
       │
       ▼
  Implement the fix
       │
       ▼
  Test PASSES (proving the fix works)
       │
       ▼
  Run full test suite (no regressions)
```

## The Test Pyramid (Manatuabon)

```
          ╱╲
         ╱  ╲         Integration (~10%)
        ╱    ╲        Bridge endpoints + DB round-trips
       ╱──────╲
      ╱        ╲      Component (~20%)
     ╱          ╲     Worker dispatch, council flow, evidence linking
    ╱────────────╲
   ╱              ╲   Unit (~70%)
  ╱                ╲  Physics engines, parsers, validators, scoring
 ╱──────────────────╲
```

## Manatuabon Test Rules

1. **All tests are pytest-discoverable** — `def test_*` only. No `check()` scripts.
2. **Use `tmp_path` for database isolation** — never test against the live `manatuabon.db`.
3. **Prefer real implementations over mocks** — use in-memory SQLite, not mock objects.
4. **Encode UTF-8 explicitly** — always `open(path, encoding="utf-8")` on Windows.
5. **One assertion per concept** — each test verifies one behavior.
6. **DAMP over DRY** — each test reads as a self-contained specification.
7. **Arrange-Act-Assert pattern** — setup, execute, verify.

## Test Naming Convention

```python
# Good: reads like a specification
def test_council_rejects_hypothesis_with_no_evidence():
def test_bridge_returns_422_for_invalid_chat_role():
def test_simulation_worker_marks_failed_after_3_retries():

# Bad: vague
def test_council():
def test_bridge_works():
def test_simulation():
```

## Running Tests

```powershell
# Full suite
python -m pytest -v --tb=short

# Specific module
python -m pytest test_council_evidence_policy.py -v

# With coverage (when available)
python -m pytest --cov=. --cov-report=term-missing
```

## Common Rationalizations

| Rationalization | Reality |
|---|---|
| "I'll write tests after the code works" | You won't. Tests written after the fact test implementation, not behavior. |
| "This is too simple to test" | Simple code gets complicated. The test documents expected behavior. |
| "I tested it manually" | Manual testing doesn't persist. Tomorrow's change can break it. |
| "It's just a prototype" | Manatuabon treats everything as production-grade. |
| "The script-style tests work fine" | They're invisible to pytest and CI. They provide zero regression protection. |

## Red Flags

- Writing code without any corresponding tests
- Tests that pass on the first run (they may not be testing what you think)
- Bug fixes without reproduction tests
- `def check()` functions instead of `def test_*()`
- Missing `encoding="utf-8"` in file opens
- Tests that depend on the live database

## Verification

After completing any implementation:

- [ ] Every new behavior has a corresponding `def test_*` function
- [ ] All tests pass: `python -m pytest -v --tb=short`
- [ ] Bug fixes include a reproduction test that failed before the fix
- [ ] Test names describe the behavior being verified
- [ ] No tests were skipped or disabled
