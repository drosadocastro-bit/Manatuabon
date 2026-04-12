---
name: documentation-and-adrs
applyTo: '**'
description: >
  Records decisions and documentation. Use when making architectural decisions,
  changing governance, shipping features, or when you need to record context that
  future sessions will need. Use when the answer to "why did we do it this way?"
  is not obvious from the code.
---

# Documentation and ADRs (Manatuabon)

## Overview

Document decisions, not just code. The most valuable documentation captures the
*why* — the context, constraints, and trade-offs that led to a decision. Code
shows what was built; documentation explains why it was built this way and what
alternatives were considered.

Manatuabon-specific: this is a long-lived personal research system. Without ADRs,
past decisions become invisible — and get re-debated or accidentally reversed.

## When to Use

- Making a significant architectural decision
- Choosing between competing approaches
- Adding or changing a bridge endpoint or worker pattern
- Changing governance logic (confidence, council, status semantics)
- When you find yourself explaining the same thing repeatedly
- After discovering a non-obvious bug (document the root cause)

**When NOT to use:** Obvious code, typo fixes, routine commits.

## Architecture Decision Records (ADRs)

ADRs capture the reasoning behind significant technical decisions. They're the
highest-value documentation you can write.

### When to Write an ADR

- Choosing a library or major dependency
- Designing a data model or schema change
- Selecting a worker pattern or retry strategy
- Choosing between offline-first approaches
- Any decision that would be expensive to reverse
- Non-obvious bug fixes (why the bug existed, why this fix)

### ADR Template

Store ADRs in `docs/decisions/` with sequential numbering:

```markdown
# ADR-NNN: [Decision Title]

## Status
Accepted | Superseded by ADR-XXX | Deprecated

## Date
YYYY-MM-DD

## Context
[What situation triggered this decision? What constraints exist?]

## Decision
[What did we decide to do and why?]

## Alternatives Considered

### [Alternative 1]
- Pros: ...
- Cons: ...
- Rejected because: ...

### [Alternative 2]
- Pros: ...
- Cons: ...
- Rejected because: ...

## Consequences
[What are the implications? What becomes easier/harder?]

## Governance Impact
[Does this affect GOVERNANCE.md? Which sections?]
```

### ADR Lifecycle

```
PROPOSED → ACCEPTED → (SUPERSEDED or DEPRECATED)
```

- **Don't delete old ADRs.** They capture historical context.
- When a decision changes, write a new ADR that references the old one.

## Inline Documentation

### When to Comment

Comment the *why*, not the *what*:

```python
# BAD: restates the code
# Set confidence to 0.3
hypothesis["confidence"] = 0.3

# GOOD: explains non-obvious intent
# Confidence floor for held hypotheses — prevents decay from pushing
# below review threshold, which would make them invisible to the
# evidence hunter (see ADR-004)
hypothesis["confidence"] = max(hypothesis["confidence"], 0.3)
```

### Document Known Gotchas

```python
# IMPORTANT: Windows CP1252 will fail on astrophysics Unicode (☉, →, ⁻¹).
# Always specify encoding='utf-8' when reading any JSON bundle file.
with open(bundle_path, encoding="utf-8") as f:
    data = json.load(f)
```

## What NOT to Document

- Self-explanatory code
- TODO comments for things you should just do now
- Commented-out code (delete it, git has history)
- "What" comments that restate the code

## Manatuabon Documentation Hierarchy

| Document | Purpose | Update Frequency |
|---|---|---|
| `GOVERNANCE.md` | Scientific governance charter | Rarely — structural changes only |
| `DECISION_POLICY.md` | Council decision rules | When evidence policy changes |
| `CHANGE_POLICY.md` | Change management rules | When governance scope changes |
| `docs/decisions/ADR-NNN.md` | Architectural decisions | Per decision |
| `README.md` | Setup + overview | Per major phase |
| `REMEDIATION.md` | AI debt backlog | Per sweep |
| `skills/*.md` | Agent skill definitions | When workflows change |
| Inline `# why` comments | Non-obvious code context | As needed |

## Common Rationalizations

| Rationalization | Reality |
|---|---|
| "The code is self-documenting" | Code shows what. It doesn't show why, what alternatives were rejected, or what constraints apply. |
| "We'll write docs when the API stabilizes" | APIs stabilize faster when you document them. |
| "Nobody reads docs" | Future-you does. Future-Copilot does. Six months from now you'll wish this existed. |
| "ADRs are overhead" | A 10-minute ADR prevents a 2-hour debate about the same decision later. |
| "It's a personal project" | Personal projects have the worst documentation because there's no handoff forcing function. ADRs ARE the forcing function. |

## Red Flags

- Architectural decisions with no written rationale
- "Why did we do it this way?" with no answer anywhere
- The same decision being re-debated across sessions
- Bridge endpoints with no documented request/response shapes
- GOVERNANCE.md referenced but never linked to specific ADRs

## Verification

After documenting:

- [ ] ADRs exist for all significant architectural decisions
- [ ] README covers quick start, commands, and architecture overview
- [ ] Known gotchas are documented inline where they matter
- [ ] No commented-out code remains
- [ ] Copilot instructions reference current project structure
