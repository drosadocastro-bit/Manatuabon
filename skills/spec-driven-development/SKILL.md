---
name: spec-driven-development
applyTo: '**'
description: >
  Creates specs before coding. Use when starting a new phase, feature, worker,
  or significant change and no specification exists yet. Use when requirements
  are unclear or ambiguous.
---

# Spec-Driven Development (Manatuabon)

## Overview

Write a structured specification before writing any code. The spec is the shared
source of truth between human and Copilot — it defines what we're building, why,
and how we'll know it's done. Code without a spec is guessing.

Manatuabon-specific: specs must also declare **domain boundaries**, **confidence
impact**, and **governance implications** for any change touching hypotheses,
evidence, or council logic.

## When to Use

- Starting a new phase or major feature
- Adding a new worker, importer, or data pipeline
- Changing hypothesis lifecycle, scoring, or governance
- Adding or modifying bridge endpoints
- Any change touching more than 3 files
- Refactoring modules or extracting packages

**When NOT to use:** Single-file bug fixes, typo corrections, config tweaks.

## The Gated Workflow

```
SPECIFY ──→ PLAN ──→ TASKS ──→ IMPLEMENT
   │          │        │          │
   ▼          ▼        ▼          ▼
 Human      Human    Human      Human
 reviews    reviews  reviews    reviews
```

Do not advance to the next phase until the current one is validated.

### Phase 1: Specify

Surface assumptions immediately:

```
ASSUMPTIONS I'M MAKING:
1. This operates offline-first (FORCE_OFFLINE)
2. SQLite is the only persistence layer
3. No new external dependencies without approval
4. Append-only for audit tables
→ Correct me now or I'll proceed with these.
```

Write a spec covering these areas:

1. **Objective** — What are we building and why? What does success look like?
2. **Domain Impact** — Which domains (sgra, pulsars, exoplanets, etc.) are affected?
3. **Governance Impact** — Does this touch hypothesis lifecycle, confidence, or council?
4. **Commands** — Build, test, run commands
5. **Files Touched** — Which modules will change
6. **Testing Strategy** — What tests, what coverage
7. **Boundaries**
   - **Always do:** Run tests, follow naming, validate at boundaries
   - **Ask first:** Schema changes, new dependencies, governance changes
   - **Never do:** Delete audit history, bypass FORCE_OFFLINE, auto-promote hypotheses

### Phase 2: Plan

Generate a technical implementation plan:
1. Identify components and dependencies
2. Determine implementation order
3. Note risks and mitigation
4. Define verification checkpoints

### Phase 3: Tasks

Break into discrete tasks:
```markdown
- [ ] Task: [Description]
  - Acceptance: [What must be true when done]
  - Verify: [pytest command or manual check]
  - Files: [Which files will be touched]
```

### Phase 4: Implement

Execute tasks one at a time following `incremental-implementation` and
`test-driven-development` skills. Each task: implement → test → verify → commit.

## Common Rationalizations

| Rationalization | Reality |
|---|---|
| "This is simple, I don't need a spec" | Simple tasks don't need long specs, but they still need acceptance criteria. |
| "I'll write the spec after I code it" | That's documentation, not specification. The spec's value is forcing clarity before code. |
| "The spec will slow us down" | A 15-minute spec prevents hours of rework. |
| "It's just a prototype" | Manatuabon treats everything as production-grade. Prototypes become permanent. |

## Red Flags

- Starting to write code without any written requirements
- Making architectural decisions without documenting them
- Skipping the spec because "it's obvious what to build"
- Changes to governance logic without referencing GOVERNANCE.md

## Verification

Before proceeding to implementation, confirm:

- [ ] The spec covers objective, domain impact, governance impact
- [ ] The human has reviewed and approved the spec
- [ ] Success criteria are specific and testable
- [ ] Boundaries (Always/Ask First/Never) are defined
- [ ] The spec is committed to the repository
