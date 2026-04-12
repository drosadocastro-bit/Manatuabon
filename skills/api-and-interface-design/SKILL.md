---
name: api-and-interface-design
applyTo: 'manatuabon_bridge.py'
description: >
  Guides stable API and interface design. Use when designing bridge endpoints,
  module boundaries, or worker contracts. Use when creating or changing HTTP
  endpoints, defining protocols between modules, or establishing worker interfaces.
---

# API and Interface Design (Manatuabon)

## Overview

Design stable, well-documented interfaces that are hard to misuse. Good interfaces
make the right thing easy and the wrong thing hard. This applies to the bridge HTTP
API, worker contracts, module boundaries, and any surface where one piece of
Manatuabon talks to another.

## When to Use

- Designing new bridge endpoints
- Defining worker protocols (BaseWorker contract)
- Creating module boundaries during refactoring
- Changing existing public interfaces
- Adding MCP server tools

**When NOT to use:** Internal function changes within a single module.

## Core Principles

### Hyrum's Law

> With a sufficient number of users of an API, all observable behaviors become
> depended on — regardless of what you promise in the contract.

For Manatuabon: the UI depends on exact status strings (`rejected_auto`, `done`,
`held`), response shapes, and undocumented behaviors. Lesson learned from the
`rejected` vs `rejected_auto` bug.

Design implications:
- Be intentional about what you expose
- Don't leak implementation details (internal DB column names)
- Document status values as an explicit contract
- Tests guard the contract, not just the implementation

### Contract First

Define the interface before implementing it:

```python
# Worker contract — what every worker must implement
class BaseWorker(ABC):
    """Shared polling + retry + status contract for all queue workers."""

    queue_table: str          # e.g. "mast_queue"
    poll_interval: int        # seconds between polls
    max_retries: int          # 0 = no retry

    @abstractmethod
    def process(self, task: dict) -> dict:
        """Execute the task. Return result dict or raise on failure."""

    def poll(self) -> None:
        """Shared: fetch pending/retryable, mark running, call process()."""

    def mark_done(self, task_id: int, result: dict) -> None:
        """Shared: status='done', store result."""

    def mark_failed(self, task_id: int, error: str) -> None:
        """Shared: status='failed', increment attempts."""
```

### Consistent Error Semantics

Bridge endpoints must follow a single error strategy:

```python
# Every error response follows the same shape
{
    "error": "<machine-readable code>",
    "detail": "<human-readable message>"
}

# Status code mapping (bridge)
# 200 → Success
# 400 → Client sent invalid data (bad role, missing field)
# 404 → Resource not found
# 422 → Validation failed (semantically invalid)
# 500 → Server error (never expose stack traces)
```

**Don't mix patterns.** Currently some endpoints return `{"error": msg}`, others
return `{"status": "error", "detail": msg}`. Pick one and migrate.

### Validate at Boundaries

Trust internal code. Validate at system edges where external input enters:

```python
# Bridge endpoint — validate here
async def handle_post_chat(reader, writer, path, headers, body):
    role = body.get("role", "").strip()
    content = body.get("content", "").strip()
    if not role or not content:
        return json_response(writer, {"error": "role and content required"}, 400)
    # After validation, internal code trusts the data
    _memory.add_chat_message(role, content)
```

Where validation belongs:
- Bridge HTTP handlers (user/UI input)
- Worker queue processors (external API responses)
- Ingest agent (inbox file parsing)

Where validation does NOT belong:
- Between internal classes (MemoryManager ↔ ConsolidateAgent)
- In utility functions called by already-validated code

### Status Values Are a Contract

Document all status enums explicitly:

```python
# Hypothesis statuses (GOVERNANCE.md §6)
HYPOTHESIS_STATUSES = {
    "proposed", "accepted", "needs_revision",
    "held", "rejected", "merged", "rejected_auto"
}

# Queue statuses (all workers)
QUEUE_STATUSES = {"pending", "running", "done", "failed"}

# Simulation statuses (aligned with queue)
SIMULATION_STATUSES = {"pending", "running", "done", "failed"}
```

### Prefer Addition Over Modification

Extend interfaces without breaking existing consumers:

```python
# Good: add optional parameter with safe default
def get_chat_history(self, limit=50, offset=0):  # offset added later

# Bad: change existing parameter meaning
def get_chat_history(self, limit=20):  # was 50, breaks UI expectations
```

## Bridge Endpoint Naming Convention

```
GET    /hypotheses             → List hypotheses
POST   /hypotheses/reject      → Reject a hypothesis
POST   /hypotheses/status      → Update hypothesis status
GET    /memories               → Search memories
POST   /chat                   → Add chat message
GET    /chat                   → Get chat history
GET    /status                 → System status
```

## Common Rationalizations

| Rationalization | Reality |
|---|---|
| "Internal APIs don't need contracts" | Internal consumers are still consumers. The rejected/rejected_auto bug proved this. |
| "We'll document the API later" | The types and status enums ARE the documentation. Define them first. |
| "Nobody depends on that response shape" | The UI does. Hyrum's Law always applies. |
| "We can just fix the UI if we change the API" | You'll forget which UI paths depend on which response shapes. |

## Red Flags

- Endpoints returning different shapes depending on conditions
- Inconsistent error formats across endpoints
- Status strings hardcoded in multiple places without a shared constant
- Validation scattered throughout internal code instead of at boundaries
- Workers with incompatible queue schemas

## Verification

After designing an API or interface:

- [ ] Every endpoint has a documented request/response shape
- [ ] Error responses follow a single consistent format
- [ ] Status values are defined as constants, not inline strings
- [ ] Validation happens at system boundaries only
- [ ] New fields are additive and optional (backward compatible)
- [ ] Worker contracts are explicit and testable
