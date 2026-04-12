"""
Smoke-test the MCP server tools against the live database.
Skipped if the DB does not exist — use for manual integration testing only.
"""

import json
import asyncio
import os
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))

_DB_EXISTS = Path(__file__).resolve().parent.joinpath("manatuabon.db").exists()

pytestmark = pytest.mark.skipif(
    not _DB_EXISTS or os.environ.get("SKIP_INTEGRATION") == "1",
    reason="Live manatuabon.db not present or SKIP_INTEGRATION=1",
)


from mcp_server import mcp


def _result(call_result):
    content_list, meta = call_result
    return meta.get("result", content_list[0].text)


def _run(coro):
    """Run an async coroutine synchronously (avoids pytest-asyncio dependency)."""
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


def test_observatory_stats():
    r = _run(mcp.call_tool("observatory_stats", {}))
    data = json.loads(_result(r))
    assert "total_memories" in data
    assert isinstance(data["total_memories"], int)


def test_search_memories():
    r = _run(mcp.call_tool("search_memories", {"query": "pulsar", "limit": 5}))
    data = json.loads(_result(r))
    assert data["count"] >= 0
    assert isinstance(data["memories"], list)


def test_search_memories_domain_filter():
    r = _run(mcp.call_tool("search_memories", {"query": "timing", "domain": "pulsars", "limit": 3}))
    data = json.loads(_result(r))
    assert data["domain_filter"] == "pulsars"


def test_search_memories_empty_query():
    r = _run(mcp.call_tool("search_memories", {"query": "  "}))
    data = json.loads(_result(r))
    assert "error" in data


def test_get_memory():
    r = _run(mcp.call_tool("list_memories", {"limit": 1}))
    data = json.loads(_result(r))
    if data["count"] > 0:
        mid = data["memories"][0]["id"]
        r2 = _run(mcp.call_tool("get_memory", {"memory_id": mid}))
        mem = json.loads(_result(r2))
        assert mem["id"] == mid
        assert "entities" in mem


def test_get_memory_not_found():
    r = _run(mcp.call_tool("get_memory", {"memory_id": 999999}))
    data = json.loads(_result(r))
    assert "error" in data


def test_list_memories():
    r = _run(mcp.call_tool("list_memories", {"min_confidence": 0.7, "limit": 5}))
    data = json.loads(_result(r))
    assert isinstance(data["memories"], list)
    for m in data["memories"]:
        assert m["confidence"] >= 0.7


def test_list_hypotheses():
    r = _run(mcp.call_tool("list_hypotheses", {"limit": 5}))
    data = json.loads(_result(r))
    assert isinstance(data["hypotheses"], list)


def test_get_hypothesis_not_found():
    r = _run(mcp.call_tool("get_hypothesis", {"hypothesis_id": "NONEXIST-999"}))
    data = json.loads(_result(r))
    assert "error" in data


def test_list_evidence_requests():
    r = _run(mcp.call_tool("list_evidence_requests", {"status": "all", "limit": 5}))
    data = json.loads(_result(r))
    assert isinstance(data["evidence_requests"], list)


def test_list_memory_link_proposals():
    r = _run(mcp.call_tool("list_memory_link_proposals", {"status": "all", "limit": 5}))
    data = json.loads(_result(r))
    assert isinstance(data["proposals"], list)
