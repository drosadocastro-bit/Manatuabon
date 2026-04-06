"""Smoke-test the MCP server tools against the live database."""

import json
import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from mcp_server import mcp


def _result(call_result):
    """Extract result text from mcp.call_tool return value."""
    content_list, meta = call_result
    return meta.get("result", content_list[0].text)


async def main():
    passed = 0
    failed = 0

    # ── 1. observatory_stats ──
    r = await mcp.call_tool("observatory_stats", {})
    data = json.loads(_result(r))
    assert "total_memories" in data, "observatory_stats missing total_memories"
    assert "total_hypotheses" in data, "observatory_stats missing total_hypotheses"
    assert isinstance(data["total_memories"], int)
    print(f"  observatory_stats: {data['total_memories']} memories, "
          f"{data['total_hypotheses']} hypotheses, "
          f"{data['pending_link_proposals']} pending proposals")
    passed += 1

    # ── 2. search_memories ──
    r = await mcp.call_tool("search_memories", {"query": "pulsar", "limit": 5})
    data = json.loads(_result(r))
    assert data["count"] >= 0, "search_memories returned negative count"
    assert isinstance(data["memories"], list)
    if data["count"] > 0:
        m = data["memories"][0]
        assert "id" in m and "summary" in m and "confidence" in m
        assert 0.0 <= m["confidence"] <= 1.0
        print(f"  search_memories(pulsar): {data['count']} hits, "
              f"top=#{m['id']} ({m['confidence_label']})")
    else:
        print("  search_memories(pulsar): 0 hits (empty DB)")
    passed += 1

    # ── 3. search_memories with domain filter ──
    r = await mcp.call_tool("search_memories", {"query": "timing", "domain": "pulsars", "limit": 3})
    data = json.loads(_result(r))
    assert data["domain_filter"] == "pulsars"
    print(f"  search_memories(timing, domain=pulsars): {data['count']} hits")
    passed += 1

    # ── 4. search_memories empty query ──
    r = await mcp.call_tool("search_memories", {"query": "  "})
    data = json.loads(_result(r))
    assert "error" in data, "Empty query should return error"
    print(f"  search_memories(empty): correctly returned error")
    passed += 1

    # ── 5. get_memory ──
    # Find an ID first
    r = await mcp.call_tool("list_memories", {"limit": 1})
    data = json.loads(_result(r))
    if data["count"] > 0:
        mid = data["memories"][0]["id"]
        r = await mcp.call_tool("get_memory", {"memory_id": mid})
        mem = json.loads(_result(r))
        assert mem["id"] == mid, f"get_memory returned wrong id"
        assert "entities" in mem and isinstance(mem["entities"], list)
        print(f"  get_memory(#{mid}): OK — {mem['confidence_label']} confidence")
        passed += 1
    else:
        print("  get_memory: SKIP (no memories in DB)")
        passed += 1

    # ── 6. get_memory not found ──
    r = await mcp.call_tool("get_memory", {"memory_id": 999999})
    data = json.loads(_result(r))
    assert "error" in data
    print(f"  get_memory(999999): correctly returned error")
    passed += 1

    # ── 7. list_memories ──
    r = await mcp.call_tool("list_memories", {"min_confidence": 0.7, "limit": 5})
    data = json.loads(_result(r))
    assert isinstance(data["memories"], list)
    for m in data["memories"]:
        assert m["confidence"] >= 0.7, f"Memory #{m['id']} below threshold"
    print(f"  list_memories(min_confidence=0.7): {data['count']} hits")
    passed += 1

    # ── 8. list_hypotheses ──
    r = await mcp.call_tool("list_hypotheses", {"limit": 5})
    data = json.loads(_result(r))
    assert isinstance(data["hypotheses"], list)
    if data["count"] > 0:
        h = data["hypotheses"][0]
        assert "id" in h and "title" in h and "confidence" in h
        print(f"  list_hypotheses: {data['count']} results, "
              f"top={h['id']} status={h['status']}")
    else:
        print(f"  list_hypotheses: 0 results (empty DB)")
    passed += 1

    # ── 9. get_hypothesis ──
    if data["count"] > 0:
        hid = data["hypotheses"][0]["id"]
        r = await mcp.call_tool("get_hypothesis", {"hypothesis_id": hid})
        hyp = json.loads(_result(r))
        assert hyp["id"] == hid
        assert "evidence" in hyp and isinstance(hyp["evidence"], list)
        print(f"  get_hypothesis({hid}): OK — {hyp['status']}, "
              f"{len(hyp['evidence'])} evidence items")
        passed += 1
    else:
        print("  get_hypothesis: SKIP (no hypotheses)")
        passed += 1

    # ── 10. get_hypothesis not found ──
    r = await mcp.call_tool("get_hypothesis", {"hypothesis_id": "NONEXIST-999"})
    data = json.loads(_result(r))
    assert "error" in data
    print(f"  get_hypothesis(NONEXIST-999): correctly returned error")
    passed += 1

    # ── 11. get_council_decision ──
    r = await mcp.call_tool("list_hypotheses", {"limit": 1})
    hyps = json.loads(_result(r))
    if hyps["count"] > 0:
        hid = hyps["hypotheses"][0]["id"]
        r = await mcp.call_tool("get_council_decision", {"hypothesis_id": hid})
        dec = json.loads(_result(r))
        assert "has_decision" in dec
        if dec["has_decision"]:
            assert "decision" in dec and "final_score" in dec
            print(f"  get_council_decision({hid}): {dec['decision']} "
                  f"({round(dec['final_score']*100)}%)")
        else:
            print(f"  get_council_decision({hid}): no decision yet")
        passed += 1
    else:
        print("  get_council_decision: SKIP")
        passed += 1

    # ── 12. get_council_reviews ──
    if hyps["count"] > 0:
        hid = hyps["hypotheses"][0]["id"]
        r = await mcp.call_tool("get_council_reviews", {"hypothesis_id": hid})
        revs = json.loads(_result(r))
        assert isinstance(revs["reviews"], list)
        print(f"  get_council_reviews({hid}): {revs['count']} reviews")
        passed += 1
    else:
        print("  get_council_reviews: SKIP")
        passed += 1

    # ── 13. list_evidence_requests ──
    r = await mcp.call_tool("list_evidence_requests", {"status": "all", "limit": 5})
    data = json.loads(_result(r))
    assert isinstance(data["evidence_requests"], list)
    print(f"  list_evidence_requests(all): {data['count']} requests")
    passed += 1

    # ── 14. list_memory_link_proposals ──
    r = await mcp.call_tool("list_memory_link_proposals", {"status": "all", "limit": 5})
    data = json.loads(_result(r))
    assert isinstance(data["proposals"], list)
    print(f"  list_memory_link_proposals(all): {data['count']} proposals")
    passed += 1

    # ── Summary ──
    total = passed + failed
    print(f"\n{'='*50}")
    print(f"  {passed}/{total} passed, {failed} failed")
    if failed:
        sys.exit(1)


if __name__ == "__main__":
    print("Running MCP server smoke tests...\n")
    asyncio.run(main())
    print("\nAll MCP smoke tests passed.")
