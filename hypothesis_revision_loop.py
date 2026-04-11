"""
MANATUABON HYPOTHESIS REVISION LOOP
=====================================
Closes the missing arc in the hypothesis lifecycle:

    needs_revision  ──[new evidence + reflection guidance]──▶  re-review
                                                                   │
                               ┌───────────────────────────────────┤
                               ▼                                   ▼
                           accepted                         held / rejected

Without this loop, hypotheses stuck in `needs_revision` stay there forever.
The Jailer Hypothesis (H3) was the first victim — this worker fixes it.

Architecture:
  1. Poll DB for `needs_revision` hypotheses (cooldown elapsed, cycle < MAX)
  2. Gather new evidence: memories tagged to hypothesis + simulation bundles from inbox
  3. Extract reflection guidance: concrete_revisions + blockers from hypothesis_reviews
  4. Patch hypothesis evidence string with new material  (audit trail in-place)
  5. POST to bridge /api/council/reprocess { hyp_id, force: true }
  6. Record revision cycle in revision_tracking table

The bridge hands off to council.review_existing(hyp_id, force=True) which
runs the full CouncilGraph pipeline fresh on the updated hypothesis.

Usage:
  python hypothesis_revision_loop.py               # continuous polling (120s)
  python hypothesis_revision_loop.py --once        # run once and exit
  python hypothesis_revision_loop.py --hypothesis H3 --dry-run
  python hypothesis_revision_loop.py --interval 300
"""

import argparse
import json
import logging
import sqlite3
import time
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path

log = logging.getLogger("manatuabon.revision_loop")

# ── Configuration ──────────────────────────────────────────────────────────────

DB_PATH            = Path(__file__).resolve().parent / "manatuabon.db"
INBOX_PATH         = Path(__file__).resolve().parent / "inbox"
BRIDGE_URL         = "http://127.0.0.1:7777/api/council/reprocess"

MAX_REVISION_CYCLES = 3      # give up after this many re-review attempts
COOLDOWN_MINUTES    = 30     # minimum gap between re-review attempts per hypothesis
POLL_INTERVAL_SEC   = 120    # default polling interval
BRIDGE_TIMEOUT_SEC  = 30     # HTTP timeout when calling the bridge


# ── Database Helpers ───────────────────────────────────────────────────────────

def open_db(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path), timeout=10)
    conn.row_factory = sqlite3.Row
    return conn


def ensure_tracking_table(conn: sqlite3.Connection):
    """Create the revision_tracking table if it doesn't exist yet."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS revision_tracking (
            hypothesis_id   TEXT PRIMARY KEY,
            revision_cycle  INTEGER NOT NULL DEFAULT 0,
            last_attempted  TEXT,
            last_status     TEXT,
            notes           TEXT
        )
    """)
    conn.commit()


def get_needs_revision_candidates(conn: sqlite3.Connection) -> list[dict]:
    """
    Return hypotheses that are stuck in `needs_revision` and eligible for re-review.

    Eligibility criteria:
      - Most recent decision in hypothesis_decisions is 'needs_revision'
      - revision_cycle < MAX_REVISION_CYCLES
      - Last re-review attempt is older than COOLDOWN_MINUTES ago
    """
    cutoff = (
        datetime.now(timezone.utc) - timedelta(minutes=COOLDOWN_MINUTES)
    ).isoformat()

    rows = conn.execute("""
        SELECT
            h.id,
            h.title,
            h.description,
            h.evidence,
            h.confidence,
            d.timestamp         AS last_decision_at,
            d.reasoning         AS last_reasoning,
            COALESCE(rt.revision_cycle, 0)                      AS revision_cycle,
            COALESCE(rt.last_attempted, '1970-01-01T00:00:00')  AS last_attempted
        FROM hypotheses h
        JOIN hypothesis_decisions d
            ON d.hypothesis_id = h.id
            AND d.id = (
                SELECT MAX(id) FROM hypothesis_decisions
                WHERE hypothesis_id = h.id
            )
        LEFT JOIN revision_tracking rt ON rt.hypothesis_id = h.id
        WHERE d.decision = 'needs_revision'
          AND COALESCE(rt.revision_cycle, 0) < ?
          AND COALESCE(rt.last_attempted, '1970-01-01T00:00:00') < ?
          AND d.timestamp < ?
    """, (MAX_REVISION_CYCLES, cutoff, cutoff)).fetchall()

    return [dict(r) for r in rows]


def get_new_memories(conn: sqlite3.Connection, hypothesis_id: str, since: str | None) -> list[dict]:
    """
    Fetch memories that support this hypothesis added after `since` timestamp.
    Ordered by significance (highest first), capped at 10.
    """
    since = since or "1970-01-01T00:00:00"
    rows = conn.execute("""
        SELECT id, timestamp, content, concept_tags, significance, domain_tags
        FROM memories
        WHERE (
            supports_hypothesis = ?
            OR supports_hypothesis LIKE ?
        )
        AND timestamp > ?
        ORDER BY significance DESC, timestamp DESC
        LIMIT 10
    """, (hypothesis_id, f"%{hypothesis_id}%", since)).fetchall()
    return [dict(r) for r in rows]


def get_reflection_guidance(conn: sqlite3.Connection, hypothesis_id: str) -> dict:
    """
    Retrieve the most recent reflection agent review for this hypothesis.
    Returns parsed concrete_revisions, blockers, and evidence_requests.
    """
    row = conn.execute("""
        SELECT review_details, reasoning, objections
        FROM hypothesis_reviews
        WHERE hypothesis_id = ? AND agent_name = 'reflection'
        ORDER BY timestamp DESC
        LIMIT 1
    """, (hypothesis_id,)).fetchone()

    if not row:
        return {}

    details: dict = {}
    if row["review_details"]:
        try:
            details = json.loads(row["review_details"])
        except (json.JSONDecodeError, TypeError):
            pass

    return {
        "concrete_revisions": details.get("concrete_revisions", []),
        "blockers":           details.get("blockers", []),
        "evidence_requests":  details.get("evidence_requests", []),
        "reasoning":          row["reasoning"] or "",
        "objections":         row["objections"] or "",
    }


def get_revision_cycle(conn: sqlite3.Connection, hypothesis_id: str) -> int:
    """Current revision cycle count for a hypothesis (0 if never attempted)."""
    row = conn.execute(
        "SELECT revision_cycle FROM revision_tracking WHERE hypothesis_id = ?",
        (hypothesis_id,)
    ).fetchone()
    return row["revision_cycle"] if row else 0


def update_tracking(conn: sqlite3.Connection, hypothesis_id: str, status: str, notes: str = ""):
    """
    Upsert revision_tracking row — increment cycle counter on each call.
    """
    now = datetime.now(timezone.utc).isoformat()
    conn.execute("""
        INSERT INTO revision_tracking (hypothesis_id, revision_cycle, last_attempted, last_status, notes)
        VALUES (?, 1, ?, ?, ?)
        ON CONFLICT(hypothesis_id) DO UPDATE SET
            revision_cycle = revision_cycle + 1,
            last_attempted = excluded.last_attempted,
            last_status    = excluded.last_status,
            notes          = excluded.notes
    """, (hypothesis_id, now, status, notes))
    conn.commit()


def patch_hypothesis_evidence(conn: sqlite3.Connection, hyp_id: str, new_evidence: str):
    """Overwrite the evidence field with the revised addendum prepended."""
    now = datetime.now(timezone.utc).isoformat()
    conn.execute(
        "UPDATE hypotheses SET evidence = ?, updated_at = ? WHERE id = ?",
        (new_evidence, now, hyp_id)
    )
    conn.commit()


# ── Inbox Scanner ──────────────────────────────────────────────────────────────

def scan_inbox_bundles(inbox: Path, hypothesis_id: str) -> list[dict]:
    """
    Search the inbox for simulation bundles that link to this hypothesis.
    Matches on `supports_hypothesis` field or hypothesis_id substring in filename.
    """
    bundles: list[dict] = []
    if not inbox.exists():
        return bundles

    for path in sorted(inbox.glob("simulation_bundle_*.json")):
        try:
            with open(path, encoding="utf-8") as f:
                b = json.load(f)
        except (json.JSONDecodeError, OSError):
            continue

        supported = b.get("supports_hypothesis", "")
        if supported == hypothesis_id or hypothesis_id in str(supported):
            se = b.get("structured_evidence", {})
            bundles.append({
                "file":                 path.name,
                "summary":              b.get("summary", ""),
                "payload_type":         b.get("payload_type", ""),
                "significance":         b.get("significance", 0.5),
                "testable_predictions": se.get("testable_predictions", []),
            })

    return bundles


# ── Evidence Builder ───────────────────────────────────────────────────────────

def build_evidence_addendum(
    original_evidence: str,
    new_memories: list[dict],
    simulation_bundles: list[dict],
    reflection: dict,
    revision_cycle: int,
) -> str:
    """
    Prepend a revision block to the hypothesis evidence string.
    Preserves all original evidence — nothing is deleted.
    """
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    lines: list[str] = [f"[Revision {revision_cycle + 1} — {today}]"]

    # Council reflection guidance
    concrete = reflection.get("concrete_revisions", [])
    if concrete:
        lines.append("Reflection council guidance:")
        for rev in concrete[:4]:
            lines.append(f"  • {rev}")

    # Simulation engine results
    if simulation_bundles:
        lines.append("New simulation evidence:")
        for b in simulation_bundles[:3]:
            lines.append(f"  • [{b['payload_type']}] {b['summary']}")
            for tp in b.get("testable_predictions", [])[:2]:
                pred = tp.get("prediction", "") if isinstance(tp, dict) else str(tp)
                if pred:
                    lines.append(f"    -> {pred}")

    # New supporting memories
    if new_memories:
        lines.append("New supporting memories:")
        for m in new_memories[:5]:
            snippet = (m.get("content") or "")[:200].replace("\n", " ").strip()
            ts = (m.get("timestamp") or "")[:10]
            lines.append(f"  • [{ts}] {snippet}")

    addendum = "\n".join(lines)
    return addendum + "\n\n" + (original_evidence or "")


# ── Bridge Submission ──────────────────────────────────────────────────────────

def submit_to_bridge(hyp_id: str, bridge_url: str = BRIDGE_URL, timeout: int = BRIDGE_TIMEOUT_SEC) -> dict:
    """
    POST { hyp_id, force: true } to the agent bridge's /api/council/reprocess.
    Returns the parsed JSON response or an error dict.
    """
    payload = json.dumps({"hyp_id": hyp_id, "force": True}).encode("utf-8")
    req = urllib.request.Request(
        bridge_url,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")
        return {"error": f"HTTP {e.code}: {body[:300]}"}
    except urllib.error.URLError as e:
        return {"error": f"Bridge unreachable: {e.reason}"}
    except Exception as e:
        return {"error": str(e)}


# ── Core Worker ────────────────────────────────────────────────────────────────

class HypothesisRevisionLoop:
    """
    Autonomous worker that re-submits `needs_revision` hypotheses to the council
    after gathering new simulation and memory evidence.

    Parameters
    ----------
    db_path      : path to manatuabon.db
    inbox_path   : path to the inbox directory watched by the ingest agent
    bridge_url   : URL of the bridge /api/council/reprocess endpoint
    dry_run      : if True, nothing is written to DB or bridge (useful for tests)
    """

    def __init__(
        self,
        db_path: Path = DB_PATH,
        inbox_path: Path = INBOX_PATH,
        bridge_url: str = BRIDGE_URL,
        dry_run: bool = False,
    ):
        self.db_path    = db_path
        self.inbox_path = inbox_path
        self.bridge_url = bridge_url
        self.dry_run    = dry_run

    # ── Internal helpers ──────────────────────────────────────────────────────

    def _open(self) -> sqlite3.Connection:
        conn = open_db(self.db_path)
        ensure_tracking_table(conn)
        return conn

    # ── Public interface ──────────────────────────────────────────────────────

    def process_one(self, candidate: dict, conn: sqlite3.Connection) -> dict:
        """
        Process a single needs_revision hypothesis candidate.
        Returns a status dict summarising what happened.
        """
        hyp_id   = candidate["id"]
        title    = candidate["title"]
        cycle    = int(candidate.get("revision_cycle", 0))
        since    = candidate.get("last_decision_at")

        log.info("[revision] %s (cycle %d): %s", hyp_id, cycle, title)

        # 1. Evidence gathering
        new_memories = get_new_memories(conn, hyp_id, since)
        sim_bundles  = scan_inbox_bundles(self.inbox_path, hyp_id)
        reflection   = get_reflection_guidance(conn, hyp_id)

        log.info(
            "   memories=%d  sim_bundles=%d  reflection_revisions=%d",
            len(new_memories),
            len(sim_bundles),
            len(reflection.get("concrete_revisions", [])),
        )

        # 2. Build revised evidence string
        original_evidence = candidate.get("evidence") or ""
        revised_evidence  = build_evidence_addendum(
            original_evidence, new_memories, sim_bundles, reflection, cycle
        )

        # 3. Patch DB evidence (audit trail — no information lost)
        if not self.dry_run:
            patch_hypothesis_evidence(conn, hyp_id, revised_evidence)

        # 4. Submit to council via bridge
        if self.dry_run:
            bridge_result = {"dry_run": True, "hyp_id": hyp_id}
            status = "dry_run"
        else:
            bridge_result = submit_to_bridge(hyp_id, self.bridge_url)
            status = "error" if "error" in bridge_result else "submitted"
            if status == "error":
                log.warning("   Bridge error: %s", bridge_result["error"])
            else:
                log.info(
                    "   Council result: %s",
                    bridge_result.get("decision", bridge_result.get("status", "?")),
                )

        # 5. Record revision cycle
        notes = json.dumps({
            "new_memories":     len(new_memories),
            "sim_bundles":      len(sim_bundles),
            "has_reflection":   bool(reflection),
            "has_new_evidence": bool(new_memories or sim_bundles),
            "bridge_result":    bridge_result,
        })
        if not self.dry_run:
            update_tracking(conn, hyp_id, status, notes)

        return {
            "hyp_id":       hyp_id,
            "title":        title,
            "cycle":        cycle + 1,
            "new_memories": len(new_memories),
            "sim_bundles":  len(sim_bundles),
            "status":       status,
            "bridge_result": bridge_result,
        }

    def run_once(self) -> list[dict]:
        """
        One full pass: find all eligible candidates and process them.
        Returns a list of per-hypothesis result dicts.
        """
        conn = self._open()
        try:
            candidates = get_needs_revision_candidates(conn)
            log.info("Found %d needs_revision candidate(s)", len(candidates))
            results: list[dict] = []
            for c in candidates:
                try:
                    r = self.process_one(c, conn)
                    results.append(r)
                except Exception as e:
                    log.error("Error processing %s: %s", c.get("id"), e, exc_info=True)
            return results
        finally:
            conn.close()

    def loop(self, interval_sec: int = POLL_INTERVAL_SEC):
        """Continuous polling loop — runs until interrupted."""
        log.info(
            "🔄 Revision loop started  interval=%ds  dry_run=%s",
            interval_sec, self.dry_run,
        )
        while True:
            try:
                results = self.run_once()
                if results:
                    log.info(
                        "Processed %d hypothesis/hypotheses: %s",
                        len(results),
                        [r["hyp_id"] for r in results],
                    )
            except Exception as e:
                log.error("Revision loop error: %s", e, exc_info=True)
            time.sleep(interval_sec)


# ── CLI ────────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Manatuabon Hypothesis Revision Loop — re-submits needs_revision hypotheses"
    )
    parser.add_argument("--db",         default=str(DB_PATH),    help="Path to manatuabon.db")
    parser.add_argument("--inbox",      default=str(INBOX_PATH), help="Path to inbox directory")
    parser.add_argument("--bridge",     default=BRIDGE_URL,      help="Bridge reprocess endpoint URL")
    parser.add_argument("--interval",   type=int, default=POLL_INTERVAL_SEC, help="Poll interval (seconds)")
    parser.add_argument("--once",       action="store_true", help="Run one pass then exit")
    parser.add_argument("--dry-run",    action="store_true", help="No DB writes, no bridge calls")
    parser.add_argument("--hypothesis", default=None, metavar="HYP_ID",
                        help="Force-process a single hypothesis by ID regardless of status")
    parser.add_argument("--verbose",    action="store_true", help="Debug-level logging")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s [revision_loop] %(levelname)s  %(message)s",
        datefmt="%H:%M:%S",
    )

    worker = HypothesisRevisionLoop(
        db_path    = Path(args.db),
        inbox_path = Path(args.inbox),
        bridge_url = args.bridge,
        dry_run    = args.dry_run,
    )

    if args.hypothesis:
        # Single-hypothesis mode: bypass eligibility checks
        conn = worker._open()
        try:
            row = conn.execute(
                "SELECT * FROM hypotheses WHERE id = ?", (args.hypothesis,)
            ).fetchone()
            if not row:
                print(f"Hypothesis '{args.hypothesis}' not found in DB.")
                return
            candidate = dict(row)
            d = conn.execute(
                "SELECT timestamp, reasoning FROM hypothesis_decisions "
                "WHERE hypothesis_id = ? ORDER BY id DESC LIMIT 1",
                (args.hypothesis,)
            ).fetchone()
            candidate["last_decision_at"] = d["timestamp"] if d else None
            candidate["last_reasoning"]   = d["reasoning"] if d else ""
            candidate["revision_cycle"]   = get_revision_cycle(conn, args.hypothesis)
            result = worker.process_one(candidate, conn)
        finally:
            conn.close()
        print(json.dumps(result, indent=2))
        return

    if args.once:
        results = worker.run_once()
        print(json.dumps(results, indent=2))
    else:
        worker.loop(interval_sec=args.interval)


if __name__ == "__main__":
    main()
