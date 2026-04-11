"""
MANATUABON CONFIDENCE DECAY WORKER
=====================================
Hypotheses that stop receiving new evidence gradually lose confidence.
This keeps the DB honest — a hypothesis that was exciting in 2022 but has
received no new supporting memories since then shouldn't hold the same
weight as one actively accumulating evidence today.

Decay model
-----------
  new_confidence = current_confidence × decay_factor ^ periods_elapsed

where:
  period     = DECAY_PERIOD_DAYS (default 30)
  decay_rate = DECAY_RATE_PER_PERIOD (default 0.03, i.e. 3 % per period)
  decay_factor = 1 - decay_rate

So a hypothesis with no new evidence for 90 days loses ~9% confidence.
Confidence is clamped to [DECAY_FLOOR, 1.0] — hypotheses never fully die
from decay alone; rejection requires explicit council review.

Exclusions (never decayed):
  - Status: accepted, rejected, rejected_auto, merged
  - Hypotheses with Tier A evidence added in the last GRACE_DAYS

Usage:
  python confidence_decay.py --once          # one pass then exit
  python confidence_decay.py                  # daily continuous loop
  python confidence_decay.py --dry-run        # preview what would change
  python confidence_decay.py --hypothesis H3  # single hypothesis
"""

import argparse
import json
import logging
import sqlite3
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

log = logging.getLogger("manatuabon.confidence_decay")

# ── Configuration ──────────────────────────────────────────────────────────────

DB_PATH              = Path(__file__).resolve().parent / "manatuabon.db"

DECAY_PERIOD_DAYS    = 30      # one period = 30 days
DECAY_RATE_PER_PERIOD = 0.03   # 3% confidence loss per period with no new evidence
DECAY_FLOOR          = 0.10    # confidence never drops below 10% from decay alone
GRACE_DAYS           = 60      # hypotheses with evidence newer than this are immune
POLL_INTERVAL_SEC    = 86400   # run once per day by default

# Statuses that are considered "closed" — no decay applied
CLOSED_STATUSES = {"accepted", "rejected", "rejected_auto", "merged"}


# ── DB Helpers ─────────────────────────────────────────────────────────────────

def open_db(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path), timeout=10)
    conn.row_factory = sqlite3.Row
    return conn


def ensure_decay_table(conn: sqlite3.Connection):
    """Create confidence_decay_log table to track every decay event."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS confidence_decay_log (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            hypothesis_id   TEXT NOT NULL,
            old_confidence  REAL NOT NULL,
            new_confidence  REAL NOT NULL,
            periods_elapsed REAL NOT NULL,
            days_since_evidence INTEGER NOT NULL,
            applied_at      TEXT NOT NULL
        )
    """)
    conn.commit()


def get_decay_candidates(conn: sqlite3.Connection) -> list[dict]:
    """
    Return active hypotheses eligible for confidence decay.

    Eligible means:
      - Status is NOT in CLOSED_STATUSES
      - Last supporting memory was added more than GRACE_DAYS ago
        (or no supporting memory exists at all)
      - Current confidence is above DECAY_FLOOR
    """
    grace_cutoff = (
        datetime.now(timezone.utc) - timedelta(days=GRACE_DAYS)
    ).isoformat()

    rows = conn.execute("""
        SELECT
            h.id,
            h.title,
            h.status,
            COALESCE(h.confidence, 0.5)         AS confidence,
            h.updated_at,
            MAX(m.timestamp)                     AS latest_evidence_at
        FROM hypotheses h
        LEFT JOIN memories m
            ON (m.supports_hypothesis = h.id OR m.supports_hypothesis LIKE '%' || h.id || '%')
            AND m.significance >= 3
        WHERE COALESCE(h.status, 'proposed') NOT IN ('accepted','rejected','rejected_auto','merged')
          AND COALESCE(h.confidence, 0.5) > ?
        GROUP BY h.id
        HAVING COALESCE(MAX(m.timestamp), '1970-01-01') < ?
    """, (DECAY_FLOOR, grace_cutoff)).fetchall()

    return [dict(r) for r in rows]


def compute_decay(
    confidence: float,
    days_since_evidence: int,
    period_days: int = DECAY_PERIOD_DAYS,
    rate: float = DECAY_RATE_PER_PERIOD,
    floor: float = DECAY_FLOOR,
) -> tuple[float, float]:
    """
    Return (new_confidence, periods_elapsed).
    Confidence is clamped to [floor, 1.0].
    """
    periods = max(0.0, days_since_evidence / period_days)
    decay_factor = (1.0 - rate) ** periods
    new_conf = max(floor, round(confidence * decay_factor, 4))
    return new_conf, round(periods, 2)


def apply_decay_to_hypothesis(
    conn: sqlite3.Connection,
    hyp_id: str,
    old_conf: float,
    new_conf: float,
    periods: float,
    days_since: int,
):
    """Write decayed confidence to hypotheses table and log the event."""
    now = datetime.now(timezone.utc).isoformat()
    conn.execute(
        "UPDATE hypotheses SET confidence = ?, updated_at = ? WHERE id = ?",
        (new_conf, now, hyp_id)
    )
    conn.execute("""
        INSERT INTO confidence_decay_log
            (hypothesis_id, old_confidence, new_confidence, periods_elapsed, days_since_evidence, applied_at)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (hyp_id, old_conf, new_conf, periods, days_since, now))
    conn.commit()


def days_since_evidence(latest_evidence_at: str | None) -> int:
    """Days between latest evidence timestamp and now. Returns large number if None."""
    if not latest_evidence_at:
        return 9999
    try:
        # Handle both offset-aware and naive timestamps
        ts_str = latest_evidence_at.replace("Z", "+00:00")
        if "+" not in ts_str and not ts_str.endswith("Z"):
            ts = datetime.fromisoformat(ts_str).replace(tzinfo=timezone.utc)
        else:
            ts = datetime.fromisoformat(ts_str)
        delta = datetime.now(timezone.utc) - ts.astimezone(timezone.utc)
        return max(0, delta.days)
    except (ValueError, TypeError):
        return 9999


def get_decay_summary(conn: sqlite3.Connection, hypothesis_id: str) -> list[dict]:
    """Return recent decay log entries for a hypothesis."""
    rows = conn.execute("""
        SELECT old_confidence, new_confidence, days_since_evidence, applied_at
        FROM confidence_decay_log
        WHERE hypothesis_id = ?
        ORDER BY id DESC
        LIMIT 10
    """, (hypothesis_id,)).fetchall()
    return [dict(r) for r in rows]


# ── Core Worker ────────────────────────────────────────────────────────────────

class ConfidenceDecayWorker:
    """
    Applies time-based confidence decay to hypotheses that are no longer
    receiving new supporting evidence.

    Parameters
    ----------
    db_path       : path to manatuabon.db
    decay_floor   : minimum confidence from decay alone (default 0.10)
    grace_days    : immunity window — hypotheses with recent evidence skip decay
    dry_run       : if True, compute decay but don't write anything
    """

    def __init__(
        self,
        db_path: Path = DB_PATH,
        decay_floor: float = DECAY_FLOOR,
        grace_days: int = GRACE_DAYS,
        dry_run: bool = False,
    ):
        self.db_path    = db_path
        self.decay_floor = decay_floor
        self.grace_days  = grace_days
        self.dry_run     = dry_run

    def _open(self) -> sqlite3.Connection:
        conn = open_db(self.db_path)
        ensure_decay_table(conn)
        return conn

    def process_one(self, candidate: dict) -> dict | None:
        """
        Compute and (if not dry_run) apply decay for one hypothesis.
        Returns a result dict, or None if no meaningful decay occurred.
        """
        hyp_id      = candidate["id"]
        title       = candidate["title"]
        confidence  = float(candidate["confidence"])
        latest_ev   = candidate.get("latest_evidence_at")

        d_since = days_since_evidence(latest_ev)
        if d_since < GRACE_DAYS:
            return None   # shouldn't happen (filtered in SQL), but guard anyway

        new_conf, periods = compute_decay(confidence, d_since, floor=self.decay_floor)
        delta = confidence - new_conf

        if delta < 0.001:
            # Already at floor or negligible change — skip
            return None

        log.info(
            "[decay] %s  conf %.3f -> %.3f  (%.0fd without evidence, %.1f periods)",
            hyp_id, confidence, new_conf, d_since, periods
        )

        if not self.dry_run:
            conn = self._open()
            try:
                apply_decay_to_hypothesis(conn, hyp_id, confidence, new_conf, periods, d_since)
            finally:
                conn.close()

        return {
            "hyp_id":            hyp_id,
            "title":             title,
            "old_confidence":    confidence,
            "new_confidence":    new_conf,
            "delta":             round(delta, 4),
            "days_since_ev":     d_since,
            "periods":           periods,
            "dry_run":           self.dry_run,
        }

    def run_once(self) -> list[dict]:
        """One full decay pass across all eligible hypotheses."""
        conn = self._open()
        try:
            candidates = get_decay_candidates(conn)
        finally:
            conn.close()

        log.info("Decay pass — %d candidate(s) to evaluate", len(candidates))
        results: list[dict] = []
        for c in candidates:
            try:
                r = self.process_one(c)
                if r:
                    results.append(r)
            except Exception as e:
                log.error("Decay error for %s: %s", c.get("id"), e, exc_info=True)

        if results:
            total_delta = sum(r["delta"] for r in results)
            log.info(
                "Decay applied to %d hypothesis/hypotheses  total Δconf=−%.4f",
                len(results), total_delta
            )
        else:
            log.info("No hypotheses required decay this pass.")
        return results

    def loop(self, interval_sec: int = POLL_INTERVAL_SEC):
        log.info("[decay_worker] started (interval=%ds, dry_run=%s)",
                 interval_sec, self.dry_run)
        while True:
            try:
                self.run_once()
            except Exception as e:
                log.error("Decay loop error: %s", e, exc_info=True)
            time.sleep(interval_sec)


# ── CLI ────────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Manatuabon Confidence Decay Worker")
    parser.add_argument("--db",           default=str(DB_PATH), help="Path to manatuabon.db")
    parser.add_argument("--interval",     type=int, default=POLL_INTERVAL_SEC)
    parser.add_argument("--once",         action="store_true", help="Run once then exit")
    parser.add_argument("--dry-run",      action="store_true", help="Compute but don't write")
    parser.add_argument("--hypothesis",   default=None, metavar="HYP_ID",
                        help="Process a single hypothesis by ID")
    parser.add_argument("--grace-days",   type=int, default=GRACE_DAYS)
    parser.add_argument("--decay-floor",  type=float, default=DECAY_FLOOR)
    parser.add_argument("--verbose",      action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s [decay] %(levelname)s  %(message)s",
        datefmt="%H:%M:%S",
    )

    worker = ConfidenceDecayWorker(
        db_path     = Path(args.db),
        decay_floor = args.decay_floor,
        grace_days  = args.grace_days,
        dry_run     = args.dry_run,
    )

    if args.hypothesis:
        conn = worker._open()
        row = conn.execute("SELECT * FROM hypotheses WHERE id=?", (args.hypothesis,)).fetchone()
        conn.close()
        if not row:
            print(f"Hypothesis {args.hypothesis} not found.")
            return
        # Fetch latest evidence timestamp
        conn = worker._open()
        m = conn.execute(
            "SELECT MAX(timestamp) AS t FROM memories WHERE supports_hypothesis=? AND significance>=3",
            (args.hypothesis,)
        ).fetchone()
        conn.close()
        candidate = dict(row)
        candidate["latest_evidence_at"] = m["t"] if m else None
        result = worker.process_one(candidate)
        print(json.dumps(result, indent=2) if result else '{"status": "no decay needed"}')
        return

    if args.once:
        results = worker.run_once()
        print(json.dumps(results, indent=2))
    else:
        worker.loop(interval_sec=args.interval)


if __name__ == "__main__":
    main()
