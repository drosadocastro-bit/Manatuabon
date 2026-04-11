"""
MANATUABON VELA GLITCH WATCH
==============================
Monitors for new Vela pulsar timing data and glitch reports, then compares
any confirmed glitch epoch against the pulsar_glitch_stress engine prediction.

Simulation engine prediction (from simulation_worker.py):
  last_glitch   = 2019.416 (MJD 58502, 2019-Feb-01)
  mean_interval = 2.496 yr  (from 12 historical glitches spanning 1969-2019)
  predicted_next_epoch   = 2021.9 yr (lower bound)
  prediction_window      = 2021.9 – 2023.5
  window_center          = ~2022.4
  predicted_delta_nu_nu  = (2.0–3.0) × 10^-6  (typical Vela glitch size)

Verdict logic:
  - Glitch confirmed INSIDE window  → CONFIRMED bundle  (Tier A, sig=0.95)
  - Glitch confirmed OUTSIDE window → FALSIFIED bundle   (Tier A, sig=0.90)
  - Window elapsed, no glitch       → MISSED bundle      (Tier A, sig=0.85)
  - No new data yet                 → no bundle dropped

Data sources:
  1. arXiv     — search "Vela pulsar glitch" for new timing papers
  2. ATel RSS  — Vela glitch announcements (keywords: vela, psrj0835)
  3. ATNF Pulsar Catalogue glitch table (static reference, polled for updates)

Usage:
  python vela_glitch_watch.py --once
  python vela_glitch_watch.py --interval 3600
  python vela_glitch_watch.py --dry-run
  python vela_glitch_watch.py --check-missed   # force-check if window elapsed
"""

import argparse
import json
import logging
import re
import time
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from pathlib import Path

log = logging.getLogger("manatuabon.vela_watch")

# ── Configuration ──────────────────────────────────────────────────────────────

INBOX_PATH        = Path(__file__).resolve().parent / "inbox"
SEEN_PATH         = Path(__file__).resolve().parent / ".vela_watch_seen.json"
STATE_PATH        = Path(__file__).resolve().parent / ".vela_watch_state.json"
POLL_INTERVAL_SEC = 3600     # check every hour
HTTP_TIMEOUT      = 20

# Engine prediction constants
VELA_LAST_GLITCH_EPOCH    = 2019.416   # decimal year
VELA_PREDICTION_WINDOW_LO = 2021.9
VELA_PREDICTION_WINDOW_HI = 2023.5
VELA_WINDOW_CENTER        = 2022.4
VELA_PULSAR_NAME          = "PSR J0835-4510"
VELA_PULSAR_ALT           = "Vela"

# arXiv search
ARXIV_ENDPOINT    = "https://export.arxiv.org/api/query"
ARXIV_MAX_RESULTS = 8
ARXIV_LOOKBACK_DAYS = 30

# ATel RSS
ATEL_RSS_URL  = "https://www.astronomerstelegram.org/rss"
ATEL_KEYWORDS = ["vela pulsar", "psrj0835", "psr j0835", "vela glitch",
                 "j0835-4510", "vela timing"]

# ATNF glitch catalogue (public, no auth needed)
ATNF_GLITCH_URL = (
    "https://www.atnf.csiro.au/people/pulsar/psrcat/glitchTbl.html"
)


# ── Helpers ────────────────────────────────────────────────────────────────────

def decimal_year_now() -> float:
    now = datetime.now(timezone.utc)
    start = datetime(now.year, 1, 1, tzinfo=timezone.utc)
    end   = datetime(now.year + 1, 1, 1, tzinfo=timezone.utc)
    return now.year + (now - start).total_seconds() / (end - start).total_seconds()


def epoch_in_window(epoch: float) -> bool:
    return VELA_PREDICTION_WINDOW_LO <= epoch <= VELA_PREDICTION_WINDOW_HI


def window_elapsed() -> bool:
    return decimal_year_now() > VELA_PREDICTION_WINDOW_HI + 0.5   # 6-month buffer


def load_json(path: Path, default):
    if path.exists():
        try:
            return json.loads(path.read_text())
        except (json.JSONDecodeError, OSError):
            pass
    return default


def save_json(path: Path, data):
    path.write_text(json.dumps(data, indent=2), encoding="utf-8")


def make_id(text: str) -> str:
    import hashlib
    return hashlib.sha1(text.encode()).hexdigest()[:16]


def make_bundle(
    verdict: str,
    glitch_epoch: float | None,
    delta_nu_nu: float | None,
    source: str,
    detail: str,
    significance: float,
    url: str = "",
) -> dict:
    """Format a Vela glitch verdict as a structured_ingest_v1 bundle."""
    window_str = f"{VELA_PREDICTION_WINDOW_LO}–{VELA_PREDICTION_WINDOW_HI}"
    summary = (
        f"[Vela Glitch Watch] {verdict}. "
        f"Prediction window: {window_str}. "
        f"{'Glitch epoch: ' + str(glitch_epoch) if glitch_epoch else 'No confirmed glitch in window.'} "
        f"{detail}"
    )
    return {
        "manatuabon_schema":  "structured_ingest_v1",
        "payload_type":       "pulsar/vela_glitch_verdict",
        "source":             source,
        "url":                url,
        "summary":            summary,
        "entities":           [VELA_PULSAR_NAME, "Vela pulsar", "glitch"],
        "significance":       significance,
        "domain_tags":        ["pulsar", "vela", "neutron_star", "glitch", "timing"],
        "supports_hypothesis": None,
        "structured_evidence": {
            "verdict":               verdict,
            "glitch_epoch":          glitch_epoch,
            "delta_nu_nu":           delta_nu_nu,
            "prediction_window_lo":  VELA_PREDICTION_WINDOW_LO,
            "prediction_window_hi":  VELA_PREDICTION_WINDOW_HI,
            "predicted_window_center": VELA_WINDOW_CENTER,
            "last_known_glitch":     VELA_LAST_GLITCH_EPOCH,
            "source":                source,
            "detail":                detail,
            "tier":                  "A" if significance >= 0.85 else "B",
            "checked_at":            datetime.now(timezone.utc).isoformat(),
        },
        "ingested_at": datetime.now(timezone.utc).isoformat(),
    }


def drop_bundle(bundle: dict, inbox: Path) -> Path:
    from datetime import datetime
    ts   = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    slug = make_id(bundle["summary"])
    out  = inbox / f"vela_watch_{ts}_{slug}.json"
    out.write_text(json.dumps(bundle, indent=2, ensure_ascii=False), encoding="utf-8")
    return out


# ── Epoch Extraction ───────────────────────────────────────────────────────────

# Regex patterns for extracting glitch epoch from text
YEAR_PATTERNS = [
    r"glitch.*?(\d{4}[\.\d]*)\s*(?:yr|year)",    # "glitch at 2022.3 yr"
    r"glitch.*?MJD\s*(\d{5,6})",                  # "glitch at MJD 59550"
    r"epoch.*?(\d{4}[\.\d]{1,4})",                # "epoch 2022.3"
    r"(\d{4}\.\d{1,3})\s*glitch",                 # "2022.3 glitch"
]

DELTA_NU_PATTERNS = [
    r"[Δδ]ν/ν\s*[=≈~]\s*([\d\.]+)\s*×?\s*10[^−\-]?(\d+|-\d+)",
    r"fractional\s+(?:spin[- ]up|frequency)\s+change.*?([\d\.]+)\s*[×x]\s*10.(\d+)",
    r"glitch\s+size.*?([\d\.]+)\s*[×x]\s*10.(\d+)",
]


def mjd_to_decimal_year(mjd: float) -> float:
    """Convert Modified Julian Date to decimal year (approximate)."""
    # MJD 51544.5 = J2000.0 = 2000.0
    return 2000.0 + (mjd - 51544.5) / 365.25


def extract_epoch(text: str) -> float | None:
    """Try to extract a glitch epoch from free text. Returns decimal year or None."""
    for pat in YEAR_PATTERNS:
        m = re.search(pat, text, re.IGNORECASE)
        if m:
            val = m.group(1)
            try:
                f = float(val)
                if 51000 < f < 70000:         # looks like MJD
                    return mjd_to_decimal_year(f)
                if 2015 < f < 2035:           # decimal year (reasonable range)
                    return f
            except ValueError:
                continue
    return None


def extract_delta_nu_nu(text: str) -> float | None:
    """Try to extract Δν/ν from free text."""
    for pat in DELTA_NU_PATTERNS:
        m = re.search(pat, text, re.IGNORECASE)
        if m:
            try:
                mantissa = float(m.group(1))
                exponent = int(m.group(2))
                return mantissa * (10 ** exponent)
            except (ValueError, IndexError):
                continue
    return None


def is_vela_related(text: str) -> bool:
    text_low = text.lower()
    return any(kw in text_low for kw in [
        "vela", "j0835", "psrj0835", "psr j0835",
        "vela pulsar", "vela glitch",
    ])


# ── arXiv Fetcher ──────────────────────────────────────────────────────────────

def fetch_arxiv_vela(timeout: int = HTTP_TIMEOUT) -> list[dict]:
    queries = [
        "ti:Vela AND ti:pulsar AND ti:glitch",
        "abs:\"PSR J0835\" AND abs:glitch",
        "abs:\"Vela pulsar\" AND abs:(glitch OR timing)",
    ]
    entries = []
    seen_ids: set = set()
    for q in queries:
        params = urllib.parse.urlencode({
            "search_query": q,
            "start": 0,
            "max_results": ARXIV_MAX_RESULTS,
            "sortBy": "submittedDate",
            "sortOrder": "descending",
        })
        url = f"{ARXIV_ENDPOINT}?{params}"
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "Manatuabon/1.0"})
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                data = resp.read().decode("utf-8")
        except Exception as e:
            log.warning("arXiv Vela fetch failed: %s", e)
            continue
        try:
            ns = {"atom": "http://www.w3.org/2005/Atom"}
            root = ET.fromstring(data)
            for entry in root.findall("atom:entry", ns):
                arxiv_id = (entry.findtext("atom:id", namespaces=ns) or "").split("/abs/")[-1]
                if arxiv_id in seen_ids:
                    continue
                seen_ids.add(arxiv_id)
                title   = (entry.findtext("atom:title", namespaces=ns) or "").strip()
                summary = (entry.findtext("atom:summary", namespaces=ns) or "").strip()
                pub     = entry.findtext("atom:published", namespaces=ns) or ""
                link    = ""
                for lnk in entry.findall("atom:link", ns):
                    if lnk.get("type") == "text/html":
                        link = lnk.get("href", "")
                text = f"{title} {summary}"
                if is_vela_related(text):
                    entries.append({
                        "id":        arxiv_id,
                        "title":     title,
                        "abstract":  summary[:1000],
                        "published": pub,
                        "url":       link or f"https://arxiv.org/abs/{arxiv_id}",
                    })
        except ET.ParseError as e:
            log.warning("arXiv parse error: %s", e)
    return entries


def fetch_atel_vela(timeout: int = HTTP_TIMEOUT) -> list[dict]:
    try:
        req = urllib.request.Request(ATEL_RSS_URL, headers={"User-Agent": "Manatuabon/1.0"})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            data = resp.read().decode("utf-8", errors="replace")
    except Exception as e:
        log.warning("ATel fetch failed: %s", e)
        return []

    entries = []
    try:
        root = ET.fromstring(data)
        channel = root.find("channel")
        if channel is None:
            return []
        for item in channel.findall("item"):
            title = (item.findtext("title") or "").strip()
            desc  = (item.findtext("description") or "").strip()
            link  = (item.findtext("link") or "").strip()
            pub   = (item.findtext("pubDate") or "").strip()
            text  = f"{title} {desc}"
            if any(kw in text.lower() for kw in ATEL_KEYWORDS):
                entries.append({
                    "id":          make_id(link or title),
                    "title":       title,
                    "description": desc[:800],
                    "url":         link,
                    "published":   pub,
                })
    except ET.ParseError:
        pass
    return entries


# ── Core Worker ────────────────────────────────────────────────────────────────

class VelaGlitchWatch:
    """
    Polls arXiv and ATel for Vela pulsar timing data, extracts glitch
    epochs, and compares against the engine prediction window.

    Drops a structured_ingest_v1 bundle whenever a verdict can be made:
      CONFIRMED  — glitch inside predicted window
      FALSIFIED  — glitch outside predicted window
      MISSED     — window elapsed without confirmed glitch
    """

    def __init__(
        self,
        inbox_path: Path = INBOX_PATH,
        seen_path:  Path = SEEN_PATH,
        state_path: Path = STATE_PATH,
        dry_run:    bool = False,
    ):
        self.inbox      = inbox_path
        self.seen_path  = seen_path
        self.state_path = state_path
        self.dry_run    = dry_run
        self._seen      = load_json(seen_path, [])
        self._seen_set  = set(self._seen)
        self._state     = load_json(state_path, {"verdict_issued": None})

    def _save(self):
        if not self.dry_run:
            save_json(self.seen_path, sorted(self._seen_set))
            save_json(self.state_path, self._state)

    def _already_verdicted(self) -> bool:
        return self._state.get("verdict_issued") is not None

    def _issue_verdict(self, bundle: dict, verdict: str):
        if not self.dry_run:
            self.inbox.mkdir(parents=True, exist_ok=True)
            out = drop_bundle(bundle, self.inbox)
            log.info("[vela] verdict '%s' -> %s", verdict, out.name)
        else:
            log.info("[vela] [dry-run] Would drop verdict: %s", verdict)
        self._state["verdict_issued"] = verdict
        self._state["issued_at"] = datetime.now(timezone.utc).isoformat()

    def _process_entry(self, entry_id: str, title: str, text: str, url: str) -> bool:
        """
        Extract epoch from entry text, issue verdict if warranted.
        Returns True if a verdict bundle was dropped.
        """
        if entry_id in self._seen_set:
            return False
        self._seen_set.add(entry_id)

        if not is_vela_related(text):
            return False

        epoch      = extract_epoch(text)
        delta_nu_nu = extract_delta_nu_nu(text)

        if epoch is None:
            log.debug("Vela entry found but no epoch extracted: %s", title[:60])
            return False

        log.info("Vela glitch epoch extracted: %.3f from '%s'", epoch, title[:60])

        if self._already_verdicted():
            log.debug("Verdict already issued — skipping duplicate")
            return False

        if epoch_in_window(epoch):
            verdict = "CONFIRMED"
            sig     = 0.95
            detail  = (
                f"Glitch at {epoch:.3f} falls inside predicted window "
                f"{VELA_PREDICTION_WINDOW_LO}–{VELA_PREDICTION_WINDOW_HI}. "
                f"Engine prediction VALIDATED."
            )
        else:
            verdict = "FALSIFIED"
            sig     = 0.90
            detail  = (
                f"Glitch at {epoch:.3f} is OUTSIDE predicted window "
                f"{VELA_PREDICTION_WINDOW_LO}–{VELA_PREDICTION_WINDOW_HI}. "
                f"Engine prediction falsified — recalibrate mean interval or stress model."
            )

        bundle = make_bundle(
            verdict      = verdict,
            glitch_epoch = epoch,
            delta_nu_nu  = delta_nu_nu,
            source       = url or "arXiv/ATel",
            detail       = detail,
            significance = sig,
            url          = url,
        )
        self._issue_verdict(bundle, verdict)
        return True

    def check_missed(self) -> bool:
        """
        If the prediction window has elapsed and no glitch was confirmed, drop
        a MISSED bundle (the silence is also evidence — model needs revision).
        """
        if self._already_verdicted():
            return False
        if not window_elapsed():
            return False
        log.info("[vela] Prediction window elapsed -- issuing MISSED verdict")
        bundle = make_bundle(
            verdict      = "MISSED",
            glitch_epoch = None,
            delta_nu_nu  = None,
            source       = "VelaGlitchWatch/elapsed",
            detail       = (
                f"No confirmed glitch found in window {VELA_PREDICTION_WINDOW_LO}–"
                f"{VELA_PREDICTION_WINDOW_HI} as of {decimal_year_now():.2f}. "
                f"Stress accumulation model may underestimate inter-glitch interval."
            ),
            significance = 0.85,
        )
        self._issue_verdict(bundle, "MISSED")
        return True

    def run_once(self, check_missed: bool = True) -> dict:
        verdicts_issued = 0

        # arXiv papers
        for e in fetch_arxiv_vela():
            text = f"{e['title']} {e['abstract']}"
            if self._process_entry(e["id"], e["title"], text, e["url"]):
                verdicts_issued += 1

        # ATel telegrams
        for e in fetch_atel_vela():
            text = f"{e['title']} {e['description']}"
            if self._process_entry(e["id"], e["title"], text, e["url"]):
                verdicts_issued += 1

        # Check if window elapsed without confirmation
        if check_missed and self.check_missed():
            verdicts_issued += 1

        self._save()
        log.info(
            "Vela watch pass complete — %d verdict(s) issued  state=%s",
            verdicts_issued, self._state.get("verdict_issued", "pending")
        )
        return {
            "verdict_issued": verdicts_issued,
            "current_verdict": self._state.get("verdict_issued"),
            "current_epoch":  decimal_year_now(),
            "window": f"{VELA_PREDICTION_WINDOW_LO}–{VELA_PREDICTION_WINDOW_HI}",
        }

    def loop(self, interval_sec: int = POLL_INTERVAL_SEC):
        log.info("[vela_watch] started (interval=%ds, dry_run=%s)",
                 interval_sec, self.dry_run)
        while True:
            try:
                self.run_once()
                if self._already_verdicted():
                    log.info("Verdict issued — watch going to sleep (checking every 24h for updates)")
                    time.sleep(86400)
                    continue
            except Exception as e:
                log.error("Vela watch error: %s", e, exc_info=True)
            time.sleep(interval_sec)


# ── CLI ────────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Manatuabon Vela Pulsar Glitch Watch")
    parser.add_argument("--inbox",         default=str(INBOX_PATH))
    parser.add_argument("--seen",          default=str(SEEN_PATH))
    parser.add_argument("--state",         default=str(STATE_PATH))
    parser.add_argument("--interval",      type=int, default=POLL_INTERVAL_SEC)
    parser.add_argument("--once",          action="store_true")
    parser.add_argument("--dry-run",       action="store_true")
    parser.add_argument("--check-missed",  action="store_true",
                        help="Force-check if prediction window has elapsed without confirmed glitch")
    parser.add_argument("--status",        action="store_true",
                        help="Print current watch state and exit")
    parser.add_argument("--verbose",       action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s [vela_watch] %(levelname)s  %(message)s",
        datefmt="%H:%M:%S",
    )

    watch = VelaGlitchWatch(
        inbox_path = Path(args.inbox),
        seen_path  = Path(args.seen),
        state_path = Path(args.state),
        dry_run    = args.dry_run,
    )

    if args.status:
        now = decimal_year_now()
        state = watch._state
        print(json.dumps({
            "current_year":  round(now, 3),
            "window":        f"{VELA_PREDICTION_WINDOW_LO}–{VELA_PREDICTION_WINDOW_HI}",
            "window_center": VELA_WINDOW_CENTER,
            "window_elapsed": window_elapsed(),
            "verdict_issued": state.get("verdict_issued"),
            "issued_at":     state.get("issued_at"),
        }, indent=2))
        return

    if args.check_missed:
        issued = watch.check_missed()
        watch._save()
        print(f"MISSED verdict {'issued' if issued else 'not needed (window not elapsed or verdict exists)'}.")
        return

    if args.once:
        result = watch.run_once()
        print(json.dumps(result, indent=2))
    else:
        watch.loop(interval_sec=args.interval)


if __name__ == "__main__":
    main()
