"""
MANATUABON GALACTIC CENTER MONITOR + EHT INGEST
================================================
Continuously fetches new astrophysics data relevant to the Galactic Center
and Event Horizon Telescope results, formats it as structured_ingest_v1
bundles, and drops them into the inbox for automatic evidence ingest.

Data sources
------------
1. arXiv API          — Sgr A*, Galactic Center, EHT papers (daily)
2. Zenodo EHT API     — new data releases from the EHT community (daily)
3. ATel RSS           — Astronomer's Telegrams for GC transient alerts (hourly)
4. Hardcoded EHT      — key published measurements pre-loaded as reference bundles

EHT key measurements already known (as of May 2025 knowledge cutoff):
  - Sgr A* shadow diameter:    51.8 ± 2.3 μas  (EHT 2022)
  - Ring diameter:             51.8 ± 2.3 μas
  - BH mass:                   4.0 ± 0.6 × 10^6 M_sun
  - Distance:                  8.2 kpc
  - First image: May 12, 2022

Usage:
  python galactic_center_monitor.py --once
  python galactic_center_monitor.py --interval 3600
  python galactic_center_monitor.py --eht-seed        # inject hardcoded EHT bundles
  python galactic_center_monitor.py --dry-run
"""

import argparse
import hashlib
import json
import logging
import time
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from pathlib import Path

log = logging.getLogger("manatuabon.gc_monitor")

# ── Configuration ──────────────────────────────────────────────────────────────

INBOX_PATH       = Path(__file__).resolve().parent / "inbox"
SEEN_PATH        = Path(__file__).resolve().parent / ".gc_monitor_seen.json"
POLL_INTERVAL_SEC = 3600    # default 1 hour
HTTP_TIMEOUT      = 20      # seconds per request

# arXiv API — returns Atom feed
ARXIV_ENDPOINT = "https://export.arxiv.org/api/query"
ARXIV_QUERIES  = [
    'ti:"Sgr A*" OR ti:"Sagittarius A*"',
    'ti:"Event Horizon Telescope" Galactic',
    'ti:"Galactic Center" AND (abs:black hole OR abs:accretion)',
    'ti:"Sgr A*" AND abs:flare',
]
ARXIV_MAX_RESULTS = 5
ARXIV_LOOKBACK_DAYS = 7    # only consider papers submitted in last 7 days

# Zenodo EHT community API
ZENODO_EHT_URL = "https://zenodo.org/api/records?communities=eht&sort=mostrecent&size=5"

# ATel RSS feed
ATEL_RSS_URL   = "https://www.astronomerstelegram.org/rss"
ATEL_KEYWORDS  = ["sgr a", "sagittarius a", "galactic center", "gc flare",
                  "event horizon telescope", "eht"]


# ── Seen-IDs persistence ───────────────────────────────────────────────────────

def load_seen(path: Path) -> set:
    if path.exists():
        try:
            return set(json.loads(path.read_text()))
        except (json.JSONDecodeError, OSError):
            pass
    return set()


def save_seen(path: Path, seen: set):
    path.write_text(json.dumps(sorted(seen), indent=2), encoding="utf-8")


def make_id(text: str) -> str:
    return hashlib.sha1(text.encode()).hexdigest()[:16]


# ── Bundle Formatter ───────────────────────────────────────────────────────────

def make_bundle(
    summary: str,
    source: str,
    payload_type: str,
    domain_tags: list[str],
    significance: float,
    entities: list[str],
    supports_hypothesis: str | None,
    structured_evidence: dict,
    url: str = "",
) -> dict:
    """Format a structured_ingest_v1 bundle ready for inbox drop."""
    return {
        "manatuabon_schema":  "structured_ingest_v1",
        "payload_type":       payload_type,
        "source":             source,
        "url":                url,
        "summary":            summary,
        "entities":           entities,
        "significance":       significance,
        "domain_tags":        domain_tags,
        "supports_hypothesis": supports_hypothesis,
        "structured_evidence": structured_evidence,
        "ingested_at":        datetime.now(timezone.utc).isoformat(),
    }


def drop_bundle(bundle: dict, inbox: Path, prefix: str = "gc_monitor") -> Path:
    """Write bundle to inbox as a uniquely named JSON file."""
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    slug = make_id(bundle["summary"])
    fname = f"{prefix}_{ts}_{slug}.json"
    out = inbox / fname
    out.write_text(json.dumps(bundle, indent=2, ensure_ascii=False), encoding="utf-8")
    return out


# ── EHT Reference Measurements (Tier A, pre-loaded) ──────────────────────────

EHT_REFERENCE_BUNDLES = [
    {
        "id": "eht_sgrA_2022_shadow",
        "summary": (
            "EHT 2022: First image of Sgr A*. Shadow diameter 51.8 ± 2.3 μas. "
            "Ring diameter consistent with GR prediction for M = 4.0 × 10^6 M_sun. "
            "Confirms Sgr A* is a supermassive black hole at d = 8.2 kpc."
        ),
        "payload_type": "eht/shadow_measurement",
        "significance": 0.98,
        "entities": ["Sgr A*", "Event Horizon Telescope", "shadow diameter"],
        "domain_tags": ["galactic_center", "black_hole", "eht", "direct_imaging"],
        "structured_evidence": {
            "shadow_diameter_uas": 51.8,
            "shadow_diameter_uncertainty_uas": 2.3,
            "bh_mass_msun": 4.0e6,
            "bh_mass_uncertainty_msun": 0.6e6,
            "distance_kpc": 8.2,
            "ring_diameter_uas": 51.8,
            "image_date": "2022-05-12",
            "observation_epoch": "2017",
            "wavelength_mm": 1.3,
            "tier": "A",
            "reference": "EHT Collaboration 2022, ApJL 930, L12-L17",
            "doi": "10.3847/2041-8213/ac6674",
            "testable_predictions": [
                {"prediction": "Shadow size should remain consistent across future EHT epochs",
                 "falsification": "Shadow size inconsistent with M=4e6 Msun at d=8.2 kpc"},
                {"prediction": "Ring substructure should reveal accretion disk orientation",
                 "falsification": "Featureless ring inconsistent with RIAF model"},
            ],
        },
    },
    {
        "id": "eht_sgrA_2022_variability",
        "summary": (
            "EHT 2022: Sgr A* exhibits rapid flux variability during 2017 observations "
            "consistent with hot spots orbiting at r ~ 1-3 r_ISCO. Variability timescale "
            "~30 min matches orbital period at innermost stable circular orbit."
        ),
        "payload_type": "eht/variability",
        "significance": 0.90,
        "entities": ["Sgr A*", "ISCO", "hot spot", "variability"],
        "domain_tags": ["galactic_center", "black_hole", "eht", "accretion", "variability"],
        "structured_evidence": {
            "variability_timescale_min": 30,
            "isco_radius_rg": 1.0,
            "orbital_period_at_isco_min": 28,
            "tier": "A",
            "reference": "EHT Collaboration 2022, ApJL 930, L13",
            "testable_predictions": [
                {"prediction": "Hot spot orbit should produce periodic near-IR flares detectable by GRAVITY",
                 "falsification": "No periodicity in GRAVITY flare timing"},
            ],
        },
    },
    {
        "id": "gravity_2020_gr_precession",
        "summary": (
            "GRAVITY Collaboration 2020: Schwarzschild precession of S2 orbit detected "
            "at 12.1 ± 0.3 arcmin/orbit. First direct measurement of GR precession "
            "around a supermassive black hole. Sgr A* mass = 4.10 ± 0.01 × 10^6 M_sun."
        ),
        "payload_type": "observation/gr_precession",
        "significance": 0.97,
        "entities": ["S2", "Sgr A*", "Schwarzschild precession", "GRAVITY"],
        "domain_tags": ["galactic_center", "gr_test", "s_stars", "orbital_dynamics"],
        "structured_evidence": {
            "precession_arcmin_per_orbit": 12.1,
            "precession_uncertainty_arcmin": 0.3,
            "bh_mass_msun": 4.10e6,
            "bh_mass_uncertainty_msun": 0.01e6,
            "tier": "A",
            "reference": "GRAVITY Collaboration, A&A 636, L5 (2020)",
            "doi": "10.1051/0004-6361/202037813",
            "testable_predictions": [
                {"prediction": "S62 and other short-period S-stars should show measurable precession",
                 "falsification": "S62 precession inconsistent with GR at Sgr A* mass"},
            ],
        },
    },
    {
        "id": "gillessen_2017_s2_orbit",
        "summary": (
            "Gillessen+ 2017: Updated S2 orbital parameters from 22 years of VLT monitoring. "
            "Semi-major axis = 0.1255 arcsec, period = 16.05 yr. Sgr A* distance = 8.32 kpc, "
            "mass = 4.28 × 10^6 M_sun (pre-GRAVITY)."
        ),
        "payload_type": "observation/stellar_orbit",
        "significance": 0.95,
        "entities": ["S2", "Sgr A*", "VLT", "stellar orbit"],
        "domain_tags": ["galactic_center", "s_stars", "orbital_dynamics"],
        "structured_evidence": {
            "semi_major_axis_arcsec": 0.1255,
            "period_yr": 16.05,
            "distance_kpc": 8.32,
            "bh_mass_msun": 4.28e6,
            "tier": "A",
            "reference": "Gillessen et al. 2017, ApJ 837, 30",
            "doi": "10.3847/1538-4357/aa5c41",
        },
    },
]


def seed_eht_bundles(inbox: Path, seen: set, dry_run: bool = False) -> list[str]:
    """Drop hardcoded EHT reference bundles that haven't been seen yet."""
    dropped = []
    for b in EHT_REFERENCE_BUNDLES:
        bid = b["id"]
        if bid in seen:
            log.debug("EHT bundle %s already seeded — skipping", bid)
            continue
        bundle = make_bundle(
            summary             = b["summary"],
            source              = "EHT Collaboration / GRAVITY Collaboration",
            payload_type        = b["payload_type"],
            domain_tags         = b["domain_tags"],
            significance        = b["significance"],
            entities            = b["entities"],
            supports_hypothesis = None,   # ingest agent will match
            structured_evidence = b["structured_evidence"],
        )
        if not dry_run:
            out = drop_bundle(bundle, inbox, prefix="eht_seed")
            log.info("[EHT seed] %s -> %s", bid, out.name)
        else:
            log.info("[EHT seed] [dry-run] Would drop: %s", bid)
        dropped.append(bid)
    return dropped


# ── arXiv Fetcher ──────────────────────────────────────────────────────────────

def fetch_arxiv(query: str, max_results: int = ARXIV_MAX_RESULTS, timeout: int = HTTP_TIMEOUT) -> list[dict]:
    """Query the arXiv Atom API and return parsed entries."""
    params = urllib.parse.urlencode({
        "search_query": query,
        "start": 0,
        "max_results": max_results,
        "sortBy": "submittedDate",
        "sortOrder": "descending",
    })
    url = f"{ARXIV_ENDPOINT}?{params}"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Manatuabon/1.0 (astrophysics research)"})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            data = resp.read().decode("utf-8")
    except Exception as e:
        log.warning("arXiv fetch failed for query '%s': %s", query[:60], e)
        return []

    entries = []
    try:
        ns = {"atom": "http://www.w3.org/2005/Atom"}
        root = ET.fromstring(data)
        for entry in root.findall("atom:entry", ns):
            arxiv_id = (entry.findtext("atom:id", namespaces=ns) or "").split("/abs/")[-1]
            title    = (entry.findtext("atom:title", namespaces=ns) or "").strip()
            summary  = (entry.findtext("atom:summary", namespaces=ns) or "").strip()
            published = entry.findtext("atom:published", namespaces=ns) or ""
            link     = ""
            for lnk in entry.findall("atom:link", ns):
                if lnk.get("type") == "text/html":
                    link = lnk.get("href", "")
            entries.append({
                "id":        arxiv_id,
                "title":     title,
                "abstract":  summary[:1000],
                "published": published,
                "url":       link or f"https://arxiv.org/abs/{arxiv_id}",
            })
    except ET.ParseError as e:
        log.warning("arXiv XML parse error: %s", e)
    return entries


def is_recent(published: str, lookback_days: int = ARXIV_LOOKBACK_DAYS) -> bool:
    """Return True if the paper was published within lookback_days."""
    if not published:
        return True  # assume recent if timestamp missing
    try:
        ts_str = published.replace("Z", "+00:00")
        ts = datetime.fromisoformat(ts_str)
        return datetime.now(timezone.utc) - ts.astimezone(timezone.utc) <= timedelta(days=lookback_days)
    except (ValueError, TypeError):
        return True


def process_arxiv_entry(entry: dict) -> dict:
    """Convert an arXiv entry into a structured_ingest_v1 bundle."""
    title    = entry["title"]
    abstract = entry["abstract"]
    text = f"{title}. {abstract}"

    # Significance heuristic: EHT/GRAVITY papers are Tier A
    sig = 0.70
    if any(kw in text.lower() for kw in ["event horizon telescope", "gravity collaboration", "eht"]):
        sig = 0.90
    elif any(kw in text.lower() for kw in ["sgr a*", "sagittarius a*"]):
        sig = 0.80

    entities = []
    for ent in ["Sgr A*", "M87", "EHT", "GRAVITY", "S2", "S62", "Hills mechanism",
                "Schwarzschild", "ISCO", "Bondi", "RIAF"]:
        if ent.lower() in text.lower():
            entities.append(ent)

    domain_tags = ["galactic_center"]
    if "eht" in text.lower() or "event horizon" in text.lower():
        domain_tags.append("eht")
    if "flare" in text.lower():
        domain_tags.append("flare")
    if "accretion" in text.lower():
        domain_tags.append("accretion")
    if "orbit" in text.lower() or "s2" in text.lower():
        domain_tags.append("orbital_dynamics")

    return make_bundle(
        summary             = f"[arXiv] {title[:200]}",
        source              = f"arXiv:{entry['id']}",
        payload_type        = "arxiv/paper",
        domain_tags         = domain_tags,
        significance        = sig,
        entities            = entities,
        supports_hypothesis = None,
        structured_evidence = {
            "arxiv_id":   entry["id"],
            "title":      title,
            "abstract":   abstract[:800],
            "published":  entry["published"],
            "tier":       "A" if sig >= 0.85 else "B",
        },
        url = entry["url"],
    )


# ── Zenodo EHT Fetcher ─────────────────────────────────────────────────────────

def fetch_zenodo_eht(timeout: int = HTTP_TIMEOUT) -> list[dict]:
    """Fetch recent EHT data releases from the Zenodo community API."""
    try:
        req = urllib.request.Request(
            ZENODO_EHT_URL,
            headers={"Accept": "application/json", "User-Agent": "Manatuabon/1.0"}
        )
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            data = json.loads(resp.read().decode("utf-8"))
    except Exception as e:
        log.warning("Zenodo EHT fetch failed: %s", e)
        return []

    records = data.get("hits", {}).get("hits", [])
    entries = []
    for r in records:
        meta = r.get("metadata", {})
        doi  = r.get("doi", "")
        title = meta.get("title", "")
        desc  = (meta.get("description") or "")[:500]
        pub   = meta.get("publication_date", "")
        url   = r.get("links", {}).get("html", "")
        entries.append({
            "id":          doi or make_id(title),
            "title":       title,
            "description": desc,
            "published":   pub,
            "url":         url,
            "doi":         doi,
        })
    return entries


def process_zenodo_entry(entry: dict) -> dict:
    """Convert a Zenodo EHT record into a structured_ingest_v1 bundle."""
    title = entry["title"]
    desc  = entry["description"]
    return make_bundle(
        summary             = f"[EHT Data Release] {title[:200]}",
        source              = f"Zenodo EHT:{entry['doi'] or entry['id']}",
        payload_type        = "eht/data_release",
        domain_tags         = ["eht", "galactic_center", "data_release"],
        significance        = 0.92,
        entities            = ["EHT", "Sgr A*"],
        supports_hypothesis = None,
        structured_evidence = {
            "title":       title,
            "description": desc,
            "doi":         entry.get("doi", ""),
            "published":   entry.get("published", ""),
            "tier":        "A",
        },
        url = entry["url"],
    )


# ── ATel RSS Fetcher ───────────────────────────────────────────────────────────

def fetch_atel_rss(timeout: int = HTTP_TIMEOUT) -> list[dict]:
    """Fetch ATel RSS feed and filter for GC-relevant telegrams."""
    try:
        req = urllib.request.Request(
            ATEL_RSS_URL,
            headers={"User-Agent": "Manatuabon/1.0"}
        )
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            data = resp.read().decode("utf-8", errors="replace")
    except Exception as e:
        log.warning("ATel RSS fetch failed: %s", e)
        return []

    entries = []
    try:
        root = ET.fromstring(data)
        channel = root.find("channel")
        if channel is None:
            return []
        for item in channel.findall("item"):
            title   = (item.findtext("title") or "").strip()
            desc    = (item.findtext("description") or "").strip()
            link    = (item.findtext("link") or "").strip()
            pub     = (item.findtext("pubDate") or "").strip()
            text    = f"{title} {desc}".lower()
            if any(kw in text for kw in ATEL_KEYWORDS):
                entries.append({
                    "id":          make_id(link or title),
                    "title":       title,
                    "description": desc[:500],
                    "url":         link,
                    "published":   pub,
                })
    except ET.ParseError as e:
        log.warning("ATel RSS parse error: %s", e)
    return entries


def process_atel_entry(entry: dict) -> dict:
    """Convert an ATel telegram into a structured_ingest_v1 bundle."""
    title = entry["title"]
    desc  = entry["description"]
    text  = f"{title} {desc}".lower()

    sig = 0.80
    if "x-ray" in text or "ir flare" in text or "near-infrared" in text:
        sig = 0.85
    if "sgr a*" in text or "sagittarius a*" in text:
        sig = min(sig + 0.05, 0.95)

    domain_tags = ["galactic_center", "transient_alert"]
    if "flare" in text:
        domain_tags.append("flare")
    if "x-ray" in text:
        domain_tags.append("x_ray")
    if "infrared" in text or "near-ir" in text:
        domain_tags.append("infrared")

    return make_bundle(
        summary             = f"[ATel] {title[:200]}",
        source              = f"ATel:{entry['url'] or entry['id']}",
        payload_type        = "transient/atel_alert",
        domain_tags         = domain_tags,
        significance        = sig,
        entities            = ["Sgr A*"],
        supports_hypothesis = None,
        structured_evidence = {
            "title":       title,
            "description": desc,
            "published":   entry["published"],
            "tier":        "B",
        },
        url = entry["url"],
    )


# ── Core Monitor ───────────────────────────────────────────────────────────────

class GalacticCenterMonitor:
    """
    Polls arXiv, Zenodo EHT, and ATel for new Galactic Center / EHT data
    and drops structured_ingest_v1 bundles into the Manatuabon inbox.

    Parameters
    ----------
    inbox_path   : path to the inbox directory
    seen_path    : path to the seen-IDs JSON file (de-duplication)
    dry_run      : if True, don't write any files
    """

    def __init__(
        self,
        inbox_path: Path = INBOX_PATH,
        seen_path:  Path = SEEN_PATH,
        dry_run:    bool = False,
    ):
        self.inbox    = inbox_path
        self.seen_path = seen_path
        self.dry_run  = dry_run
        self._seen    = load_seen(seen_path)

    def _drop(self, bundle: dict, item_id: str, prefix: str) -> bool:
        """Drop bundle if item_id not already seen. Returns True if dropped."""
        if item_id in self._seen:
            return False
        if not self.dry_run:
            self.inbox.mkdir(parents=True, exist_ok=True)
            out = drop_bundle(bundle, self.inbox, prefix=prefix)
            log.info("[inbox] %s -> %s", prefix, out.name)
        else:
            log.info("[inbox] [dry-run] Would drop: %s / %s", prefix, item_id)
        self._seen.add(item_id)
        return True

    def _save_seen(self):
        if not self.dry_run:
            save_seen(self.seen_path, self._seen)

    def run_arxiv(self) -> int:
        dropped = 0
        for q in ARXIV_QUERIES:
            entries = fetch_arxiv(q)
            for e in entries:
                if not is_recent(e["published"]):
                    continue
                bundle = process_arxiv_entry(e)
                if self._drop(bundle, e["id"], prefix="arxiv_gc"):
                    dropped += 1
        return dropped

    def run_zenodo(self) -> int:
        dropped = 0
        for e in fetch_zenodo_eht():
            bundle = process_zenodo_entry(e)
            if self._drop(bundle, e["id"], prefix="zenodo_eht"):
                dropped += 1
        return dropped

    def run_atel(self) -> int:
        dropped = 0
        for e in fetch_atel_rss():
            bundle = process_atel_entry(e)
            if self._drop(bundle, e["id"], prefix="atel_gc"):
                dropped += 1
        return dropped

    def run_eht_seed(self) -> int:
        dropped = 0
        for b in EHT_REFERENCE_BUNDLES:
            if b["id"] not in self._seen:
                bundle = make_bundle(
                    summary             = b["summary"],
                    source              = "EHT Collaboration / GRAVITY Collaboration",
                    payload_type        = b["payload_type"],
                    domain_tags         = b["domain_tags"],
                    significance        = b["significance"],
                    entities            = b["entities"],
                    supports_hypothesis = None,
                    structured_evidence = b["structured_evidence"],
                )
                if self._drop(bundle, b["id"], prefix="eht_seed"):
                    dropped += 1
        return dropped

    def run_once(self, sources: list[str] | None = None) -> dict:
        """
        Run one full monitoring pass.
        sources: list of 'arxiv', 'zenodo', 'atel', 'eht_seed' (all if None)
        """
        sources = sources or ["eht_seed", "arxiv", "zenodo", "atel"]
        results: dict[str, int] = {}

        if "eht_seed" in sources:
            results["eht_seed"] = self.run_eht_seed()
        if "arxiv" in sources:
            results["arxiv"] = self.run_arxiv()
        if "zenodo" in sources:
            results["zenodo"] = self.run_zenodo()
        if "atel" in sources:
            results["atel"] = self.run_atel()

        self._save_seen()
        total = sum(results.values())
        log.info("GC monitor pass complete — %d new bundle(s) dropped %s", total, results)
        return results

    def loop(self, interval_sec: int = POLL_INTERVAL_SEC):
        log.info("[gc_monitor] started (interval=%ds, dry_run=%s)", interval_sec, self.dry_run)
        while True:
            try:
                self.run_once()
            except Exception as e:
                log.error("GC monitor loop error: %s", e, exc_info=True)
            time.sleep(interval_sec)


# ── CLI ────────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Manatuabon Galactic Center Monitor + EHT Ingest")
    parser.add_argument("--inbox",      default=str(INBOX_PATH))
    parser.add_argument("--seen",       default=str(SEEN_PATH))
    parser.add_argument("--interval",   type=int, default=POLL_INTERVAL_SEC)
    parser.add_argument("--once",       action="store_true")
    parser.add_argument("--dry-run",    action="store_true")
    parser.add_argument("--eht-seed",   action="store_true",
                        help="Only inject hardcoded EHT reference bundles, then exit")
    parser.add_argument("--sources",    default="eht_seed,arxiv,zenodo,atel",
                        help="Comma-separated list of sources to poll")
    parser.add_argument("--verbose",    action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s [gc_monitor] %(levelname)s  %(message)s",
        datefmt="%H:%M:%S",
    )

    monitor = GalacticCenterMonitor(
        inbox_path = Path(args.inbox),
        seen_path  = Path(args.seen),
        dry_run    = args.dry_run,
    )

    if args.eht_seed:
        n = monitor.run_eht_seed()
        monitor._save_seen()
        print(f"EHT seed: {n} bundle(s) dropped.")
        return

    sources = [s.strip() for s in args.sources.split(",")]

    if args.once:
        results = monitor.run_once(sources=sources)
        print(json.dumps(results, indent=2))
    else:
        monitor.loop(interval_sec=args.interval)


if __name__ == "__main__":
    main()
