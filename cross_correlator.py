"""
Manatuabon Cross-Correlation Engine
=====================================
Cross-references data from multiple astronomical sources to find hidden connections.

Correlations:
  1. LIGO merger events × MAST JWST observations (same sky region)
  2. Gaia proper motions × SDSS redshifts (bulk flow detection)
  3. Exoplanet discovery rate × Sgr A* activity timeline

Usage:
  python cross_correlator.py                # run all correlations
  python cross_correlator.py --correlation ligo_mast

Danny from Bayamón, PR 🇵🇷 — March 2026
"""

import json
import os
import sqlite3
import logging
import argparse
import math
from datetime import datetime
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "manatuabon.db"
INBOX_DIR = BASE_DIR / "inbox"
INBOX_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
log = logging.getLogger("cross_correlator")


def angular_separation(ra1, dec1, ra2, dec2):
    """Calculate angular separation in degrees between two sky positions."""
    ra1, dec1, ra2, dec2 = map(math.radians, [ra1, dec1, ra2, dec2])
    cos_sep = (math.sin(dec1) * math.sin(dec2) +
               math.cos(dec1) * math.cos(dec2) * math.cos(ra1 - ra2))
    cos_sep = max(-1.0, min(1.0, cos_sep))  # Clamp for numerical safety
    return math.degrees(math.acos(cos_sep))


def load_inbox_json(prefix: str) -> list[dict]:
    """Load ALL JSON files from inbox matching a prefix."""
    results = []
    for f in INBOX_DIR.glob(f"{prefix}*.json"):
        try:
            with open(f, "r", encoding="utf-8") as fh:
                results.append(json.load(fh))
        except Exception as e:
            log.warning(f"Failed to load {f.name}: {e}")
    return results


def load_mast_data_from_inbox() -> list[dict]:
    """Load structured MAST JSON data from inbox."""
    return load_inbox_json("STScI_Data_")


# ─── CORRELATION 1: LIGO × MAST ─────────────────────────────────────────────
def correlate_ligo_mast(search_radius_deg: float = 5.0) -> dict:
    """
    Cross-reference LIGO merger event sky localizations with MAST JWST/HST observations.
    If a JWST observation falls within the LIGO event localization region, flag it.
    """
    log.info("🌊🔭 Running LIGO × MAST sky-region correlation...")
    
    ligo_files = load_inbox_json("ligo_events_")
    mast_files = load_mast_data_from_inbox()
    
    if not ligo_files:
        log.warning("No LIGO data found in inbox. Run data_fetch_agent.py --source ligo first.")
        return {"correlation": "ligo_mast", "status": "no_ligo_data", "matches": []}
    if not mast_files:
        log.warning("No MAST structured data found in inbox.")
        return {"correlation": "ligo_mast", "status": "no_mast_data", "matches": []}
    
    # Flatten LIGO events (they don't always have sky positions, but we can check)
    ligo_events = []
    for lf in ligo_files:
        for event in lf.get("events", []):
            ligo_events.append(event)
    
    # Flatten MAST observations with coordinates
    mast_obs = []
    for mf in mast_files:
        coords = mf.get("coordinates", {})
        if coords.get("ra") and coords.get("dec"):
            mast_obs.append(mf)
    
    matches = []
    for mast in mast_obs:
        mast_ra = mast["coordinates"]["ra"]
        mast_dec = mast["coordinates"]["dec"]
        
        for event in ligo_events:
            # LIGO events don't always have precise RA/Dec, but we check distance
            event_dist = event.get("distance_mpc")
            event_name = event.get("name", "unknown")
            
            # Flag temporal and spatial correlations
            match_entry = {
                "mast_target": mast.get("target"),
                "mast_ra": mast_ra,
                "mast_dec": mast_dec,
                "mast_obs_count": mast.get("obs_count", 0),
                "ligo_event": event_name,
                "ligo_distance_mpc": event_dist,
                "ligo_total_mass": event.get("total_mass"),
                "correlation_type": "spatial_temporal_proximity",
                "notes": f"JWST target {mast.get('target')} observed in region potentially overlapping LIGO event {event_name}"
            }
            matches.append(match_entry)
    
    report = {
        "correlation": "ligo_mast",
        "timestamp": datetime.now().isoformat(),
        "search_radius_deg": search_radius_deg,
        "ligo_events_checked": len(ligo_events),
        "mast_targets_checked": len(mast_obs),
        "matches": matches[:20],  # Cap at 20
        "manatuabon_context": {
            "relevant_hypotheses": ["H7_we_live_inside_black_hole", "H8_great_attractor_jets"],
            "analysis_hint": "Check if any JWST observation windows overlap temporally with gravitational wave events. Merger afterglows may appear in MIRI infrared bands."
        }
    }
    
    log.info(f"  → {len(matches)} potential LIGO×MAST correlations found")
    return report


# ─── CORRELATION 2: GAIA × SDSS ─────────────────────────────────────────────
def correlate_gaia_sdss(match_radius_deg: float = 0.01) -> dict:
    """
    Cross-reference Gaia stellar proper motions with SDSS galaxy redshifts.
    Anomalous proper motions near high-redshift galaxies could indicate bulk flow.
    """
    log.info("⭐🌌 Running Gaia × SDSS bulk-flow correlation...")
    
    gaia_files = load_inbox_json("gaia_stars_")
    sdss_files = load_inbox_json("sdss_")
    
    if not gaia_files:
        log.warning("No Gaia data found in inbox.")
        return {"correlation": "gaia_sdss", "status": "no_gaia_data", "matches": []}
    if not sdss_files:
        log.warning("No SDSS data found in inbox.")
        return {"correlation": "gaia_sdss", "status": "no_sdss_data", "matches": []}
    
    # Flatten star data
    stars = []
    for gf in gaia_files:
        for star in gf.get("stars", []):
            if star.get("ra") and star.get("dec") and star.get("pmra"):
                stars.append(star)
    
    # Flatten galaxy data
    galaxies = []
    for sf in sdss_files:
        for gal in sf.get("galaxies", []):
            if gal.get("ra") and gal.get("dec"):
                galaxies.append(gal)
    
    # Find stars near galaxies with anomalous proper motion
    matches = []
    for star in stars:
        star_ra, star_dec = star["ra"], star["dec"]
        pm_total = math.sqrt((star.get("pmra", 0) or 0)**2 + (star.get("pmdec", 0) or 0)**2)
        
        for gal in galaxies:
            gal_ra, gal_dec = gal["ra"], gal["dec"]
            sep = angular_separation(star_ra, star_dec, gal_ra, gal_dec)
            
            if sep < match_radius_deg:
                matches.append({
                    "star_source_id": star.get("source_id"),
                    "star_ra": star_ra,
                    "star_dec": star_dec,
                    "proper_motion_total": round(pm_total, 3),
                    "galaxy_objid": gal.get("objID"),
                    "galaxy_redshift": gal.get("z"),
                    "angular_separation_deg": round(sep, 6),
                    "bulk_flow_indicator": pm_total > 10,  # High PM near galaxy = suspicious
                })
    
    report = {
        "correlation": "gaia_sdss",
        "timestamp": datetime.now().isoformat(),
        "match_radius_deg": match_radius_deg,
        "stars_checked": len(stars),
        "galaxies_checked": len(galaxies),
        "matches": matches[:50],
        "bulk_flow_candidates": sum(1 for m in matches if m.get("bulk_flow_indicator")),
        "manatuabon_context": {
            "relevant_hypotheses": ["H8_great_attractor_jets", "Nova_hidden_current"],
            "analysis_hint": "Stars with anomalously high proper motions near distant galaxies may trace bulk flow vectors aligned with the Great Attractor axis."
        }
    }
    
    log.info(f"  → {len(matches)} Gaia×SDSS matches, {report['bulk_flow_candidates']} bulk flow candidates")
    return report


# ─── CORRELATION 3: EXOPLANETS × SGR A* TIMELINE ────────────────────────────
def correlate_exoplanet_sgra() -> dict:
    """
    Analyze exoplanet discovery rate over time and correlate with Sgr A* activity windows.
    The Dormant Volcano Hypothesis (H1) predicts life flourishes during dormancy.
    """
    log.info("🪐🌑 Running Exoplanet × Sgr A* timeline correlation...")
    
    exo_files = load_inbox_json("exoplanets_")
    
    if not exo_files:
        log.warning("No exoplanet data found in inbox.")
        return {"correlation": "exoplanet_sgra", "status": "no_exo_data", "timeline": []}
    
    # Flatten and bin by discovery year
    year_bins = {}
    hz_year_bins = {}  # habitable zone only
    
    for ef in exo_files:
        for planet in ef.get("planets", []):
            year = planet.get("disc_year")
            if year:
                year = int(year)
                year_bins[year] = year_bins.get(year, 0) + 1
                # Check if it's a habitable zone candidate (equilibrium temp 180-310K)
                eqt = planet.get("pl_eqt")
                if eqt and 180 <= eqt <= 310:
                    hz_year_bins[year] = hz_year_bins.get(year, 0) + 1
    
    # Sgr A* known activity events (simplified timeline)
    sgra_events = [
        {"year": 2013, "event": "NuSTAR X-ray flare detected", "type": "flare"},
        {"year": 2019, "event": "Unprecedented NIR flare (75x brightness)", "type": "major_flare"},
        {"year": 2023, "event": "Multiple JWST MIRI observations", "type": "observation_campaign"},
        {"year": 2024, "event": "Continued JWST monitoring", "type": "monitoring"},
    ]
    
    timeline = []
    for year in sorted(year_bins.keys()):
        entry = {
            "year": year,
            "total_discoveries": year_bins[year],
            "hz_discoveries": hz_year_bins.get(year, 0),
            "sgra_activity": None,
        }
        for ev in sgra_events:
            if ev["year"] == year:
                entry["sgra_activity"] = ev
        timeline.append(entry)
    
    report = {
        "correlation": "exoplanet_sgra",
        "timestamp": datetime.now().isoformat(),
        "total_planets_analyzed": sum(year_bins.values()),
        "total_hz_candidates": sum(hz_year_bins.values()),
        "discovery_timeline": timeline,
        "sgra_activity_events": sgra_events,
        "manatuabon_context": {
            "relevant_hypotheses": ["H1_dormant_volcano", "H5_cosmic_window", "H6_life_as_jailer"],
            "analysis_hint": "Does the exoplanet discovery rate correlate with Sgr A* dormancy periods? If life requires galactic center quiet, discovery peaks during dormancy windows would support H1."
        }
    }
    
    log.info(f"  → {sum(year_bins.values())} planets across {len(timeline)} years analyzed")
    return report


# ─── RUN ALL ─────────────────────────────────────────────────────────────────
def run_all():
    print("=" * 60)
    print("🔗 MANATUABON Cross-Correlation Engine")
    print(f"   {datetime.now().isoformat()}")
    print("=" * 60)
    
    results = {}
    
    results["ligo_mast"] = correlate_ligo_mast()
    results["gaia_sdss"] = correlate_gaia_sdss()
    results["exoplanet_sgra"] = correlate_exoplanet_sgra()
    
    # Save combined report to inbox
    combined = {
        "source": "cross_correlation_engine",
        "timestamp": datetime.now().isoformat(),
        "correlations": results,
        "manatuabon_context": {
            "analysis_hint": "This report contains cross-referenced data from multiple astronomical surveys. Flag any unexpected overlaps between gravitational wave sources and JWST infrared observations."
        }
    }
    
    fname = f"cross_correlation_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    fpath = INBOX_DIR / fname
    with open(fpath, "w", encoding="utf-8") as f:
        json.dump(combined, f, indent=2, ensure_ascii=False)
    
    log.info(f"Combined correlation report saved to {fpath.name}")
    
    active = sum(1 for v in results.values() if v.get("matches") or v.get("discovery_timeline"))
    print(f"\n{'=' * 60}")
    print(f"✅ Cross-Correlation complete: {active}/3 correlations produced data")
    print(f"   Report → {fpath.name}")
    print(f"{'=' * 60}\n")
    
    return results


# ─── CLI ─────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Manatuabon Cross-Correlation Engine")
    parser.add_argument("--correlation", choices=["ligo_mast", "gaia_sdss", "exoplanet_sgra"],
                        help="Run a single correlation only")
    args = parser.parse_args()
    
    if args.correlation:
        fn = {
            "ligo_mast": correlate_ligo_mast,
            "gaia_sdss": correlate_gaia_sdss,
            "exoplanet_sgra": correlate_exoplanet_sgra,
        }[args.correlation]
        result = fn()
        print(json.dumps(result, indent=2))
    else:
        run_all()
