"""
Manatuabon DataFetchAgent
=========================
Pulls real astrophysics data from:
  - LIGO Open Science Center (gravitational wave events)
  - arXiv API (latest Sgr A* / astrophysics papers)
  - SDSS SkyServer (galaxy survey data)
  - ESA Gaia Archive (stellar positions near Sgr A*)

Drops structured JSON files into D:\\Manatuabon\\inbox\\
WatcherAgent picks them up automatically (~3 seconds)

Usage:
  python data_fetch_agent.py              # run all sources once
  python data_fetch_agent.py --schedule   # run daily at 6am
  python data_fetch_agent.py --source ligo
  python data_fetch_agent.py --source arxiv
  python data_fetch_agent.py --source sdss
  python data_fetch_agent.py --source gaia
"""

import requests
import json
import time
import argparse
import schedule
import os
from datetime import datetime, timezone
from pathlib import Path

# ── Config ──────────────────────────────────────────────────────────────────
INBOX_DIR = Path(r"D:\Manatuabon\inbox")
INBOX_DIR.mkdir(parents=True, exist_ok=True)

HEADERS = {"User-Agent": "Manatuabon/1.0 (astrophysics research; contact@manatuabon.local)"}

def timestamp():
    return datetime.now(timezone.utc).isoformat()

def save_to_inbox(filename: str, data: dict):
    """Save structured JSON to inbox for WatcherAgent to pick up."""
    path = INBOX_DIR / filename
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    print(f"  ✅ Saved → {path}")
    return path


# ── 1. LIGO Open Science Center ──────────────────────────────────────────────
def fetch_ligo(max_events=10):
    """
    Fetch latest gravitational wave detection events from GWOSC.
    API docs: https://gwosc.org/apidocs/
    """
    print("\n🌊 Fetching LIGO gravitational wave events...")
    
    url = "https://gwosc.org/eventapi/json/allevents/"
    
    try:
        resp = requests.get(url, headers=HEADERS, timeout=30)
        resp.raise_for_status()
        raw = resp.json()
        
        events = []
        # GWOSC returns dict keyed by event name
        event_dict = raw.get("events", raw)
        
        for name, info in list(event_dict.items())[:max_events]:
            event = {
                "name": name,
                "gps_time": info.get("GPS", None),
                "mass_1": info.get("mass_1_source", None),
                "mass_2": info.get("mass_2_source", None),
                "total_mass": info.get("total_mass_source", None),
                "chirp_mass": info.get("chirp_mass", None),
                "distance_mpc": info.get("luminosity_distance", None),
                "redshift": info.get("redshift", None),
                "snr": info.get("network_matched_filter_snr", None),
                "false_alarm_rate": info.get("far", None),
                "detector": info.get("strain", {}).get("detector", "unknown"),
                "url": f"https://gwosc.org/events/{name}/",
            }
            events.append(event)
        
        payload = {
            "source": "LIGO_GWOSC",
            "fetched_at": timestamp(),
            "type": "gravitational_wave_events",
            "count": len(events),
            "events": events,
            "manatuabon_context": {
                "relevant_hypotheses": ["H7_we_live_inside_black_hole", "H8_great_attractor_jets", "H2_bad_eater"],
                "analysis_hint": "Cross-reference merger masses and distances with Sgr A* dormancy window and Great Attractor jet axis alignment"
            }
        }
        
        fname = f"ligo_events_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        save_to_inbox(fname, payload)
        print(f"  → {len(events)} events fetched")
        return payload
        
    except Exception as e:
        print(f"  ❌ LIGO fetch failed: {e}")
        return None


# ── 2. arXiv API ─────────────────────────────────────────────────────────────
def fetch_arxiv(queries=None, max_results=5):
    """
    Fetch latest astrophysics papers from arXiv.
    API docs: https://arxiv.org/help/api/
    Searches: Sgr A*, Great Attractor, black hole information paradox
    """
    print("\n📄 Fetching arXiv papers...")
    
    if queries is None:
        queries = [
            "Sagittarius A* accretion",
            "Great Attractor dark flow",
            "black hole information paradox 2025",
            "gravitational wave merger galaxy",
        ]
    
    base_url = "http://export.arxiv.org/api/query"
    all_papers = []
    
    for query in queries:
        print(f"  Searching: '{query}'")
        params = {
            "search_query": f"all:{query}",
            "start": 0,
            "max_results": max_results,
            "sortBy": "submittedDate",
            "sortOrder": "descending",
        }
        
        try:
            resp = requests.get(base_url, params=params, headers=HEADERS, timeout=30)
            resp.raise_for_status()
            
            # arXiv returns Atom XML — parse it simply
            text = resp.text
            papers = parse_arxiv_xml(text, query)
            all_papers.extend(papers)
            print(f"    → {len(papers)} papers found")
            time.sleep(3)  # arXiv rate limit: be respectful
            
        except Exception as e:
            print(f"  ❌ arXiv query '{query}' failed: {e}")
    
    if not all_papers:
        return None
    
    payload = {
        "source": "arXiv",
        "fetched_at": timestamp(),
        "type": "research_papers",
        "queries": queries,
        "count": len(all_papers),
        "papers": all_papers,
        "manatuabon_context": {
            "relevant_hypotheses": ["H1_dormant_volcano", "H2_bad_eater", "H4_observer_collapse", "H8_great_attractor_jets"],
            "analysis_hint": "Extract measurements, findings, and contradictions with founding hypotheses. Flag any Sgr A* flare activity or new accretion rate measurements."
        }
    }
    
    fname = f"arxiv_papers_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    save_to_inbox(fname, payload)
    return payload


def parse_arxiv_xml(xml_text: str, query: str) -> list:
    """Simple XML parser for arXiv Atom feed — no external dependencies."""
    import re
    papers = []
    
    # Split into entries
    entries = xml_text.split("<entry>")[1:]
    
    for entry in entries:
        def extract(tag):
            m = re.search(rf"<{tag}[^>]*>(.*?)</{tag}>", entry, re.DOTALL)
            return m.group(1).strip() if m else None
        
        # Get authors
        authors = re.findall(r"<name>(.*?)</name>", entry)
        
        # Get arxiv ID
        id_raw = extract("id") or ""
        arxiv_id = id_raw.split("/abs/")[-1].strip()
        
        paper = {
            "arxiv_id": arxiv_id,
            "title": extract("title"),
            "abstract": extract("summary"),
            "authors": authors[:5],  # first 5 authors
            "published": extract("published"),
            "updated": extract("updated"),
            "url": f"https://arxiv.org/abs/{arxiv_id}",
            "pdf_url": f"https://arxiv.org/pdf/{arxiv_id}",
            "search_query": query,
        }
        papers.append(paper)
    
    return papers


# ── 3. SDSS SkyServer ─────────────────────────────────────────────────────────
def fetch_sdss(ra_center=266.4168, dec_center=-29.0078, radius_arcmin=60, max_results=50):
    """
    Query SDSS DR18 for galaxies near Sgr A* direction.
    Default center: RA=266.4168, Dec=-29.0078 (Sgr A*)
    API docs: https://skyserver.sdss.org/dr18/SkyServerWS/
    """
    print("\n🌌 Fetching SDSS galaxy data...")
    
    # SQL query for galaxies in cone search
    sql = f"""
    SELECT TOP {max_results}
        objID, ra, dec, z, zErr,
        petroMag_r, petroMag_g,
        type, subClass,
        velDisp, velDispErr
    FROM PhotoObj
    WHERE
        ra BETWEEN {ra_center - radius_arcmin/60} AND {ra_center + radius_arcmin/60}
        AND dec BETWEEN {dec_center - radius_arcmin/60} AND {dec_center + radius_arcmin/60}
        AND type = 3
    ORDER BY petroMag_r ASC
    """.strip()
    
    url = "https://skyserver.sdss.org/dr18/SkyServerWS/SearchTools/SqlSearch"
    params = {"cmd": sql, "format": "json"}
    
    try:
        resp = requests.get(url, params=params, headers=HEADERS, timeout=60)
        resp.raise_for_status()
        data = resp.json()
        
        # SDSS returns list of result tables
        rows = []
        if isinstance(data, list) and len(data) > 0:
            table = data[0]
            rows = table.get("Rows", [])
        
        payload = {
            "source": "SDSS_DR18",
            "fetched_at": timestamp(),
            "type": "galaxy_survey",
            "query_center": {"ra": ra_center, "dec": dec_center},
            "query_radius_arcmin": radius_arcmin,
            "count": len(rows),
            "galaxies": rows,
            "manatuabon_context": {
                "relevant_hypotheses": ["H8_great_attractor_jets", "Nova_hidden_current", "H10_inflation_scar"],
                "analysis_hint": "Analyze redshift distribution and velocity dispersion for bulk flow signatures. Cross-reference with Great Attractor jet axis predictions."
            }
        }
        
        fname = f"sdss_galaxies_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        save_to_inbox(fname, payload)
        print(f"  → {len(rows)} galaxies fetched near Sgr A* direction")
        return payload
        
    except Exception as e:
        print(f"  ❌ SDSS fetch failed: {e}")
        # Try alternate endpoint
        return fetch_sdss_cone(ra_center, dec_center, radius_arcmin, max_results)


def fetch_sdss_cone(ra, dec, radius_arcmin, max_results):
    """Fallback: SDSS cone search endpoint."""
    print("  Trying SDSS cone search fallback...")
    url = "https://skyserver.sdss.org/dr18/SkyServerWS/SearchTools/RadialSearch"
    params = {
        "ra": ra, "dec": dec,
        "radius": radius_arcmin / 60,  # degrees
        "limit": max_results,
        "format": "json"
    }
    try:
        resp = requests.get(url, params=params, headers=HEADERS, timeout=60)
        resp.raise_for_status()
        data = resp.json()
        rows = data[0].get("Rows", []) if isinstance(data, list) else []
        
        payload = {
            "source": "SDSS_DR18_cone",
            "fetched_at": timestamp(),
            "type": "galaxy_survey",
            "count": len(rows),
            "galaxies": rows,
            "manatuabon_context": {
                "relevant_hypotheses": ["H8_great_attractor_jets", "Nova_hidden_current"],
                "analysis_hint": "Bulk flow analysis near Sgr A* direction"
            }
        }
        fname = f"sdss_cone_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        save_to_inbox(fname, payload)
        print(f"  → {len(rows)} objects via cone search")
        return payload
    except Exception as e:
        print(f"  ❌ SDSS cone fallback failed: {e}")
        return None


# ── 4. ESA Gaia Archive ───────────────────────────────────────────────────────
def fetch_gaia(ra_center=266.4168, dec_center=-29.0078, radius_deg=0.5, max_results=100):
    """
    Query ESA Gaia DR3 for stars near Sgr A*.
    Uses ADQL via TAP (Table Access Protocol).
    API docs: https://gea.esac.esa.int/tap-server/tap/
    """
    print("\n⭐ Fetching Gaia stellar data near Sgr A*...")
    
    # ADQL query — stars near Sgr A* with proper motion data
    adql = f"""
    SELECT TOP {max_results}
        source_id, ra, dec,
        parallax, parallax_error,
        pmra, pmra_error,
        pmdec, pmdec_error,
        radial_velocity, radial_velocity_error,
        phot_g_mean_mag,
        distance_gspphot
    FROM gaiadr3.gaia_source
    WHERE CONTAINS(
        POINT('ICRS', ra, dec),
        CIRCLE('ICRS', {ra_center}, {dec_center}, {radius_deg})
    ) = 1
    AND parallax IS NOT NULL
    AND pmra IS NOT NULL
    ORDER BY phot_g_mean_mag ASC
    """.strip()
    
    url = "https://gea.esac.esa.int/tap-server/tap/sync"
    params = {
        "REQUEST": "doQuery",
        "LANG": "ADQL",
        "FORMAT": "json",
        "QUERY": adql,
    }
    
    try:
        resp = requests.get(url, params=params, headers=HEADERS, timeout=60)
        resp.raise_for_status()
        data = resp.json()
        
        # Gaia TAP returns {metadata: [...], data: [...]}
        columns = [col["name"] for col in data.get("metadata", [])]
        rows_raw = data.get("data", [])
        
        # Convert to list of dicts
        stars = [dict(zip(columns, row)) for row in rows_raw]
        
        payload = {
            "source": "ESA_Gaia_DR3",
            "fetched_at": timestamp(),
            "type": "stellar_catalog",
            "query_center": {"ra": ra_center, "dec": dec_center},
            "query_radius_deg": radius_deg,
            "count": len(stars),
            "stars": stars,
            "manatuabon_context": {
                "relevant_hypotheses": ["H1_dormant_volcano", "H2_bad_eater", "H5_cosmic_window"],
                "analysis_hint": "Analyze proper motions for stellar stream signatures. Cross-reference with Sgr A* gravitational influence radius. Look for anomalous velocities suggesting past Sgr A* activity."
            }
        }
        
        fname = f"gaia_stars_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        save_to_inbox(fname, payload)
        print(f"  → {len(stars)} stars fetched near Sgr A*")
        return payload
        
    except Exception as e:
        print(f"  ❌ Gaia fetch failed: {e}")
        return None


# ── 5. NASA Exoplanet Archive (bonus) ────────────────────────────────────────
def fetch_exoplanets(max_results=50):
    """
    Fetch confirmed exoplanets from NASA Exoplanet Archive.
    Relevant to H5 (Cosmic Window) — how many worlds in the habitable window?
    API docs: https://exoplanetarchive.ipac.caltech.edu/docs/TAP/usingTAP.html
    """
    print("\n🪐 Fetching NASA Exoplanet data...")
    
    url = "https://exoplanetarchive.ipac.caltech.edu/TAP/sync"
    
    # Query habitable zone candidates
    adql = f"""
    SELECT TOP {max_results}
        pl_name, hostname,
        pl_orbper, pl_rade, pl_bmasse,
        pl_eqt, st_teff, st_rad, st_mass,
        sy_dist, sy_gaiamag,
        pl_controv_flag, discoverymethod, disc_year
    FROM ps
    WHERE pl_eqt BETWEEN 180 AND 310
    AND pl_rade < 2.5
    AND pl_controv_flag = 0
    ORDER BY disc_year DESC
    """.strip()
    
    params = {
        "REQUEST": "doQuery",
        "LANG": "ADQL",
        "FORMAT": "json",
        "QUERY": adql,
    }
    
    try:
        resp = requests.get(url, params=params, headers=HEADERS, timeout=60)
        resp.raise_for_status()
        data = resp.json()
        
        columns = [col["name"] for col in data.get("metadata", [])]
        rows = [dict(zip(columns, row)) for row in data.get("data", [])]
        
        payload = {
            "source": "NASA_Exoplanet_Archive",
            "fetched_at": timestamp(),
            "type": "exoplanet_catalog",
            "filter": "habitable_zone_candidates",
            "count": len(rows),
            "planets": rows,
            "manatuabon_context": {
                "relevant_hypotheses": ["H5_cosmic_window", "H6_life_as_jailer", "Claude_silence_paradox"],
                "analysis_hint": "How many potentially habitable worlds exist in our galaxy during Sgr A* dormancy? Cross-reference discovery years with Fermi Bubbles timeline."
            }
        }
        
        fname = f"exoplanets_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        save_to_inbox(fname, payload)
        print(f"  → {len(rows)} habitable zone candidates fetched")
        return payload
        
    except Exception as e:
        print(f"  ❌ Exoplanet fetch failed: {e}")
        return None


# ── 6. Swift BAT Transient Monitor ──────────────────────────────────────────
def fetch_swift_bat():
    """
    Scrape NASA Swift BAT hard X-ray transient monitor for recent detections.
    Source: https://swift.gsfc.nasa.gov/results/transients/
    Swift BAT monitors the sky in 15-195 keV — perfect for detecting Sgr A* flares.
    """
    print("\n⚡ Fetching Swift BAT transient monitor data...")
    
    url = "https://swift.gsfc.nasa.gov/results/transients/"
    
    try:
        resp = requests.get(url, headers=HEADERS, timeout=30)
        resp.raise_for_status()
        html = resp.text
        
        # Parse source table from the HTML page
        # Swift BAT pages list sources with their detection significance
        import re
        
        # Extract table rows — Swift BAT lists source names and fluxes
        sources = []
        
        # Look for links to individual source pages
        source_links = re.findall(r'href="([^"]*?)/"[^>]*>\s*([^<]+)', html)
        for link, name in source_links[:30]:
            name = name.strip()
            if len(name) > 1 and not name.startswith(('.', '#', '?')):
                source = {
                    "name": name,
                    "url": f"https://swift.gsfc.nasa.gov/results/transients/{link}/",
                    "energy_band": "15-195 keV (hard X-ray)",
                    "instrument": "Swift BAT",
                }
                # Flag Sgr A* and known targets of interest
                name_lower = name.lower()
                if any(kw in name_lower for kw in ['sgr', 'grb', 'crab', 'cyg', 'v404', 'maxi', 'swift']):
                    source["priority"] = "high"
                else:
                    source["priority"] = "normal"
                sources.append(source)
        
        payload = {
            "source": "Swift_BAT",
            "fetched_at": timestamp(),
            "type": "xray_transient_monitor",
            "monitor_url": url,
            "energy_band": "15-195 keV",
            "count": len(sources),
            "transient_sources": sources,
            "manatuabon_context": {
                "relevant_hypotheses": ["H1_dormant_volcano", "H2_bad_eater", "H9_pulse_code"],
                "analysis_hint": "Monitor for Sgr A* X-ray flare activity. Sudden BAT detections near galactic center coordinates (RA~266.4, Dec~-29.0) would indicate Sgr A* awakening from dormancy — directly testing H1."
            }
        }
        
        fname = f"swift_bat_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        save_to_inbox(fname, payload)
        print(f"  → {len(sources)} transient sources cataloged")
        return payload
        
    except Exception as e:
        print(f"  ❌ Swift BAT fetch failed: {e}")
        return None


# ── 7. Fermi LAT All-Sky Monitor ────────────────────────────────────────────
def fetch_fermi_lat():
    """
    Fetch Fermi LAT gamma-ray source catalog data.
    Source: https://fermi.gsfc.nasa.gov/ssc/data/access/lat/
    Fermi LAT covers 20 MeV - 300 GeV — the highest energy photons in the universe.
    """
    print("\n🔥 Fetching Fermi LAT gamma-ray data...")
    
    # Use the Fermi LAT Light Curve Repository API for monitored sources
    # This provides flux measurements for ~1800 sources
    lcr_url = "https://fermi.gsfc.nasa.gov/ssc/data/access/lat/LightCurveRepository/queryDB.php"
    
    try:
        # Try the light curve repository query for recent bright sources
        params = {
            "typeOfRequest": "query",
            "catalogue": "4FGL",
        }
        resp = requests.get(lcr_url, params=params, headers=HEADERS, timeout=30)
        
        # If the API works, parse it; if not, scrape the main page
        sources = []
        
        if resp.status_code == 200 and len(resp.text) > 100:
            # Try JSON parsing
            try:
                data = resp.json()
                if isinstance(data, list):
                    for entry in data[:30]:
                        sources.append({
                            "name": entry.get("name", entry.get("source_name", "unknown")),
                            "ra": entry.get("ra"),
                            "dec": entry.get("dec"),
                            "flux": entry.get("flux"),
                            "variability": entry.get("variability_index"),
                        })
            except (ValueError, KeyError):
                pass
        
        # Fallback: scrape the main data access page for key info
        if not sources:
            main_resp = requests.get("https://fermi.gsfc.nasa.gov/ssc/data/access/lat/", 
                                     headers=HEADERS, timeout=30)
            main_resp.raise_for_status()
            
            import re
            # Extract any catalog references
            catalogs = re.findall(r'(4FGL[^<"]*|FL8Y[^<"]*|3FHL[^<"]*)', main_resp.text)
            sources = [{
                "catalog": cat.strip(),
                "type": "gamma_ray_catalog_reference",
                "energy_band": "20 MeV - 300 GeV",
            } for cat in set(catalogs[:20])]
        
        payload = {
            "source": "Fermi_LAT",
            "fetched_at": timestamp(),
            "type": "gamma_ray_monitor",
            "monitor_url": "https://fermi.gsfc.nasa.gov/ssc/data/access/lat/",
            "energy_band": "20 MeV - 300 GeV",
            "count": len(sources),
            "gamma_sources": sources,
            "manatuabon_context": {
                "relevant_hypotheses": ["H1_dormant_volcano", "H8_great_attractor_jets", "H9_pulse_code"],
                "analysis_hint": "Fermi Bubbles extend 25,000 light-years above and below the galactic plane — remnants of an ancient Sgr A* outburst. Any new gamma-ray activity near the galactic center tests the Dormant Volcano hypothesis directly."
            }
        }
        
        fname = f"fermi_lat_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        save_to_inbox(fname, payload)
        print(f"  → {len(sources)} gamma-ray sources/catalogs captured")
        return payload
        
    except Exception as e:
        print(f"  ❌ Fermi LAT fetch failed: {e}")
        return None


# ── 8. MAXI GSC Monitor (Sgr A*) ────────────────────────────────────────────
def fetch_maxi_sgra():
    """
    Scrape RIKEN MAXI/GSC X-ray data for Sgr A* and galactic center sources.
    MAXI is mounted on the ISS and scans the entire sky every 92 minutes!
    
    Sources:
      - Source list:    http://maxi.riken.jp/top/slist.html
      - On-demand LC:   http://maxi.riken.jp/mxondem/
      - Today's flux:   http://maxi.riken.jp/fluxtop/fluxtop.html
    """
    print("\n🛰️ Fetching MAXI GSC X-ray monitor data...")
    
    import re
    
    sources_found = []
    sgra_mentions = []
    flux_data = []
    
    # Strategy 1: Scrape the source list for galactic center sources
    slist_url = "http://maxi.riken.jp/top/slist.html"
    try:
        resp = requests.get(slist_url, headers=HEADERS, timeout=30)
        resp.raise_for_status()
        html = resp.text
        
        # Extract source IDs from star_data links (format: J1745-290, J1746-295, etc.)
        import re
        source_ids = re.findall(r'star_data/(J\d{4}[+-]\d{2,3})/', html)
        source_ids = list(dict.fromkeys(source_ids))  # Deduplicate preserving order
        
        # Galactic center sources are near RA~17h45m, Dec~-29° → J17XX-2XX to J17XX-3XX
        gc_keywords = ['J1745', 'J1746', 'J1747', 'J1748', 'J1742', 'J1750', 'J1733',
                       'J1758', 'J1714', 'J1731', 'J1732', 'J1734']
        
        for sid in source_ids:
            is_gc = any(sid.startswith(kw) for kw in gc_keywords)
            if is_gc:
                sources_found.append({
                    "name": sid,
                    "url": f"http://maxi.riken.jp/star_data/{sid}/{sid}.html",
                    "data_url": f"http://maxi.riken.jp/pubdata/v7.7l/{sid}/index.html",
                    "priority": "high",
                    "instrument": "MAXI/GSC",
                    "region": "galactic_center",
                })
        
        print(f"  → {len(sources_found)} X-ray sources from source list")
    except Exception as e:
        print(f"  ⚠ Source list scrape failed: {e}")
    
    # Strategy 2: Scrape Today's Top Flux for the brightest X-ray objects right now
    flux_url = "http://maxi.riken.jp/fluxtop/fluxtop.html"
    try:
        resp = requests.get(flux_url, headers=HEADERS, timeout=30)
        resp.raise_for_status()
        html = resp.text
        
        text_content = re.sub(r'<[^>]+>', ' ', html)
        text_content = re.sub(r'\s+', ' ', text_content).strip()
        
        # Extract numerical flux values
        numbers = re.findall(r'(\d+\.?\d*)\s*(?:c/s|mCrab|photons|cts)', text_content, re.I)
        flux_data = numbers[:15]
        
        # Check for Sgr A* mentions
        sentences = text_content.split('.')
        for s in sentences:
            if any(kw in s.lower() for kw in ['sgr a', 'sagittarius', 'galactic center', 'flare']):
                sgra_mentions.append(s.strip()[:200])
        
        print(f"  → {len(flux_data)} flux measurements from today's top sources")
    except Exception as e:
        print(f"  ⚠ Flux top scrape failed: {e}")
    
    payload = {
        "source": "MAXI_GSC_ISS",
        "fetched_at": timestamp(),
        "type": "xray_monitor_sgra",
        "target": "Sgr A* / Galactic Center",
        "instrument": "MAXI/GSC (ISS)",
        "energy_band": "2-20 keV (soft X-ray)",
        "scan_cadence": "92 minutes (full-sky)",
        "monitor_urls": {
            "source_list": slist_url,
            "todays_flux": flux_url,
            "on_demand": "http://maxi.riken.jp/mxondem/",
        },
        "galactic_center_sources": sources_found,
        "flux_values_detected": flux_data,
        "sgra_context_text": sgra_mentions,
        "source_count": len(sources_found),
        "manatuabon_context": {
            "relevant_hypotheses": ["H1_dormant_volcano", "H2_bad_eater", "H3_feedback_loop"],
            "analysis_hint": "MAXI monitors Sgr A* in soft X-rays from the ISS with 92-minute cadence. Any flux increase above baseline directly indicates Sgr A* accretion activity — the #1 metric for the Dormant Volcano hypothesis. Cross-reference with Swift BAT hard X-ray detections."
        }
    }
    
    fname = f"maxi_sgra_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    save_to_inbox(fname, payload)
    print(f"  → MAXI data captured ({len(sources_found)} gc sources, {len(flux_data)} flux values)")
    return payload


# ── Run All ───────────────────────────────────────────────────────────────────
def run_all():
    print("=" * 60)
    print("🌌 MANATUABON DataFetchAgent")
    print(f"   {timestamp()}")
    print(f"   → inbox: {INBOX_DIR}")
    print("=" * 60)
    
    results = {}
    results["ligo"]       = fetch_ligo(max_events=10)
    results["arxiv"]      = fetch_arxiv(max_results=3)
    results["sdss"]       = fetch_sdss()
    results["gaia"]       = fetch_gaia()
    results["exoplanets"] = fetch_exoplanets()
    results["swift_bat"]  = fetch_swift_bat()
    results["fermi_lat"]  = fetch_fermi_lat()
    results["maxi_sgra"]  = fetch_maxi_sgra()
    
    success = sum(1 for v in results.values() if v is not None)
    print(f"\n{'=' * 60}")
    print(f"✅ DataFetchAgent complete: {success}/8 sources fetched")
    print(f"   WatcherAgent will pick up files in ~3 seconds")
    print(f"{'=' * 60}\n")
    return results


# ── CLI ────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Manatuabon DataFetchAgent")
    parser.add_argument("--source", choices=["ligo", "arxiv", "sdss", "gaia", "exoplanets",
                                              "swift_bat", "fermi_lat", "maxi_sgra"],
                        help="Fetch from a single source only")
    parser.add_argument("--schedule", action="store_true",
                        help="Run on schedule: daily at 6am + every 6 hours for arXiv")
    args = parser.parse_args()
    
    if args.source:
        # Single source
        fn = {"ligo": fetch_ligo, "arxiv": fetch_arxiv,
              "sdss": fetch_sdss, "gaia": fetch_gaia,
              "exoplanets": fetch_exoplanets,
              "swift_bat": fetch_swift_bat, "fermi_lat": fetch_fermi_lat,
              "maxi_sgra": fetch_maxi_sgra}[args.source]
        fn()
    elif args.schedule:
        print("⏰ DataFetchAgent scheduled mode")
        print("   Full fetch: daily at 06:00")
        print("   arXiv only: every 6 hours")
        print("   Transient monitors: every 4 hours")
        print("   Press Ctrl+C to stop\n")
        
        schedule.every().day.at("06:00").do(run_all)
        schedule.every(6).hours.do(fetch_arxiv)
        schedule.every(4).hours.do(fetch_swift_bat)
        schedule.every(4).hours.do(fetch_maxi_sgra)
        
        # Run once immediately on start
        run_all()
        
        while True:
            schedule.run_pending()
            time.sleep(60)
    else:
        # Run all once
        run_all()

