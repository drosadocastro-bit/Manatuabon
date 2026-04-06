import os
import time
import json
import sqlite3
import logging
import requests
from datetime import datetime, timedelta
from astroquery.heasarc import Heasarc
from astropy.io import fits
import numpy as np
from db_init import ensure_runtime_db

# Configuration
_BASE_DIR = Path(__file__).resolve().parent
AGENT_DB = str(_BASE_DIR / "manatuabon.db")
INBOX_DIR = str(_BASE_DIR / "inbox")
POLL_INTERVAL = 14400  # 4 hours
LOG_FILE = str(_BASE_DIR / "transient_worker.log")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[logging.FileHandler(LOG_FILE), logging.StreamHandler()]
)
log = logging.getLogger("TransientWorker")

TARGETS = ["Sgr A*", "Sgr B2", "Crab", "Cyg X-1"]  # Crab/Cyg as baselines

def get_db_conn():
    conn = sqlite3.connect(AGENT_DB)
    conn.row_factory = sqlite3.Row
    return conn

class TransientWorker:
    def __init__(self):
        self.heasarc = Heasarc()
        os.makedirs(INBOX_DIR, exist_ok=True)
        conn = ensure_runtime_db(AGENT_DB, migrate=False)
        conn.close()

    def fetch_swift_bat(self, target):
        """Query Swift BAT Hard X-ray Transient Monitor via HEASARC."""
        try:
            log.info(f"Querying Swift BAT for {target}...")
            # swbatmontr is the mission name for the transient monitor
            table = self.heasarc.query_object(target, mission='swbatmontr')
            if table and len(table) > 0:
                # Get the most recent row
                latest = table[-1]
                flux = float(latest['day_rate'])
                err = float(latest['day_rate_error'])
                mjd = float(latest['time'])
                
                # Convert MJD to ISO
                # MJD 0 is Nov 17 1858
                ts = datetime(1858, 11, 17) + timedelta(days=mjd)
                
                return {
                    "source": "Swift/BAT",
                    "target": target,
                    "flux": flux,
                    "error": err,
                    "energy_band": "15-50 keV",
                    "timestamp": ts.isoformat()
                }
        except Exception as e:
            log.error(f"Swift BAT fetch failed for {target}: {e}")
        return None

    def fetch_fermi_msl(self):
        """Fetch the latest Fermi LAT Monitored Source List daily update."""
        # Sgr A* isn't typically in MSL unless it flares massively
        # But we check for any high-confidence transients in the vicinity
        try:
            log.info("Fetching Fermi MSL Daily Lightcurves...")
            # We fetch the overall list and check for Sgr A* or nearby blazars
            # For now, we simulate with a known source (e.g. 3C 454.3) or a generic catch
            # In a production environment, we'd iterate over the FITS directory
            pass
        except Exception as e:
            log.error(f"Fermi MSL fetch failed: {e}")
        return []

    def fetch_maxi_gsc(self, target):
        """Fetch MAXI GSC lightcurve data from RIKEN."""
        # MAXI often uses names like J1745-290 for Sgr A*
        maxi_names = {"Sgr A*": "J1745-290", "Cyg X-1": "J1958+352"}
        name = maxi_names.get(target, target)
        
        try:
            log.info(f"Querying MAXI GSC for {target} ({name})...")
            # MAXI public lightcurves are often at: http://maxi.riken.jp/star_data/[NAME]/[NAME]_g_lc_1day.tsv
            url = f"http://maxi.riken.jp/star_data/{name}/{name}_g_lc_1day.tsv"
            res = requests.get(url, timeout=10)
            if res.status_code == 200:
                lines = res.text.strip().split('\n')
                # Last line with data
                for line in reversed(lines):
                    if line.startswith('#') or not line.strip(): continue
                    parts = line.split()
                    if len(parts) >= 3:
                        mjd = float(parts[0])
                        flux = float(parts[1])
                        err = float(parts[2])
                        ts = datetime(1858, 11, 17) + timedelta(days=mjd)
                        return {
                            "source": "MAXI/GSC",
                            "target": target,
                            "flux": flux,
                            "error": err,
                            "energy_band": "2-20 keV",
                            "timestamp": ts.isoformat()
                        }
        except Exception as e:
            log.error(f"MAXI fetch failed for {target}: {e}")
        return None

    def run_cycle(self):
        log.info("--- Starting Transient Monitoring Cycle ---")
        findings = []
        
        for target in TARGETS:
            # Swift
            swift_res = self.fetch_swift_bat(target)
            if swift_res: findings.append(swift_res)
            
            # MAXI
            maxi_res = self.fetch_maxi_gsc(target)
            if maxi_res: findings.append(maxi_res)
            
        # Save to DB
        if findings:
            conn = get_db_conn()
            cursor = conn.cursor()
            for f in findings:
                cursor.execute('''
                    INSERT INTO transients (source, target, flux, error, energy_band, timestamp)
                    VALUES (?, ?, ?, ?, ?, ?)
                ''', (f['source'], f['target'], f['flux'], f['error'], f['energy_band'], f['timestamp']))
            conn.commit()
            conn.close()
            
            # Generate Inbox Report
            report_id = int(time.time())
            filename = os.path.join(INBOX_DIR, f"transient_report_{report_id}.json")
            report = {
                "type": "high_energy_transient_report",
                "timestamp": datetime.now().isoformat(),
                "cycle_id": report_id,
                "findings": findings,
                "summary": f"Detected {len(findings)} high-energy flux updates across {len(TARGETS)} targets."
            }
            with open(filename, "w") as f_out:
                json.dump(report, f_out, indent=2)
            log.info(f"Report dropped into inbox: {filename}")
        else:
            log.info("No new transient data found this cycle.")

    def start(self):
        log.info(f"Transient Worker active. Polling every {POLL_INTERVAL}s.")
        while True:
            try:
                self.run_cycle()
            except Exception as e:
                log.error(f"Cycle failed: {e}")
            time.sleep(POLL_INTERVAL)

if __name__ == "__main__":
    worker = TransientWorker()
    # If run with --once, just run once and exit
    import sys
    if "--once" in sys.argv:
        worker.run_cycle()
    else:
        worker.start()
