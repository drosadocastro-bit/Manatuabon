import requests
import json
import logging
import time
import os
from datetime import datetime, timedelta
from pathlib import Path
from manatuabon_agent import MemoryManager

# Configure Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler("mission_worker.log"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)

# Constants
HORIZONS_API = "https://ssd.jpl.nasa.gov/api/horizons.api"
NASA_GCS_BASE = "https://storage.googleapis.com/storage/v1/b/p-2-cen1/o/"
ORION_ID = "'-1024'"  # Artemis II Orion MPCV
OBSERVER_GEO = "'-66.1527,18.4346,0'"  # Bayamón, PR (Lon, Lat, Alt km)
INBOX_PATH = Path(__file__).resolve().parent / "inbox"
DB_PATH = Path(__file__).resolve().parent / "manatuabon.db"

# NASA Parameter Mappings
# Parm 2003,2004,2005: ECI Position (ft)
# Parm 2009,2010,2011: ECI Velocity (ft/s)
# Earth Radius: 20,902,231 ft
EARTH_RADIUS_FT = 20902231.0
FT_TO_MI = 1.0 / 5280.0
AU_TO_MI = 92955807.3

class MissionWorker:
    def __init__(self):
        self.memory = MemoryManager(str(DB_PATH))
        INBOX_PATH.mkdir(parents=True, exist_ok=True)

    def fetch_nasa_telemetry(self):
        """Fetch high-fidelity telemetry from NASA AROW GCS Bucket."""
        # Dynamic URL construction based on current date (UTC)
        now = datetime.utcnow()
        month_str = now.strftime("%B")
        day_str = str(now.day)
        
        # We try to find the current active file. 
        # Since pulse numbers/day-of-year varies, we might need to list or guess.
        # For simulation, we'll try the subagent's confirmed path format.
        # Example: October/1/October_105_1.txt
        # We'll use a placeholder target for now or try to fetch the 'o' listing.
        
        # Hardcoding a likely pulse for the current simulation day
        # In a production environment, we would use the GCS listing API to find the latest pulse
        url = f"{NASA_GCS_BASE}{month_str}%2F{day_str}%2F{month_str}_latest.txt?alt=media"
        
        # Fallback to the subagent's found path if current guess fails
        fallback_url = f"{NASA_GCS_BASE}October%2F1%2FOctober_105_1.txt?alt=media"
        
        try:
            log.info(f"Attempting NASA GCS Fetch: {url}")
            response = requests.get(url, timeout=10)
            if response.status_code != 200:
                log.warning(f"NASA GCS {url} not found, using fallback.")
                response = requests.get(fallback_url, timeout=10)
            
            data = response.json()
            
            # Extract state vectors (ECI Position in feet)
            x = float(data.get('Parameter_2003', {}).get('Value', 0))
            y = float(data.get('Parameter_2004', {}).get('Value', 0))
            z = float(data.get('Parameter_2005', {}).get('Value', 0))
            
            # Extract velocity vectors (ECI Velocity in ft/s)
            vx = float(data.get('Parameter_2009', {}).get('Value', 0))
            vy = float(data.get('Parameter_2010', {}).get('Value', 0))
            vz = float(data.get('Parameter_2011', {}).get('Value', 0))
            
            import math
            dist_ft = math.sqrt(x**2 + y**2 + z**2)
            dist_mi = (dist_ft - EARTH_RADIUS_FT) * FT_TO_MI
            
            vel_fps = math.sqrt(vx**2 + vy**2 + vz**2)
            vel_mph = vel_fps * 3600.0 / 5280.0
            
            return {
                'dist_mi': dist_mi,
                'vel_mph': vel_mph,
                'timestamp': data.get('Parameter_2003', {}).get('Time', now.isoformat())
            }
        except Exception as e:
            log.error(f"NASA GCS Fetch failed: {e}")
            return None

    def fetch_horizons_telemetry(self):
        """Fetch real-time ephemeris from JPL Horizons."""
        log.info(f"Querying JPL Horizons for Artemis II (Orion {ORION_ID})...")
        
        # We query for a 2-hour window starting 1 hour ago to ensure we catch current state
        now = datetime.utcnow()
        start_time = (now - timedelta(hours=1)).strftime("'%Y-%m-%d %H:%M'")
        stop_time = (now + timedelta(hours=1)).strftime("'%Y-%m-%d %H:%M'")
        
        params = {
            'format': 'json',
            'COMMAND': ORION_ID,
            'OBJ_DATA': "'YES'",
            'MAKE_EPHEM': "'YES'",
            'EPHEM_TYPE': "'OBSERVER'",
            'CENTER': "'COORD'",
            'COORD_TYPE': "'GEODETIC'",
            'SITE_COORD': OBSERVER_GEO,
            'START_TIME': start_time,
            'STOP_TIME': stop_time,
            'STEP_SIZE': "'5 m'",
            'QUANTITIES': "'1,3,4,20'", # RA/Dec, Alt/Az, Dist, Velocity
        }
        
        try:
            response = requests.get(HORIZONS_API, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            if 'result' not in data:
                log.error("JPL Horizons returned no result.")
                return None
                
            return self.parse_horizons_output(data['result'])
            
        except Exception as e:
            log.error(f"Horizons API request failed: {e}")
            return None

    def parse_horizons_output(self, output_text):
        """Extract the most recent telemetry point from the Horizons text output."""
        try:
            # Horizons ephemeris data is between $$SOE and $$EOE
            if "$$SOE" not in output_text:
                return None
            
            eph_data = output_text.split("$$SOE")[1].split("$$EOE")[0].strip().split("\n")
            
            # Use the point closest to 'now'
            # Each line: 2026-Apr-02 13:00 * m  17 48 53.64 -28 53 17.5  -15.5391   294.4939  0.98453488208945 -14.3415175
            latest_line = eph_data[-1] 
            parts = latest_line.split()
            
            # Basic parsing (positions may vary slightly depending on exact quantities)
            # Quantity 1: RA (h m s), Dec (d m s)
            # Quantity 4: Azimuth, Elevation (Alt)
            # Quantity 20: Range, Range Rate
            
            # Indices for QUANTITIES '1,4,20' (Observer table):
            # 0: Date, 1: Time, 2: Solar Presence, 3: Lunar Presence
            # 4,5,6: RA (h, m, s), 7,8,9: Dec (d, m, s)
            # 10: Azimuth, 11: Elevation (Alt)
            # 12: Delta (AU), 13: Delta-dot (km/s)
            
            # Validation: JPL sometimes changes column count if flags change.
            if len(parts) < 13:
                log.warning(f"Unexpected Horizons format: {latest_line}")
                return None

            record = {
                'mission_id': '-1024',
                'mission_name': 'Artemis II',
                'timestamp': f"{parts[0]}T{parts[1]}",
                'ra': float(parts[4]) + float(parts[5])/60 + float(parts[6])/3600,
                'dec': float(parts[7]) + (float(parts[8])/60 + float(parts[9])/3600) * (1 if '-' not in parts[7] else -1),
                'az': float(parts[10]),
                'alt': float(parts[11]),
                'dist_au': float(parts[12]),
                'vel_km_s': float(parts[13]),
                'status': 'active'
            }
            return record

        except Exception as e:
            log.error(f"Error parsing Horizons output: {e}")
            return None

    def drop_report(self, record):
        """Save a report to the inbox for the agent to ingest."""
        report = {
            "type": "mission_telemetry_report",
            "source": "JPL Horizons",
            "mission": record['mission_name'],
            "timestamp": datetime.now().isoformat(),
            "data": record
        }
        filename = f"mission_report_{int(time.time())}.json"
        with open(INBOX_PATH / filename, "w") as f:
            json.dump(report, f, indent=4)
        log.info(f"Report dropped: {filename}")

    def run_once(self):
        # 1. Fetch Primary Sky coordinates from JPL
        record = self.fetch_horizons_telemetry()
        
        # 2. Augment with High-Fidelity telemetry from NASA GCS if available
        nasa_data = self.fetch_nasa_telemetry()
        
        if record:
            if nasa_data:
                log.info("Merging NASA high-fidelity telemetry into JPL record.")
                # NASA distance is in miles. JPL distance is in AU. 
                # We'll store NASA's official mission metrics but keep JPL's AU for history.
                record['vel_mph'] = nasa_data['vel_mph']
                record['dist_mi'] = nasa_data['dist_mi']
                # Potential status update based on NASA telemetry
                record['status'] = 'active'
            
            self.memory.add_mission_record(record)
            self.drop_report(record)
        elif nasa_data:
            # If JPL is down, we still want NASA stats
            record = {
                'mission_id': '-1024',
                'mission_name': 'Artemis II',
                'timestamp': nasa_data['timestamp'],
                'ra': 0, 'dec': 0, 'alt': -99, 'az': 0, # unknowns
                'dist_au': nasa_data['dist_mi'] / AU_TO_MI,
                'dist_mi': nasa_data['dist_mi'],
                'vel_mph': nasa_data['vel_mph'],
                'vel_km_s': nasa_data['vel_mph'] * 1.60934 / 3600.0,
                'status': 'active'
            }
            self.memory.add_mission_record(record)
            self.drop_report(record)
        else:
            log.warning("No mission telemetry fetched from any source this cycle.")

    def loop(self, interval_sec=600):
        log.info(f"Starting Mission Worker loop (Interval: {interval_sec}s)...")
        while True:
            try:
                self.run_once()
            except Exception as e:
                log.error(f"Worker loop error: {e}")
            time.sleep(interval_sec)

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args()
    
    worker = MissionWorker()
    if args.once:
        worker.run_once()
    else:
        worker.loop()
