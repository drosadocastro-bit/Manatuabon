import os
import sys
import json
import time
import sqlite3
import logging
from pathlib import Path
from datetime import datetime
from db_init import ensure_runtime_db

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
log = logging.getLogger("mast_worker")

DB_PATH = Path(__file__).resolve().parent / "manatuabon.db"
INBOX_DIR = Path(__file__).resolve().parent / "inbox"

def ensure_tables():
    conn = ensure_runtime_db(DB_PATH, migrate=False)
    conn.close()

def process_target(target_name: str) -> tuple[str, dict] | tuple[None, None]:
    """Query MAST and return (text_report, structured_json) or (None, None) on failure."""
    try:
        from astroquery.mast import Observations
        log.info(f"Querying STScI MAST Archive for: {target_name}")
        
        # Query the MAST API
        obs_table = Observations.query_object(target_name, radius="0.05 deg")
        if not obs_table or len(obs_table) == 0:
            log.warning(f"No observations found for {target_name}.")
            return f"No observations found for {target_name}.", {"target": target_name, "obs_count": 0}

        # Filter for JWST and Hubble
        jwst_hst = obs_table[(obs_table['obs_collection'] == 'JWST') | (obs_table['obs_collection'] == 'HST')]
        
        if len(jwst_hst) == 0:
            log.warning(f"Observations found, but none from JWST or HST for {target_name}")
            return f"Observations exist for {target_name}, but none from James Webb or Hubble.", {"target": target_name, "obs_count": 0}

        # Sort by latest
        jwst_hst.sort('t_min', reverse=True)
        recent_obs = jwst_hst[:5] # Top 5 newest

        # ── Text Report ──
        report = f"🔭 MAST Telescopic Archive Report: {target_name.upper()}\n"
        report += "="*50 + "\n"
        report += f"Generated: {datetime.now().isoformat()}\n"
        report += f"Total JWST/HST observations found: {len(jwst_hst)}\n\n"
        
        report += "MOST RECENT TELEMETRY DATA:\n"
        obs_records = []
        for row in recent_obs:
            report += f"\n--- Observation ID: {row['obs_id']} ---\n"
            report += f"Instrument: {row['instrument_name']} ({row['obs_collection']})\n"
            report += f"Target Name: {row['target_name']}\n"
            report += f"Filters: {row['filters']}\n"
            report += f"Exposure Time: {row['t_exptime']} seconds\n"
            report += f"Coordinates (RA, Dec): {row['s_ra']}, {row['s_dec']}\n"
            report += f"Start Time (MJD): {row['t_min']}\n"
            obs_records.append({
                "obs_id": str(row['obs_id']),
                "instrument": str(row['instrument_name']),
                "collection": str(row['obs_collection']),
                "target": str(row['target_name']),
                "filters": str(row['filters']),
                "exposure_s": float(row['t_exptime']) if row['t_exptime'] else 0,
                "ra": float(row['s_ra']) if row['s_ra'] else 0,
                "dec": float(row['s_dec']) if row['s_dec'] else 0,
                "t_min_mjd": float(row['t_min']) if row['t_min'] else 0,
            })

        report += "\n" + "="*50 + "\n"
        report += "CRITICAL RAG EXTRACTION INSTRUCTION: Extract anomalous infrared or waveband metadata from these instruments. If MIRI or NIRCam captured unexpected exposure profiles, note it."
        
        # ── Structured JSON ──
        structured = {
            "target": target_name,
            "obs_count": int(len(jwst_hst)),
            "latest_obs_id": str(recent_obs[0]['obs_id']),
            "coordinates": {
                "ra": float(recent_obs[0]['s_ra']) if recent_obs[0]['s_ra'] else 0,
                "dec": float(recent_obs[0]['s_dec']) if recent_obs[0]['s_dec'] else 0,
            },
            "instruments": list(set(str(r['instrument_name']) for r in recent_obs)),
            "collections": list(set(str(r['obs_collection']) for r in recent_obs)),
            "observations": obs_records,
            "timestamp": datetime.now().isoformat(),
        }
        
        return report, structured

    except Exception as e:
        log.error(f"MAST API Failed for {target_name}: {e}")
        return None, None

def write_to_inbox(target_name: str, report_body: str, structured_data: dict | None = None):
    safe_name = "".join(c if c.isalnum() else "_" for c in target_name)
    ts = int(time.time())
    
    # Write .txt telemetry report
    txt_path = INBOX_DIR / f"STScI_Report_{safe_name}_{ts}.txt"
    with open(txt_path, "w", encoding="utf-8") as f:
        f.write(report_body)
    log.info(f"Dropped telemetry report into Inbox: {txt_path.name}")
    
    # Write .json structured companion
    if structured_data:
        json_path = INBOX_DIR / f"STScI_Data_{safe_name}_{ts}.json"
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(structured_data, f, indent=2, ensure_ascii=False)
        log.info(f"Dropped structured JSON into Inbox: {json_path.name}")

def worker_loop():
    ensure_tables()
    log.info("MAST Background Worker initialized. Dynamic polling active...")
    INBOX_DIR.mkdir(exist_ok=True)
    
    while True:
        job_found = False
        try:
            with sqlite3.connect(DB_PATH) as conn:
                conn.row_factory = sqlite3.Row
                row = conn.execute("SELECT * FROM mast_queue WHERE status='pending' OR (status='failed' AND attempts < 3) ORDER BY queued_at ASC LIMIT 1").fetchone()
                
                if row:
                    job_found = True
                    task_id = row['id']
                    target_name = row['target_name']
                    current_attempts = row['attempts'] + 1
                    
                    # Mark as running, increment attempts, and update timestamp
                    now_str = datetime.now().isoformat()
                    conn.execute("UPDATE mast_queue SET status='running', attempts=attempts+1, last_run=? WHERE id=?", (now_str, task_id))
                    conn.commit()
                    
                    # Execute API Call
                    t0 = time.time()
                    report, structured = process_target(target_name)
                    elapsed = time.time() - t0
                    
                    # Determine final status
                    if report:
                        write_to_inbox(target_name, report, structured)
                        conn.execute("UPDATE mast_queue SET status='done' WHERE id=?", (task_id,))
                        log.info(f"[JOB] target={target_name} attempt={current_attempts} status=done elapsed={elapsed:.1f}s")
                    else:
                        conn.execute("UPDATE mast_queue SET status='failed' WHERE id=?", (task_id,))
                        log.warning(f"[JOB] target={target_name} attempt={current_attempts}/3 status=failed elapsed={elapsed:.1f}s")
                    conn.commit()
                    
        except Exception as e:
            log.error(f"Worker iteration threw exception: {e}")
        
        # Dynamic sleep: fast when busy, slow when idle
        if job_found:
            time.sleep(5)
        else:
            time.sleep(25)

if __name__ == "__main__":
    worker_loop()
