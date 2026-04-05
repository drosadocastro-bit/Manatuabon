import sqlite3
import time
import threading
from mast_worker import worker_loop, DB_PATH
import os

def check_db():
    start = time.time()
    last_attempts = -1
    while time.time() - start < 45:
        with sqlite3.connect(DB_PATH) as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute("SELECT attempts, status FROM mast_queue WHERE target_name = 'TEST_FAIL_ORION'").fetchone()
            if row:
                if row['attempts'] != last_attempts:
                    print(f"[TEST] Current status: {row['status']}, attempts: {row['attempts']}")
                    last_attempts = row['attempts']
                if row['attempts'] >= 3 and row['status'] == 'failed':
                    print("[TEST] SUCCESS! The worker stopped retrying after 3 attempts and marked it as failed.")
                    # Let's forcefully exit since worker_loop runs forever.
                    os._exit(0)
        time.sleep(1)
    
    # Timeout reached
    print("[TEST] FAILED! Timeout reached before 3 attempts completed.")
    os._exit(1)

def main():
    print("Setting up test data...")
    from mast_worker import ensure_tables
    ensure_tables()
    with sqlite3.connect(DB_PATH) as conn:
        # Clear old tests
        conn.execute("DELETE FROM mast_queue WHERE target_name = 'TEST_FAIL_ORION'")
        # Insert a target name that will surely fail
        conn.execute("""
            INSERT INTO mast_queue (target_name, queued_at, status, attempts)
            VALUES ('TEST_FAIL_ORION', datetime('now'), 'pending', 0)
        """)
        conn.commit()
    
    # Start monitor
    threading.Thread(target=check_db, daemon=True).start()
    
    # Start worker loop (blocks)
    worker_loop()

if __name__ == "__main__":
    main()
