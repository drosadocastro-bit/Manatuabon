import time
import requests
import json
from pathlib import Path

BRIDGE_URL = "http://127.0.0.1:7777"
RENDERS_DIR = Path("D:/Manatuabon/renders")

print(r"""
=========================================
 MANATUABON — Colab Worker Mock (Ph 8D)
=========================================
Polling bridge for pending simulations...
""")

def run_worker():
    RENDERS_DIR.mkdir(parents=True, exist_ok=True)
    
    while True:
        try:
            res = requests.post(f"{BRIDGE_URL}/simulations/dequeue", timeout=5)
            if res.status_code == 200:
                task = res.json()
                print(f"\n[+] DEQUEUED TASK: {task['id']}")
                print(f"    Recommendation: {task.get('recommendation')}")
                
                # Mock running a simulation
                print("    [Worker] Running simulation (mock 5s)...")
                time.sleep(5)
                
                # Save a fake render
                # Extract some keywords to make a dynamic filename
                rec = task.get('recommendation', '').lower()
                mass = "4.2" if "mass" not in rec else "custom"
                spin = "0.9" if "spin" not in rec else "custom"
                
                filename = f"sgra_mass{mass}_spin{spin}_auto.png"
                out_path = RENDERS_DIR / filename
                
                # Create a dummy file
                with open(out_path, "w") as f:
                    f.write(f"Mock render data for task {task['id']}\nRecommendation: {task.get('recommendation')}")
                    
                print(f"    [Worker] Saved render to {out_path.name}")
                print(f"    [Worker] Waiting for agent to ingest it...")
                
            elif res.status_code == 204:
                # No tasks pending
                time.sleep(10)
            else:
                print(f"[-] Unexpected status: {res.status_code}")
                time.sleep(10)
                
        except requests.exceptions.ConnectionError:
            print("[-] Bridge unreachable. Waiting 10s...")
            time.sleep(10)
        except Exception as e:
            print(f"[-] Error: {e}")
            time.sleep(10)

if __name__ == "__main__":
    run_worker()
