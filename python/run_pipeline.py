import subprocess
import sys
from datetime import datetime

def run_script(script_name, description):
    print(f"\n{description}")
    
    result = subprocess.run(
        [sys.executable, script_name],
        capture_output=False,
        text=True
    )
    
    if result.returncode != 0:
        print(f"ERROR: {description} failed")
        sys.exit(1)
    
    return result.returncode

try:
    print(f"\nPipeline started: {datetime.now()}")

    run_script("python/bronze_setup.py", "0. BRONZE SETUP - Infrastructure & Tables")
    run_script("python/bronze_load.py", "1. BRONZE LAYER - Raw Data Ingestion")
    run_script("python/silver_transform.py", "2. SILVER LAYER - Data Cleansing")
    run_script("python/gold_transform.py", "3. GOLD LAYER - Dimensional Model")
    run_script("python/mart_transform.py", "4. MART LAYER - Business Views")

    print(f"\nPipeline completed: {datetime.now()}")
    print("All layers ready: BRONZE → SILVER → GOLD → MART")

except Exception as e:
    print(f"PIPELINE FAILED: {e}")
    sys.exit(1)
