import subprocess
import sys
from datetime import datetime

def run_script(script_name, description):
    print(f"\n{'='*70}")
    print(f"{description}")
    print(f"{'='*70}")
    
    result = subprocess.run(
        [sys.executable, script_name],
        capture_output=False,
        text=True
    )
    
    if result.returncode != 0:
        print(f"\n✗ ERROR: {description} failed")
        sys.exit(1)
    
    return result.returncode

try:
    print("\n" + "="*70)
    print("FULL PIPELINE EXECUTION")
    print(f"Started: {datetime.now()}")
    print("="*70)

    run_script("python/bronze_load.py", "1. BRONZE LAYER - Raw Data Ingestion")
    run_script("python/silver_transform.py", "2. SILVER LAYER - Data Cleansing")
    run_script("python/gold_transform.py", "3. GOLD LAYER - Dimensional Model")
    run_script("python/mart_transform.py", "4. MART LAYER - Business Views")

    print("\n" + "="*70)
    print("✓ PIPELINE COMPLETED SUCCESSFULLY")
    print(f"Finished: {datetime.now()}")
    print("="*70)
    print("\nAll 4 layers ready:")
    print("  BRONZE → Raw data ingestion")
    print("  SILVER → Cleansed & conformed data")
    print("  GOLD   → Dimensional model (star schema)")
    print("  MART   → Business-friendly views")
    print("="*70 + "\n")

except Exception as e:
    print(f"\n✗ PIPELINE FAILED: {e}\n")
    sys.exit(1)
