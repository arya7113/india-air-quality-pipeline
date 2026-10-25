"""
Run the whole pipeline in order:  extract -> load -> transform -> quality checks.
Each stage is its own script and can also be run alone.  Usage:  python pipeline.py
"""
import subprocess
import sys
import time

STAGES = ["extract.py", "load.py", "transform.py", "quality_checks.py"]


def main():
    t0 = time.time()
    for stage in STAGES:
        print(f"\n===== {stage} =====")
        result = subprocess.run([sys.executable, stage])
        if result.returncode != 0:
            sys.exit(f"pipeline stopped: {stage} failed")
    print(f"\n[pipeline] all stages finished in {time.time() - t0:.0f}s")


if __name__ == "__main__":
    main()
