import os
import subprocess
import sys

def test_run_agent_smoke():
    result = subprocess.run([sys.executable, "src/run_agent.py"], capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
    assert os.path.exists("outputs/recon_summary.json")
    assert os.path.exists("outputs/matched.csv")
    assert os.path.exists("outputs/suggestions.csv")

