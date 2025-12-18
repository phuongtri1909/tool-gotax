import subprocess
import time
import sys
import os

processes = []

def run():
    python_cmd = "py" if sys.platform == "win32" else "python"
    
    num_go_quick_workers = 10
    
    cmds = [
        [python_cmd, "api_server.py"],
        [python_cmd, "workers/go_soft_worker.py"],
    ]
    
    for i in range(num_go_quick_workers):
        cmds.append([python_cmd, "workers/go_quick_worker.py"])

    for cmd in cmds:
        print(f"✅ Started: {' '.join(cmd)}")
        p = subprocess.Popen(cmd, shell=(sys.platform == "win32"))
        processes.append(p)

    try:
        for p in processes:
            p.wait()
    except KeyboardInterrupt:
        print("Stopping all...")
        for p in processes:
            p.terminate()

if __name__ == "__main__":
    run()
