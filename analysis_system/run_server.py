import os
import subprocess
import sys

os.chdir(os.path.dirname(os.path.abspath(__file__)))
env = os.environ.copy()
env.setdefault("PYTHONUTF8", "1")
env.setdefault("PYTHONIOENCODING", "utf-8")
p = subprocess.Popen([sys.executable, 'app.py'], env=env)
print('server started, pid:', p.pid)
try:
    p.wait()
except KeyboardInterrupt:
    p.terminate()
