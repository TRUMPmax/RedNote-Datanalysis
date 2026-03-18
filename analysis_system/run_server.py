import subprocess, time, os, sys
os.chdir(os.path.dirname(os.path.abspath(__file__)))
p = subprocess.Popen(['python', 'app.py'])
print('server started, pid:', p.pid)
try:
    p.wait()
except KeyboardInterrupt:
    p.terminate()
