@echo off
setlocal
cd /d C:\Users\kornkanok\Documents\Automation_api
rem ถ้าใช้ venv ให้ uncomment 2 บรรทัดถัดไป
rem call .\.venv\Scripts\activate
rem python -m pip install -q -r requirements.txt

rem ใช้ Python 3.9 ตัวที่มี paramiko/watchdog
"C:\Users\kornkanok\AppData\Local\Programs\Python\Python39\python.exe" watch_and_push.py >> logs\watcher.log 2>&1
