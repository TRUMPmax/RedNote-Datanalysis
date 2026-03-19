@echo off
setlocal EnableExtensions
chcp 65001 >nul
set "PYTHONUTF8=1"
set "PYTHONIOENCODING=utf-8"

set "SCRIPT_DIR=%~dp0"
if "%SCRIPT_DIR:~-1%"=="\" set "SCRIPT_DIR=%SCRIPT_DIR:~0,-1%"
cd /d "%SCRIPT_DIR%"

set "PYTHON_BIN=python"
set "PROJECT_VENV_PY=%SCRIPT_DIR%\..\.venv\Scripts\python.exe"
if exist "%PROJECT_VENV_PY%" (
    "%PROJECT_VENV_PY%" -c "import sys; raise SystemExit(0 if sys.version_info >= (3, 10) else 1)" >nul 2>&1
    if not errorlevel 1 (
        for %%I in ("%PROJECT_VENV_PY%") do set "PYTHON_BIN=%%~fI"
    )
)

set "APP_URL=http://127.0.0.1:8080"
set "PORT=8080"
if not defined PORT set "PORT=8080"

echo.
echo ==================================================
echo     XHS Data Collection and Analytics Platform
echo ==================================================
echo.

if /I "%PYTHON_BIN%"=="python" (
    where python >nul 2>&1
    if errorlevel 1 (
        echo [ERROR] Python 3.10+ was not found in PATH.
        pause
        exit /b 1
    )
) else (
    if not exist "%PYTHON_BIN%" (
        echo [ERROR] Virtual env python was not found: %PYTHON_BIN%
        pause
        exit /b 1
    )
)

"%PYTHON_BIN%" -c "import sys; raise SystemExit(0 if sys.version_info >= (3, 10) else 1)" >nul 2>&1
if errorlevel 1 (
    echo [ERROR] Python 3.10+ is required.
    pause
    exit /b 1
)

echo [1/4] Checking dependencies...
"%PYTHON_BIN%" -c "import flask, jieba" >nul 2>&1
if errorlevel 1 (
    echo     Installing requirements...
    "%PYTHON_BIN%" -m pip install -r requirements.txt
    if errorlevel 1 (
        echo [ERROR] Failed to install requirements.
        pause
        exit /b 1
    )
)
echo     Dependencies ready.

echo [2/4] Restarting project server...
powershell -NoProfile -ExecutionPolicy Bypass -File "%SCRIPT_DIR%\start_server.ps1" -PythonBin "%PYTHON_BIN%" -ScriptDir "%SCRIPT_DIR%" -Port "%PORT%"
if errorlevel 1 (
    echo [ERROR] Failed to restart the web server.
    pause
    exit /b 1
)

echo [3/4] Waiting for server...
timeout /t 2 /nobreak >nul

echo [4/4] Opening browser...
start "" "%APP_URL%"

echo.
echo Browser opened: %APP_URL%
echo The server is running in background mode.
echo Run the launcher again to restart it.
echo If the page does not load immediately, refresh once.
echo.
pause
exit /b 0
