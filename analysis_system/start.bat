@echo off
setlocal

set "SCRIPT_DIR=%~dp0"
cd /d "%SCRIPT_DIR%"

set "PYTHON_BIN=python"
if exist "%SCRIPT_DIR%..\..\.venv\Scripts\python.exe" (
    for %%I in ("%SCRIPT_DIR%..\..\.venv\Scripts\python.exe") do set "PYTHON_BIN=%%~fI"
)

set "APP_URL=http://127.0.0.1:8080"
set "PORT=8080"
set "APP_SCRIPT=%SCRIPT_DIR%app.py"

echo.
echo ==================================================
echo     XHS Analysis Launcher
echo ==================================================
echo.

if /I "%PYTHON_BIN%"=="python" (
    where python >nul 2>&1
    if errorlevel 1 (
        echo [ERROR] Python 3.8+ was not found in PATH.
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

echo [1/3] Checking dependencies...
"%PYTHON_BIN%" -m pip show flask >nul 2>&1
if errorlevel 1 (
    echo     Installing requirements...
    "%PYTHON_BIN%" -m pip install -r requirements.txt
    if errorlevel 1 (
        echo [ERROR] Failed to install requirements.
        pause
        exit /b 1
    )
)

"%PYTHON_BIN%" -m pip show pandas >nul 2>&1
if errorlevel 1 (
    echo     Installing pandas/openpyxl...
    "%PYTHON_BIN%" -m pip install -r requirements.txt
    if errorlevel 1 (
        echo [ERROR] Failed to install data dependencies.
        pause
        exit /b 1
    )
)
echo     Dependencies ready.

echo [2/4] Restarting project server...
powershell -NoProfile -ExecutionPolicy Bypass -File "%SCRIPT_DIR%start_server.ps1" -PythonBin "%PYTHON_BIN%" -ScriptDir "%SCRIPT_DIR%" -Port %PORT%
if errorlevel 1 (
    echo [ERROR] Failed to restart the web server.
    pause
    exit /b 1
)

echo [4/4] Opening browser...
timeout /t 3 /nobreak >nul
start "" "%APP_URL%"

echo.
echo Browser opened: %APP_URL%
echo The server is running in a new window.
echo Close that window to stop the service.
echo If the page does not load immediately, refresh once.
echo.
pause
exit /b 0
