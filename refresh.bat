@echo off
title N4SFL Dashboard Refresh
cd /d "%~dp0"

set PYTHON=C:\Users\Jason\AppData\Local\Python\pythoncore-3.14-64\python.exe
if not exist "%PYTHON%" set PYTHON=python

echo Refreshing dashboard...
"%PYTHON%" bpq_dashboard.py

if %ERRORLEVEL% NEQ 0 (
    echo.
    echo Something went wrong. Check the output above.
    pause
    exit /b 1
)

echo.
echo Starting dashboard server...
"%PYTHON%" dashboard_server.py > nul 2>&1
if %ERRORLEVEL% NEQ 0 (
    start "" /B "%PYTHON%" dashboard_server.py
    timeout /t 1 /nobreak > nul
)

echo Done! Opening dashboard...
start "" "http://127.0.0.1:5999"
echo.
pause
