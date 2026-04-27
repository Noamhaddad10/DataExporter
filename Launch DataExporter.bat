@echo off
title Data Exporter Launcher
cd /d "%~dp0"

REM ============================================================
REM  Data Exporter -- one-click GUI launcher
REM  Double-click this file to open the launcher (no venv activation
REM  needed). To get a desktop shortcut: right-click this file ->
REM  Send to -> Desktop (create shortcut).
REM ============================================================

if not exist ".venv\Scripts\python.exe" (
    echo.
    echo [ERROR] Python venv not found at .venv\Scripts\python.exe
    echo.
    echo This .bat must sit at the root of the WiresharkLogger project,
    echo and the venv must already be created. See docs\INSTALL-PROD.md
    echo for the install procedure.
    echo.
    pause
    exit /b 1
)

if not exist "launcher.py" (
    echo.
    echo [ERROR] launcher.py not found in this directory.
    echo Place this .bat at the root of the project, next to launcher.py.
    echo.
    pause
    exit /b 1
)

echo Launching Data Exporter GUI...
echo (this console window stays open while the GUI is running)
echo.

".venv\Scripts\python.exe" "launcher.py"
set EXITCODE=%ERRORLEVEL%

if not %EXITCODE% == 0 (
    echo.
    echo ============================================================
    echo  Launcher exited with error code %EXITCODE%
    echo  Read the messages above for details.
    echo ============================================================
    pause
) else (
    REM Clean exit -- close the window without asking
    timeout /t 2 /nobreak > nul
)
