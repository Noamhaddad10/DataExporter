@echo off
title Data Exporter -- Step 1 (Install prerequisites)

REM ============================================================
REM  Wrapper that bypasses PowerShell execution policy without
REM  changing system settings. Just double-click this .bat
REM  AS ADMINISTRATOR (right-click -> Run as administrator).
REM ============================================================

cd /d "%~dp0"

net session >nul 2>&1
if %ERRORLEVEL% neq 0 (
    echo.
    echo ============================================================
    echo  ADMINISTRATOR RIGHTS REQUIRED
    echo ============================================================
    echo.
    echo  Right-click this .bat file and choose
    echo  "Run as administrator", then click YES on the prompt.
    echo.
    pause
    exit /b 1
)

echo.
echo Running 1-install-prereqs.ps1 (admin) -- this may take 15-25 minutes.
echo Do NOT close this window.
echo.

powershell.exe -NoProfile -ExecutionPolicy Bypass -File "%~dp01-install-prereqs.ps1"
set EXITCODE=%ERRORLEVEL%

echo.
if %EXITCODE% == 0 (
    echo ============================================================
    echo  STEP 1 COMPLETE -- now run INSTALL-2-deploy.bat
    echo ============================================================
) else (
    echo ============================================================
    echo  STEP 1 FAILED -- exit code %EXITCODE%
    echo  Read the error message above and try again.
    echo ============================================================
)
echo.
pause
