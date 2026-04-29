@echo off
title Data Exporter -- Step 2 (Deploy project)

REM ============================================================
REM  Wrapper that bypasses PowerShell execution policy without
REM  changing system settings. Just double-click this .bat.
REM  No admin rights needed.
REM ============================================================

cd /d "%~dp0"

echo.
echo Running 2-deploy-project.ps1 -- this takes 2 to 3 minutes.
echo.

powershell.exe -NoProfile -ExecutionPolicy Bypass -File "%~dp02-deploy-project.ps1"
set EXITCODE=%ERRORLEVEL%

echo.
if %EXITCODE% == 0 (
    echo ============================================================
    echo  STEP 2 COMPLETE -- read the message above for what to
    echo  edit in DataExporterConfig.json before launching the GUI.
    echo ============================================================
) else (
    echo ============================================================
    echo  STEP 2 FAILED -- exit code %EXITCODE%
    echo  Read the error message above. The most common cause is
    echo  "prerequisites missing" -- run INSTALL-1-prereqs.bat first.
    echo ============================================================
)
echo.
pause
