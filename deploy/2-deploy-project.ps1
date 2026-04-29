<#
.SYNOPSIS
    Deploy the Data Exporter project to C:\WiresharkLogger and install all
    Python dependencies offline. Re-runnable: wipes the project folder and
    venv each time, but preserves DataExporterConfig.json if you have edited
    it (e.g. real DB name + SQL server).

.DESCRIPTION
    No administrator rights needed. Run this AFTER 1-install-prereqs.ps1.

.PARAMETER ProjectDir
    Where the project will live. Default: C:\WiresharkLogger
#>

[CmdletBinding()]
param(
    [string]$ProjectDir = 'C:\WiresharkLogger'
)

# ---------------------------------------------------------------------------
# Prelude
# ---------------------------------------------------------------------------
$ErrorActionPreference = 'Stop'
$KitRoot       = $PSScriptRoot
$ProjectSource = Join-Path $KitRoot 'data-exporter'
$Wheels        = Join-Path $KitRoot 'python-offline-packages'
$ConfigTemplate= Join-Path $KitRoot 'DataExporterConfig.json.template'

function Write-Step([string]$msg) { Write-Host "`n=== $msg ===" -ForegroundColor Cyan }
function Write-OK([string]$msg)   { Write-Host "[OK]   $msg"  -ForegroundColor Green }
function Write-Skip([string]$msg) { Write-Host "[SKIP] $msg"  -ForegroundColor Yellow }
function Write-Info([string]$msg) { Write-Host "[INFO] $msg"  -ForegroundColor Gray }
function Write-Fail([string]$msg) { Write-Host "[FAIL] $msg"  -ForegroundColor Red }
function Write-Warn([string]$msg) { Write-Host "[WARN] $msg"  -ForegroundColor Yellow }

Write-Step "Data Exporter -- Project deployment"
Write-Info "Kit root      : $KitRoot"
Write-Info "Project target: $ProjectDir"

# ---------------------------------------------------------------------------
# 1. Verify prerequisites are present
# ---------------------------------------------------------------------------
Write-Step "1/6  Verifying prerequisites"

function Fail-IfMissing([string]$what, [bool]$present) {
    if (-not $present) {
        Write-Fail "$what is missing. Run 1-install-prereqs.ps1 first (admin)."
        exit 1
    }
}

# Python 3.10
$pyOK = $false
try {
    $v = & py -3.10 -c "import sys; print(sys.version_info[:2])" 2>$null
    if ($LASTEXITCODE -eq 0 -and $v -match '\(3, 10\)') { $pyOK = $true }
} catch {}
Fail-IfMissing "Python 3.10 (py -3.10)" $pyOK
Write-OK "Python 3.10 found"

# JAVA_HOME
$javaHome = [Environment]::GetEnvironmentVariable('JAVA_HOME', 'Machine')
$javaOK = ($javaHome -and (Test-Path (Join-Path $javaHome 'bin\java.exe')))
Fail-IfMissing "JAVA_HOME pointing to a JDK" $javaOK
Write-OK "JAVA_HOME=$javaHome"

# Kafka
$kafkaStartBat = 'C:\kafka\bin\windows\kafka-server-start.bat'
Fail-IfMissing "Kafka at C:\kafka" (Test-Path $kafkaStartBat)
Write-OK "Kafka found at C:\kafka"

# ODBC Driver 17
$odbcOK = Test-Path 'HKLM:\SOFTWARE\ODBC\ODBCINST.INI\ODBC Driver 17 for SQL Server'
Fail-IfMissing "ODBC Driver 17 for SQL Server" $odbcOK
Write-OK "ODBC Driver 17 registered"

# Kit contents
Fail-IfMissing "Kit folder data-exporter\"          (Test-Path $ProjectSource)
Fail-IfMissing "Kit folder python-offline-packages\" (Test-Path $Wheels)
Fail-IfMissing "DataExporterConfig.json.template"   (Test-Path $ConfigTemplate)
Write-OK "Kit contents present"

# ---------------------------------------------------------------------------
# 2. Preserve user-edited config (if it exists), then wipe project folder
# ---------------------------------------------------------------------------
Write-Step "2/6  Preparing project folder"

$ExistingConfig = Join-Path $ProjectDir 'DataExporterConfig.json'
$ConfigBackup   = $null

if (Test-Path $ExistingConfig) {
    $ConfigBackup = "$env:TEMP\DataExporterConfig.json.backup-$(Get-Date -Format yyyyMMdd-HHmmss)"
    Copy-Item -Path $ExistingConfig -Destination $ConfigBackup -Force
    Write-Info "Backed up existing DataExporterConfig.json to $ConfigBackup"
}

if (Test-Path $ProjectDir) {
    Write-Info "Removing existing $ProjectDir (clean redeploy)..."
    Remove-Item -Recurse -Force $ProjectDir
}
New-Item -ItemType Directory -Path $ProjectDir | Out-Null
Write-OK "Project folder ready: $ProjectDir"

# ---------------------------------------------------------------------------
# 3. Copy project code from kit
# ---------------------------------------------------------------------------
Write-Step "3/6  Copying project code"
Copy-Item -Path (Join-Path $ProjectSource '*') -Destination $ProjectDir -Recurse -Force
Write-OK "Copied data-exporter\* into $ProjectDir"

# Restore user config if we had one, otherwise drop the template
if ($ConfigBackup) {
    Copy-Item -Path $ConfigBackup -Destination $ExistingConfig -Force
    Write-OK "Restored your previous DataExporterConfig.json"
} else {
    Copy-Item -Path $ConfigTemplate -Destination $ExistingConfig -Force
    Write-Warn "DataExporterConfig.json was created from the TEMPLATE. You MUST edit it (see end of script)."
}

# ---------------------------------------------------------------------------
# 4. Create venv
# ---------------------------------------------------------------------------
Write-Step "4/6  Creating Python virtual environment"
$VenvDir   = Join-Path $ProjectDir '.venv'
$VenvPython= Join-Path $VenvDir 'Scripts\python.exe'

& py -3.10 -m venv $VenvDir
if (-not (Test-Path $VenvPython)) { Write-Fail "venv creation failed: $VenvPython missing"; exit 1 }
Write-OK "venv created at $VenvDir"

# ---------------------------------------------------------------------------
# 5. Install Python dependencies (OFFLINE, no internet access)
# ---------------------------------------------------------------------------
Write-Step "5/6  Installing Python packages from local wheels (offline)"

$Requirements = Join-Path $ProjectDir 'requirements.txt'
& $VenvPython -m pip install --no-index --find-links $Wheels -r $Requirements
if ($LASTEXITCODE -ne 0) { Write-Fail "pip install failed (see output above)"; exit 1 }
Write-OK "Python packages installed"

# Smoke test: import every project module + the custom AggregateStatePdu
Write-Info "Smoke-testing imports..."
Push-Location $ProjectDir
try {
    & $VenvPython -c @"
import sys
import launcher
import kafka_main
import kafka_producer
import kafka_consumer
import kafka_config
import LoggerPduProcessor
import LoggerSQLExporter
from opendis.dis7 import AggregateStatePdu
p = AggregateStatePdu()
assert p.pduType == 33, 'AggregateStatePdu pduType should be 33'
print('SMOKE OK')
"@
    if ($LASTEXITCODE -ne 0) { Write-Fail "Import smoke test failed"; exit 1 }
} finally {
    Pop-Location
}
Write-OK "All project modules import successfully (incl. AggregateStatePdu)"

# ---------------------------------------------------------------------------
# 6. Format Kafka storage if empty / first-time
# ---------------------------------------------------------------------------
Write-Step "6/6  Kafka storage"

$KraftLogs    = 'C:\kafka\kraft-logs'
$KafkaStorage = 'C:\kafka\bin\windows\kafka-storage.bat'

if ((Test-Path $KraftLogs) -and (Get-ChildItem $KraftLogs -ErrorAction SilentlyContinue)) {
    Write-Skip "C:\kafka\kraft-logs already initialized (the launcher will reset it on each start when nuke_kafka_on_start=true)"
} else {
    Write-Info "First-time storage init..."
    $env:JAVA_HOME = $javaHome  # ensure current process sees it
    $uuidRaw = & $KafkaStorage random-uuid
    $uuid = ($uuidRaw -split "`r?`n" | Where-Object { $_.Trim() } | Select-Object -Last 1).Trim()
    if (-not $uuid) { Write-Fail "Could not get cluster UUID from kafka-storage.bat"; exit 1 }
    Write-Info "Cluster UUID: $uuid"
    & $KafkaStorage format --config 'C:\kafka\config\kraft\server.properties' --cluster-id $uuid
    if ($LASTEXITCODE -ne 0) { Write-Fail "kafka-storage format failed"; exit 1 }
    Write-OK "Kafka storage formatted"
}

# ---------------------------------------------------------------------------
# Summary + what's left for the human
# ---------------------------------------------------------------------------
Write-Step "Deployment OK"
Write-Host ""
Write-Host "Project deployed at: $ProjectDir" -ForegroundColor Green
Write-Host ""
Write-Host "WHAT YOU MUST DO NEXT (manually):" -ForegroundColor Yellow
Write-Host "  1. Open this file in Notepad:"
Write-Host "       $ExistingConfig"
Write-Host "     Replace the values starting with 'TODO_' :"
Write-Host "       - database_name : the real PROD database name"
Write-Host "       - sql_server    : the real SQL Server address (e.g. localhost\SQLEXPRESS)"
Write-Host ""
Write-Host "  2. Make sure your SQL DBA has already created the database with:"
Write-Host "       - schema 'dis' + 6 tables (FirePdu, Entities, EntityLocations,"
Write-Host "         DetonationPdu, Aggregates, AggregateLocations)"
Write-Host "       - schema 'dbo' + table 'Loggers'"
Write-Host ""
Write-Host "  3. Launch the GUI:"
Write-Host "       Double-click '$ProjectDir\Launch DataExporter.bat'"
Write-Host "     -> Click 'START EVERYTHING' -> watch the live log."
Write-Host ""
