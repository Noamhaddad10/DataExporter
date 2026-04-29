<#
.SYNOPSIS
    Install Python 3.10, Java 17, Kafka 3.9.0, ODBC Driver 17 on a fresh
    Windows PC. Idempotent: anything already installed is skipped.

.DESCRIPTION
    Run this ONCE on the production PC, with administrator rights. It uses
    only the installers shipped in the transfer kit (offline-friendly).

.EXAMPLE
    Right-click this file -> "Run with PowerShell" -> click YES on the UAC
    prompt. Script finishes in 15-25 minutes depending on the PC.
#>

# ---------------------------------------------------------------------------
# Prelude: stop on first error, force admin, locate the kit, set up logging
# ---------------------------------------------------------------------------
$ErrorActionPreference = 'Stop'
$KitRoot = $PSScriptRoot
$Installers = Join-Path $KitRoot 'installers'

function Write-Step([string]$msg) { Write-Host "`n=== $msg ===" -ForegroundColor Cyan }
function Write-OK([string]$msg)   { Write-Host "[OK]   $msg"  -ForegroundColor Green }
function Write-Skip([string]$msg) { Write-Host "[SKIP] $msg"  -ForegroundColor Yellow }
function Write-Info([string]$msg) { Write-Host "[INFO] $msg"  -ForegroundColor Gray }
function Write-Fail([string]$msg) { Write-Host "[FAIL] $msg"  -ForegroundColor Red }

function Assert-Admin {
    $current = [Security.Principal.WindowsIdentity]::GetCurrent()
    $principal = New-Object Security.Principal.WindowsPrincipal($current)
    if (-not $principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) {
        Write-Fail "This script needs to run as Administrator."
        Write-Info "Right-click the .ps1 file -> 'Run with PowerShell' -> answer YES to the UAC prompt."
        exit 1
    }
}

function Refresh-EnvPath {
    $machinePath = [Environment]::GetEnvironmentVariable('Path', 'Machine')
    $userPath    = [Environment]::GetEnvironmentVariable('Path', 'User')
    $env:Path    = "$machinePath;$userPath"
}

Assert-Admin
Write-Step "Data Exporter -- Prerequisites installer"
Write-Info "Kit root  : $KitRoot"
Write-Info "Installers: $Installers"

if (-not (Test-Path $Installers)) {
    Write-Fail "Installers folder not found: $Installers"
    Write-Info "Make sure you are running this script from inside the transfer-kit folder."
    exit 1
}

# tar.exe ships with Windows 10 1803 and later -- needed to extract Kafka.
# Check up front so we fail loud BEFORE installing Python / Java.
$tarPath = (Get-Command tar.exe -ErrorAction SilentlyContinue).Source
if (-not $tarPath) {
    Write-Fail "tar.exe is not available on this system."
    Write-Info "tar.exe ships with Windows 10 (April 2018 update / build 1803) and later."
    Write-Info "Either upgrade Windows, or extract kafka_2.13-3.9.0.tgz manually to C:\kafka before retrying."
    exit 1
}
Write-Info "tar.exe   : $tarPath"

# ---------------------------------------------------------------------------
# 1. Python 3.10
# ---------------------------------------------------------------------------
Write-Step "1/4  Python 3.10"

function Test-Python310 {
    try {
        $v = & py -3.10 -c "import sys; print(sys.version_info[:2])" 2>$null
        if ($LASTEXITCODE -eq 0 -and $v -match '\(3, 10\)') { return $true }
    } catch {}
    return $false
}

if (Test-Python310) {
    Write-Skip "Python 3.10 already installed (py -3.10 works)"
} else {
    $pyInstaller = Join-Path $Installers 'python-3.10.11-amd64.exe'
    if (-not (Test-Path $pyInstaller)) { Write-Fail "Missing: $pyInstaller"; exit 1 }
    Write-Info "Installing Python 3.10.11 (silent, all users, PATH on)..."
    $args = @(
        '/quiet',
        'InstallAllUsers=1',
        'PrependPath=1',
        'Include_test=0',
        'Include_doc=0',
        'Include_launcher=1',
        'Include_pip=1'
    )
    Start-Process -FilePath $pyInstaller -ArgumentList $args -Wait
    Refresh-EnvPath
    if (-not (Test-Python310)) { Write-Fail "Python install completed but py -3.10 still doesn't work."; exit 1 }
    Write-OK "Python 3.10 installed"
}

# ---------------------------------------------------------------------------
# 2. Java JDK 17 (Temurin / Eclipse Adoptium)
# ---------------------------------------------------------------------------
Write-Step "2/4  Java JDK 17"

function Find-Jdk17 {
    $candidates = @()
    $envHome = $env:JAVA_HOME
    if ($envHome) { $candidates += $envHome }
    $adoptium = 'C:\Program Files\Eclipse Adoptium'
    if (Test-Path $adoptium) {
        $candidates += (Get-ChildItem $adoptium -Directory |
                        Where-Object { $_.Name -like 'jdk-17*' } |
                        Sort-Object Name -Descending |
                        Select-Object -ExpandProperty FullName)
    }
    foreach ($c in $candidates) {
        if ($c -and (Test-Path (Join-Path $c 'bin\java.exe'))) { return $c }
    }
    return $null
}

$jdkPath = Find-Jdk17
if ($jdkPath) {
    Write-Skip "Java 17 already installed at: $jdkPath"
} else {
    $javaInstaller = Join-Path $Installers 'OpenJDK17U-jdk_x64_windows_hotspot_17.0.18_8.msi'
    if (-not (Test-Path $javaInstaller)) { Write-Fail "Missing: $javaInstaller"; exit 1 }
    Write-Info "Installing Java JDK 17 (silent)..."
    $args = @('/i', "`"$javaInstaller`"", '/quiet', '/norestart',
              'ADDLOCAL=FeatureMain,FeatureEnvironment,FeatureJarFileRunWith,FeatureJavaHome')
    Start-Process -FilePath 'msiexec.exe' -ArgumentList $args -Wait
    $jdkPath = Find-Jdk17
    if (-not $jdkPath) { Write-Fail "Java install completed but no jdk-17 folder found."; exit 1 }
    Write-OK "Java 17 installed at: $jdkPath"
}

# Set JAVA_HOME (machine-wide) if not already correct
$currentJavaHome = [Environment]::GetEnvironmentVariable('JAVA_HOME', 'Machine')
if ($currentJavaHome -ne $jdkPath) {
    [Environment]::SetEnvironmentVariable('JAVA_HOME', $jdkPath, 'Machine')
    $env:JAVA_HOME = $jdkPath
    Write-OK "Set JAVA_HOME=$jdkPath (machine scope)"
} else {
    Write-Skip "JAVA_HOME already set to $currentJavaHome"
}

# ---------------------------------------------------------------------------
# 3. ODBC Driver 17 for SQL Server
# ---------------------------------------------------------------------------
Write-Step "3/4  ODBC Driver 17 for SQL Server"

function Test-OdbcDriver17 {
    $regPath = 'HKLM:\SOFTWARE\ODBC\ODBCINST.INI\ODBC Driver 17 for SQL Server'
    return (Test-Path $regPath)
}

if (Test-OdbcDriver17) {
    Write-Skip "ODBC Driver 17 already installed"
} else {
    $odbcInstaller = Join-Path $Installers 'msodbcsql.msi'
    if (-not (Test-Path $odbcInstaller)) { Write-Fail "Missing: $odbcInstaller"; exit 1 }
    Write-Info "Installing ODBC Driver 17 (silent)..."
    $args = @('/i', "`"$odbcInstaller`"", '/quiet', '/norestart',
              'IACCEPTMSODBCSQLLICENSETERMS=YES')
    Start-Process -FilePath 'msiexec.exe' -ArgumentList $args -Wait
    if (-not (Test-OdbcDriver17)) { Write-Fail "ODBC install completed but registry key not found."; exit 1 }
    Write-OK "ODBC Driver 17 installed"
}

# ---------------------------------------------------------------------------
# 4. Kafka 3.9.0 (extract to C:\kafka)
# ---------------------------------------------------------------------------
Write-Step "4/4  Apache Kafka 3.9.0"

$kafkaTarget = 'C:\kafka'
$kafkaStartBat = Join-Path $kafkaTarget 'bin\windows\kafka-server-start.bat'

if (Test-Path $kafkaStartBat) {
    Write-Skip "Kafka already extracted at $kafkaTarget"
} else {
    $kafkaArchive = Join-Path $Installers 'kafka_2.13-3.9.0.tgz'
    if (-not (Test-Path $kafkaArchive)) { Write-Fail "Missing: $kafkaArchive"; exit 1 }

    if (Test-Path $kafkaTarget) {
        Write-Info "Removing partial $kafkaTarget before fresh extract..."
        Remove-Item -Recurse -Force $kafkaTarget
    }

    Write-Info "Extracting Kafka to $kafkaTarget (uses Windows built-in tar, ~30s)..."
    New-Item -ItemType Directory -Path $kafkaTarget | Out-Null
    & tar.exe -xzf $kafkaArchive -C $kafkaTarget --strip-components=1
    if ($LASTEXITCODE -ne 0) { Write-Fail "tar extraction failed"; exit 1 }

    if (-not (Test-Path $kafkaStartBat)) { Write-Fail "Extraction OK but $kafkaStartBat missing"; exit 1 }
    Write-OK "Kafka extracted to $kafkaTarget"
}

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
Write-Step "All prerequisites installed"
Write-OK "Python 3.10  : $(& py -3.10 --version 2>&1)"
Write-OK "Java 17      : $jdkPath"
Write-OK "ODBC 17      : registered"
Write-OK "Kafka 3.9.0  : $kafkaTarget"
Write-Host ""
Write-Host "Next step: run 2-deploy-project.ps1 (no admin needed)" -ForegroundColor Cyan
Write-Host ""
