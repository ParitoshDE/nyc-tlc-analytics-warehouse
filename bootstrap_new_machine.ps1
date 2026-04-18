$ErrorActionPreference = "Stop"

param(
    [string]$BaseDir = "C:\DataEng",
    [string]$RepoUrl = "https://github.com/ParitoshDE/nyc-tlc-analytics-warehouse.git",
    [string]$RepoName = "nyc-tlc-analytics-warehouse",
    [string]$GcpProject = "dataengkestra",
    [switch]$RunFullDbt
)

function Step([string]$Message) {
    Write-Host "`n=== $Message ===" -ForegroundColor Cyan
}

function Die([string]$Message) {
    Write-Host "ERROR: $Message" -ForegroundColor Red
    exit 1
}

function Require-Command([string]$CommandName) {
    if (-not (Get-Command $CommandName -ErrorAction SilentlyContinue)) {
        Die "$CommandName is not found in PATH. Install it first."
    }
}

function Load-DotEnv([string]$Path) {
    if (-not (Test-Path $Path)) {
        Die ".env file not found at $Path"
    }

    Get-Content $Path |
        Where-Object { $_ -match '^[A-Za-z_][A-Za-z0-9_]*=' } |
        ForEach-Object {
            $k, $v = $_ -split '=', 2
            [Environment]::SetEnvironmentVariable($k, $v, 'Process')
        }
}

$RepoDir = Join-Path $BaseDir $RepoName

Step "Checking required tools"
Require-Command "git"
Require-Command "python"
Require-Command "gcloud"
Write-Host "Tool check passed" -ForegroundColor Green

Step "Cloning/updating repository"
New-Item -ItemType Directory -Force -Path $BaseDir | Out-Null
Set-Location $BaseDir

if (-not (Test-Path $RepoDir)) {
    git clone $RepoUrl
}

Set-Location $RepoDir
git fetch --all
git checkout main
git pull --ff-only

Step "Creating and activating virtual environment"
if (-not (Test-Path ".\.venv\Scripts\Activate.ps1")) {
    python -m venv .venv
}

Set-ExecutionPolicy -Scope Process -ExecutionPolicy RemoteSigned
& .\.venv\Scripts\Activate.ps1

Step "Installing Python/dbt dependencies"
python -m pip install --upgrade pip
pip install -r requirements.txt
pip install dbt-bigquery==1.11.*

Step "Preparing .env"
if (-not (Test-Path ".\.env")) {
    Copy-Item .\.env.example .\.env
    Write-Host ".env created from .env.example" -ForegroundColor Yellow
}

$requiredKeys = @(
    "GCP_PROJECT_ID",
    "GCS_BUCKET",
    "BQ_RAW_DATASET",
    "BQ_PROD_DATASET",
    "GCP_REGION",
    "GOOGLE_APPLICATION_CREDENTIALS"
)

$envMap = @{}
Get-Content .\.env |
    Where-Object { $_ -match '^[A-Za-z_][A-Za-z0-9_]*=' } |
    ForEach-Object {
        $k, $v = $_ -split '=', 2
        $envMap[$k] = $v
    }

$missing = @()
foreach ($key in $requiredKeys) {
    if (-not $envMap.ContainsKey($key) -or [string]::IsNullOrWhiteSpace($envMap[$key])) {
        $missing += $key
    }
}

if ($missing.Count -gt 0) {
    Write-Host "Missing .env keys: $($missing -join ', ')" -ForegroundColor Yellow
    Write-Host "Open .env, fill values, save, and press Enter to continue." -ForegroundColor Yellow
    notepad .\.env
    Read-Host "Press Enter after updating .env"
}

Load-DotEnv ".\.env"

if (-not (Test-Path $env:GOOGLE_APPLICATION_CREDENTIALS)) {
    Die "GOOGLE_APPLICATION_CREDENTIALS path does not exist: $env:GOOGLE_APPLICATION_CREDENTIALS"
}

Step "Configuring/authenticating gcloud"
gcloud auth login
gcloud config set project $GcpProject
gcloud auth application-default login
gcloud auth application-default set-quota-project $GcpProject

Step "Checking GCS access"
gcloud storage buckets list --project=$GcpProject --format="value(name)" | Out-Null
Write-Host "GCS access check passed" -ForegroundColor Green

Step "Running dbt smoke checks"
Set-Location .\dbt
$env:DBT_PROFILES_DIR='.'

..\.venv\Scripts\dbt.exe deps
..\.venv\Scripts\dbt.exe parse
..\.venv\Scripts\dbt.exe run --select stg_events
..\.venv\Scripts\dbt.exe test --select stg_events

if ($RunFullDbt) {
    Step "Running full dbt run/test"
    ..\.venv\Scripts\dbt.exe run
    ..\.venv\Scripts\dbt.exe test
}

Step "Done"
Write-Host "Bootstrap completed successfully." -ForegroundColor Green
Write-Host "Repo: $RepoDir"
Write-Host "If you skipped full validation, run with -RunFullDbt when ready." -ForegroundColor Yellow
