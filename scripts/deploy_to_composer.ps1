param(
    [string]$ProjectId = $env:GCP_PROJECT_ID,
    [string]$Region = $env:GCP_REGION,
    [string]$ComposerEnvName = "nyc-tlc-analytics-composer"
)

$ErrorActionPreference = "Stop"

if (-not $ProjectId) {
    throw "ProjectId is required. Pass -ProjectId or set GCP_PROJECT_ID."
}
if (-not $Region) {
    $Region = "us-central1"
}

$repoRoot = Split-Path -Parent $PSScriptRoot
Write-Output "[deploy] Repo root: $repoRoot"

$dagPrefix = gcloud composer environments describe $ComposerEnvName `
    --location $Region `
    --project $ProjectId `
    --format "value(config.dagGcsPrefix)"

if (-not $dagPrefix) {
    throw "Could not resolve Composer DAG GCS prefix for environment '$ComposerEnvName'."
}

$targetRoot = "$dagPrefix/nyc-tlc-analytics-warehouse"
Write-Output "[deploy] Composer DAG path: $targetRoot"

$pipelineSa = gcloud iam service-accounts list `
    --project $ProjectId `
    --filter "email~nyc-tlc-pipeline-sa" `
    --format "value(email)" | Select-Object -First 1

$composerRunBase = "gcloud composer environments run $ComposerEnvName --location $Region --project $ProjectId"

# Sync directories needed at runtime.
$dirMappings = @(
    @{ src = "$repoRoot/airflow"; dst = "$targetRoot/airflow" },
    @{ src = "$repoRoot/scripts"; dst = "$targetRoot/scripts" },
    @{ src = "$repoRoot/spark"; dst = "$targetRoot/spark" },
    @{ src = "$repoRoot/dbt"; dst = "$targetRoot/dbt" }
)

foreach ($m in $dirMappings) {
    if (Test-Path $m.src) {
        Write-Output "[deploy] Sync dir: $($m.src) -> $($m.dst)"
        gcloud storage rsync $m.src $m.dst --recursive
    }
}

# Copy top-level support files.
$fileMappings = @(
    @{ src = "$repoRoot/README.md"; dst = "$targetRoot/README.md" },
    @{ src = "$repoRoot/requirements.txt"; dst = "$targetRoot/requirements.txt" },
    @{ src = "$repoRoot/.env.example"; dst = "$targetRoot/.env.example" }
)

foreach ($m in $fileMappings) {
    if (Test-Path $m.src) {
        Write-Output "[deploy] Copy file: $($m.src) -> $($m.dst)"
        gcloud storage cp $m.src $m.dst
    }
}

# Promote DAG entrypoint to Composer top-level dags folder.
$dagFile = "$repoRoot/airflow/dags/nyc_tlc_pipeline_dag.py"
$composerDagEntry = "$dagPrefix/nyc_tlc_pipeline_dag.py"
if (Test-Path $dagFile) {
    Write-Output "[deploy] Publish DAG entrypoint: $composerDagEntry"
    gcloud storage cp $dagFile $composerDagEntry
}

# Load selected .env values into Airflow Variables for templated export in DAG.
$envPath = "$repoRoot/.env"
if (Test-Path $envPath) {
    Write-Output "[deploy] Syncing Airflow Variables from .env"
    $allowed = @(
        "GCP_PROJECT_ID",
        "GCP_REGION",
        "GCS_BUCKET",
        "BQ_RAW_DATASET",
        "BQ_PROD_DATASET",
        "TLC_TAXI_TYPE",
        "TLC_START_MONTH",
        "TLC_END_MONTH"
    )

    $envLines = Get-Content $envPath
    foreach ($line in $envLines) {
        $trim = $line.Trim()
        if (-not $trim -or $trim.StartsWith("#") -or -not $trim.Contains("=")) {
            continue
        }

        $parts = $trim.Split("=", 2)
        $key = $parts[0].Trim()
        $value = $parts[1].Trim()

        if ($allowed -contains $key) {
            Write-Output "[deploy] set variable: $key"
            cmd /c "$composerRunBase variables -- set $key $value"
        }
    }
}

Write-Output "[deploy] set variable: COMPOSER_REPO_ROOT_GCS"
cmd /c "$composerRunBase variables -- set COMPOSER_REPO_ROOT_GCS $targetRoot"

if ($pipelineSa) {
    Write-Output "[deploy] set variable: PIPELINE_SERVICE_ACCOUNT"
    cmd /c "$composerRunBase variables -- set PIPELINE_SERVICE_ACCOUNT $pipelineSa"
}

Write-Output "[deploy] Unpausing DAG nyc_tlc_analytics_pipeline"
cmd /c "$composerRunBase dags -- unpause nyc_tlc_analytics_pipeline"

Write-Output "[deploy] Done. Use scripts/trigger_composer_dag.ps1 to trigger a run."
