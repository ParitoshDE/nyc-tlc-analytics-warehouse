param(
    [string]$ProjectId = $env:GCP_PROJECT_ID,
    [string]$Region = $env:GCP_REGION,
    [string]$ComposerEnvName = "ecom-pipeline-composer",
    [string]$DagId = "nyc_tlc_analytics_pipeline",
    [int]$TimeoutMinutes = 45,
    [int]$PollSeconds = 30
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
$PSNativeCommandUseErrorActionPreference = $true

if (-not $ProjectId) {
    throw "ProjectId is required. Pass -ProjectId or set GCP_PROJECT_ID."
}
if (-not $Region) {
    $Region = "us-central1"
}
if ($TimeoutMinutes -lt 1) {
    throw "TimeoutMinutes must be >= 1"
}
if ($PollSeconds -lt 5) {
    throw "PollSeconds must be >= 5"
}

$repoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $repoRoot

Write-Output "[prod-run] Validating Composer environment"
gcloud composer environments describe $ComposerEnvName --location $Region --project $ProjectId --format "value(name)" | Out-Null

Write-Output "[prod-run] Deploying DAG and runtime files"
powershell -ExecutionPolicy Bypass -File "$repoRoot/scripts/deploy_to_composer.ps1" -ProjectId $ProjectId -Region $Region -ComposerEnvName $ComposerEnvName

$runId = "manual_$(Get-Date -Format 'yyyyMMdd_HHmmss')"
Write-Output "[prod-run] Triggering DAG '$DagId' with run-id '$runId'"
$composerRunCmd = "gcloud composer environments run $ComposerEnvName --location $Region --project $ProjectId"
cmd /c "$composerRunCmd dags -- trigger $DagId --run-id $runId"

$deadline = (Get-Date).AddMinutes($TimeoutMinutes)
Write-Output "[prod-run] Waiting up to $TimeoutMinutes minutes for completion"

while ((Get-Date) -lt $deadline) {
    $runsText = cmd /c "$composerRunCmd dags list-runs -- -d $DagId" | Out-String
    $line = ($runsText -split "`r?`n" | Where-Object { $_ -match [regex]::Escape($runId) } | Select-Object -First 1)

    if ($line) {
        $cols = $line -split "\|"
        if ($cols.Length -ge 3) {
            $state = $cols[2].Trim().ToLowerInvariant()
            Write-Output "[prod-run] Run $runId state: $state"

            if ($state -eq "success") {
                Write-Output "[prod-run] Completed successfully"
                exit 0
            }

            if ($state -in @("failed", "upstream_failed")) {
                throw "DAG run $runId failed with state '$state'"
            }
        }
    }

    Start-Sleep -Seconds $PollSeconds
}

throw "Timed out after $TimeoutMinutes minutes waiting for DAG run $runId"
