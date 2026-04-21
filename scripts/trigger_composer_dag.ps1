param(
    [string]$ProjectId = $env:GCP_PROJECT_ID,
    [string]$Region = $env:GCP_REGION,
    [string]$ComposerEnvName = "nyc-tlc-analytics-composer",
    [string]$DagId = "nyc_tlc_analytics_pipeline"
)

$ErrorActionPreference = "Stop"

if (-not $ProjectId) {
    throw "ProjectId is required. Pass -ProjectId or set GCP_PROJECT_ID."
}
if (-not $Region) {
    $Region = "us-central1"
}

$runId = "manual_$(Get-Date -Format 'yyyyMMdd_HHmmss')"
Write-Output "[trigger] Triggering DAG '$DagId' with run-id '$runId'"

$composerRunBase = "gcloud composer environments run $ComposerEnvName --location $Region --project $ProjectId"

# Some gcloud + PowerShell combinations drop forwarded args unless routed via cmd.
cmd /c "$composerRunBase dags -- trigger $DagId --run-id $runId"

Write-Output "[trigger] Recent DAG runs:"
cmd /c "$composerRunBase dags list-runs -- -d $DagId"
