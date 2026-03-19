$ErrorActionPreference = "Continue"
$token = az account get-access-token --resource "https://api.fabric.microsoft.com" --query accessToken -o tsv
$wsId = "WORKSPACE_ID_PLACEHOLDER"
$headers = @{ Authorization = "Bearer $token"; "Content-Type" = "application/json" }

$sourceId  = "4cd508fe-2f57-4295-8a59-36eb184b78c9"
$landingId = "f7350150-8fa3-475f-a49b-ffc07120019e"
$bronzeId  = "37cdb7a3-0f57-4c23-b1ba-eb9cd03c16ac"
$silverId  = "0e2d0128-71e0-45bc-9436-1e30ff1e6925"
$goldId    = "6b079648-ff2e-4ff1-8e98-8765a7e6c20c"

# Notebook ID map
$nbs = @()
$uri = "https://api.fabric.microsoft.com/v1/workspaces/$wsId/items?type=Notebook"
while ($uri) {
    $resp = Invoke-RestMethod -Uri $uri -Method GET -Headers $headers
    $nbs += $resp.value
    $uri = if ($resp.PSObject.Properties.Match("continuationUri").Count -gt 0) { $resp.continuationUri } else { $null }
}
$nbMap = @{}
foreach ($nb in $nbs) { $nbMap[$nb.displayName] = $nb.id }
Write-Host "Found $($nbMap.Count) notebooks"

# Get existing pipelines
$allPipelines = Invoke-RestMethod -Uri "https://api.fabric.microsoft.com/v1/workspaces/$wsId/items?type=DataPipeline" -Method GET -Headers $headers

$pipelinesDir = Join-Path $PSScriptRoot "assets" "pipelines"
foreach ($pFile in (Get-ChildItem $pipelinesDir -Filter "*.json" | Sort-Object Name)) {
    $pName = [System.IO.Path]::GetFileNameWithoutExtension($pFile.Name)
    $template = Get-Content $pFile.FullName -Raw -Encoding UTF8

    $template = $template.Replace('{{WORKSPACE_ID}}', $wsId)
    foreach ($kvp in $nbMap.GetEnumerator()) {
        $template = $template.Replace("{{NB_$($kvp.Key)}}", $kvp.Value)
    }
    $template = $template.Replace('{{SOURCE_LH}}', $sourceId)
    $template = $template.Replace('{{LANDING_LH}}', $landingId)
    $template = $template.Replace('{{BRONZE_LH}}', $bronzeId)
    $template = $template.Replace('{{SILVER_LH}}', $silverId)
    $template = $template.Replace('{{GOLD_LH}}', $goldId)

    $payloadBase64 = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($template))
    $definition = @{
        parts = @(@{ path = "pipeline-content.json"; payload = $payloadBase64; payloadType = "InlineBase64" })
    }

    $match = $allPipelines.value | Where-Object { $_.displayName -eq $pName } | Select-Object -First 1
    if ($match) {
        try {
            $body = @{ definition = $definition } | ConvertTo-Json -Depth 20 -Compress
            Invoke-WebRequest -Uri "https://api.fabric.microsoft.com/v1/workspaces/$wsId/items/$($match.id)/updateDefinition" -Method POST -Headers $headers -Body $body -UseBasicParsing | Out-Null
            Write-Host "  $pName -- updated"
        } catch {
            Write-Warning "  $pName -- FAILED: $_"
        }
    } else {
        Write-Host "  $pName -- NOT FOUND, skipping"
    }
}

# Now trigger seed pipeline
$seedPl = $allPipelines.value | Where-Object { $_.displayName -eq "pl_ibp_seed_test_data" } | Select-Object -First 1
Write-Host "`nTriggering seed pipeline..."
$runResp = Invoke-WebRequest -Uri "https://api.fabric.microsoft.com/v1/workspaces/$wsId/items/$($seedPl.id)/jobs/instances?jobType=Pipeline" -Method POST -Headers $headers -UseBasicParsing
Write-Host "Status: $($runResp.StatusCode)"

$jobId = $null
if ($runResp.Headers.ContainsKey("Location")) {
    $loc = $runResp.Headers["Location"]
    $opUrl = if ($loc -is [array]) { $loc[0] } else { $loc }
    if ($opUrl -match '/instances/([^/\?]+)') { $jobId = $Matches[1] }
    Write-Host "Job ID: $jobId"
}

# Poll for completion
if ($jobId) {
    $pollUrl = "https://api.fabric.microsoft.com/v1/workspaces/$wsId/items/$($seedPl.id)/jobs/instances/$jobId"
    for ($i = 0; $i -lt 40; $i++) {
        Start-Sleep -Seconds 15
        try {
            $token = az account get-access-token --resource "https://api.fabric.microsoft.com" --query accessToken -o tsv
            $pollHeaders = @{ Authorization = "Bearer $token" }
            $resp = Invoke-RestMethod -Uri $pollUrl -Method GET -Headers $pollHeaders
            $status = $resp.status
            Write-Host "[$(Get-Date -Format 'HH:mm:ss')] Status: $status"
            if ($status -eq "Completed") {
                Write-Host "Seed pipeline completed!"
                break
            }
            if ($status -eq "Failed" -or $status -eq "Cancelled") {
                Write-Host "Pipeline $status!"
                if ($resp.PSObject.Properties.Match("failureReason").Count -gt 0) {
                    Write-Host "Reason: $($resp.failureReason.message)"
                }
                break
            }
        } catch {
            Write-Host "[$(Get-Date -Format 'HH:mm:ss')] Poll error, retrying..."
        }
    }
}
