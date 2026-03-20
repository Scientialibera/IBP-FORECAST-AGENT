<#
.SYNOPSIS
    Idempotent deployment of IBP Forecast lakehouses, folders, and notebooks
    to a Microsoft Fabric workspace.
.DESCRIPTION
    Reads deploy.config.toml, creates a top-level project folder, nests
    lakehouses and notebook folders inside it, then deploys all notebooks
    in parallel (fire all creates, batch-poll operations).
#>
param(
    [string]$ConfigPath = "$PSScriptRoot/deploy.config.toml"
)
Set-StrictMode -Version Latest
$ErrorActionPreference = "Continue"

# ── Config ──────────────────────────────────────────────────────
function Get-Config {
    param([string]$Path)
    if (-not (Test-Path $Path)) { throw "Config file not found: $Path" }
    $json = python -c "import json, pathlib, tomllib; p=pathlib.Path(r'$Path'); print(json.dumps(tomllib.loads(p.read_text(encoding='utf-8'))))"
    if ($LASTEXITCODE -ne 0) { throw "Failed to parse config file: $Path" }
    return $json | ConvertFrom-Json
}

$config = Get-Config -Path $ConfigPath

Write-Host "`n=== IBP Forecast -- Fabric Deployment ===" -ForegroundColor Cyan

# ── Token cache (refresh every 4 min) ──────────────────────────
$script:tokenCache = $null
$script:tokenTime  = [datetime]::MinValue

function Get-FabricToken {
    if (-not $script:tokenCache -or ([datetime]::UtcNow - $script:tokenTime).TotalMinutes -gt 4) {
        $script:tokenCache = az account get-access-token --resource "https://api.fabric.microsoft.com" --query accessToken -o tsv
        $script:tokenTime  = [datetime]::UtcNow
    }
    return $script:tokenCache
}

# ── Low-level API call (returns WebResponse, no LRO wait) ──────
function Invoke-FabricRaw {
    param([string]$Method, [string]$Uri, [object]$Body = $null)
    $token = Get-FabricToken
    $headers = @{ Authorization = "Bearer $token"; "Content-Type" = "application/json" }
    $params = @{ Uri = $Uri; Method = $Method; Headers = $headers; UseBasicParsing = $true }
    if ($Body) { $params.Body = ($Body | ConvertTo-Json -Depth 20 -Compress) }
    return Invoke-WebRequest @params
}

# ── Blocking API call (waits for LRO if 202) ───────────────────
function Invoke-FabricApi {
    param([string]$Method, [string]$Uri, [object]$Body = $null)
    $resp = Invoke-FabricRaw -Method $Method -Uri $Uri -Body $Body
    if ($resp.StatusCode -eq 202) { Wait-SingleOperation -Response $resp }
    if ($resp.Content -and $resp.Content -ne "null" -and $resp.Content.Length -gt 2) {
        return $resp.Content | ConvertFrom-Json
    }
    return $null
}

function Wait-SingleOperation {
    param($Response)
    $opUrl = $null
    if ($Response.Headers.ContainsKey("Location")) {
        $v = $Response.Headers["Location"]; $opUrl = if ($v -is [array]) { $v[0] } else { $v }
    }
    if (-not $opUrl) { return }
    for ($i = 0; $i -lt 20; $i++) {
        Start-Sleep -Seconds 5
        try {
            $token = Get-FabricToken
            $poll = Invoke-WebRequest -Uri $opUrl -Method GET -Headers @{ Authorization = "Bearer $token" } -UseBasicParsing
            if ($poll.Content -and $poll.Content -ne "null") {
                $body = $poll.Content | ConvertFrom-Json
                if ($body.PSObject.Properties.Match("status").Count -gt 0) {
                    if ($body.status -eq "Succeeded" -or $body.status -eq "Completed") { return }
                    if ($body.status -eq "Failed") {
                        $msg = if ($body.PSObject.Properties.Match("error").Count -gt 0) { $body.error.message } else { "unknown" }
                        Write-Warning "LRO failed: $msg"
                        return
                    }
                }
            }
        } catch { Start-Sleep -Seconds 3 }
    }
    Write-Warning "LRO polling timed out -- will complete in background"
}

function Get-FabricItems {
    param([string]$WorkspaceId, [string]$Type)
    $items = @()
    $token = Get-FabricToken
    $headers = @{ Authorization = "Bearer $token" }
    $uri = "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/items?type=$Type"
    while ($uri) {
        $resp = Invoke-RestMethod -Uri $uri -Method GET -Headers $headers
        $items += $resp.value
        $uri = if ($resp.PSObject.Properties.Match("continuationUri").Count -gt 0) { $resp.continuationUri } else { $null }
    }
    return $items
}

# ── Resolve workspace ID ───────────────────────────────────────
$workspaceId = $config.fabric.workspace_id
if ([string]::IsNullOrWhiteSpace($workspaceId)) {
    $wsName = $config.fabric.workspace_name
    if ([string]::IsNullOrWhiteSpace($wsName)) { throw "Set fabric.workspace_id or fabric.workspace_name." }
    Write-Host "Looking up workspace '$wsName'..."
    $token = Get-FabricToken
    $allWs = Invoke-RestMethod -Uri "https://api.fabric.microsoft.com/v1/workspaces" -Method GET -Headers @{ Authorization = "Bearer $token" }
    $match = $allWs.value | Where-Object { $_.displayName -eq $wsName } | Select-Object -First 1
    if (-not $match) { throw "Workspace '$wsName' not found." }
    $workspaceId = $match.id
}
Write-Host "Workspace: $workspaceId"

# ── Folder helper ───────────────────────────────────────────────
function Ensure-FabricFolder {
    param([string]$WorkspaceId, [string]$FolderName, [string]$ParentFolderId)
    $token = Get-FabricToken
    $folders = Invoke-RestMethod -Uri "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/folders" -Method GET -Headers @{ Authorization = "Bearer $token" }
    foreach ($f in $folders.value) {
        if ($f.displayName -ne $FolderName) { continue }
        $fpid = if ($f.PSObject.Properties.Match("parentFolderId").Count -gt 0) { $f.parentFolderId } else { $null }
        if ((-not $ParentFolderId -and -not $fpid) -or ($ParentFolderId -and $fpid -eq $ParentFolderId)) {
            Write-Host "  Folder '$FolderName' -- exists: $($f.id)"
            return $f.id
        }
    }
    $body = @{ displayName = $FolderName }
    if ($ParentFolderId) { $body.parentFolderId = $ParentFolderId }
    $created = Invoke-FabricApi -Method "POST" -Uri "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/folders" -Body $body
    $cid = if ($created -and $created.PSObject.Properties.Match("id").Count -gt 0) { $created.id } else { $null }
    if (-not $cid) {
        Start-Sleep -Seconds 3
        $token2 = Get-FabricToken
        $folders2 = Invoke-RestMethod -Uri "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/folders" -Method GET -Headers @{ Authorization = "Bearer $token2" }
        foreach ($f in $folders2.value) {
            if ($f.displayName -ne $FolderName) { continue }
            $fpid = if ($f.PSObject.Properties.Match("parentFolderId").Count -gt 0) { $f.parentFolderId } else { $null }
            if ((-not $ParentFolderId -and -not $fpid) -or ($ParentFolderId -and $fpid -eq $ParentFolderId)) { $cid = $f.id; break }
        }
    }
    Write-Host "  Folder '$FolderName' -- created: $cid"
    return $cid
}

# ── Lakehouse helper ────────────────────────────────────────────
function Ensure-FabricLakehouse {
    param([string]$WorkspaceId, [string]$LakehouseId, [string]$LakehouseName, [string]$FolderId)
    if (-not [string]::IsNullOrWhiteSpace($LakehouseId)) {
        Write-Host "  Lakehouse '$LakehouseName' -- using ID: $LakehouseId"; return $LakehouseId
    }
    $existing = Get-FabricItems -WorkspaceId $WorkspaceId -Type "Lakehouse" | Where-Object { $_.displayName -eq $LakehouseName } | Select-Object -First 1
    if ($existing) { Write-Host "  Lakehouse '$LakehouseName' -- exists: $($existing.id)"; return $existing.id }
    $body = @{ displayName = $LakehouseName; type = "Lakehouse" }
    if (-not [string]::IsNullOrWhiteSpace($FolderId)) { $body.folderId = $FolderId }
    Invoke-FabricApi -Method "POST" -Uri "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/items" -Body $body | Out-Null
    Start-Sleep -Seconds 3
    $items = Get-FabricItems -WorkspaceId $WorkspaceId -Type "Lakehouse"
    $found = $items | Where-Object { $_.displayName -eq $LakehouseName } | Select-Object -First 1
    $cid = if ($found) { $found.id } else { "pending" }
    Write-Host "  Lakehouse '$LakehouseName' -- created: $cid"
    return $cid
}

# ── PARALLEL notebook deployment ────────────────────────────────
function Deploy-NotebooksParallel {
    param([string]$WorkspaceId, [string]$FolderId, [string]$LocalDir, [string]$Label)

    $files = Get-ChildItem $LocalDir -Filter "*.py" | Sort-Object Name
    if ($files.Count -eq 0) { Write-Host "  No notebooks in $LocalDir"; return }

    $existing = Get-FabricItems -WorkspaceId $WorkspaceId -Type "Notebook"
    $existingMap = @{}
    foreach ($nb in $existing) { $existingMap[$nb.displayName] = $nb.id }

    $operations = @()
    $baseUri = "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId"

    foreach ($file in $files) {
        $name = [System.IO.Path]::GetFileNameWithoutExtension($file.Name)
        $source = Get-Content -Path $file.FullName -Raw -Encoding UTF8
        $payloadBase64 = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($source))
        $definition = @{
            format = "fabricGitSource"
            parts = @(@{ path = "notebook-content.py"; payload = $payloadBase64; payloadType = "InlineBase64" })
        }

        try {
            if ($existingMap.ContainsKey($name)) {
                $resp = Invoke-FabricRaw -Method "POST" -Uri "$baseUri/items/$($existingMap[$name])/updateDefinition" -Body @{ definition = $definition }
                Write-Host "  $name -- update fired"
            } else {
                $body = @{ displayName = $name; type = "Notebook"; definition = $definition }
                if ($FolderId) { $body.folderId = $FolderId }
                $resp = Invoke-FabricRaw -Method "POST" -Uri "$baseUri/items" -Body $body
                Write-Host "  $name -- create fired"
            }

            if ($resp.StatusCode -eq 202 -and $resp.Headers.ContainsKey("Location")) {
                $loc = $resp.Headers["Location"]; $opUrl = if ($loc -is [array]) { $loc[0] } else { $loc }
                $operations += @{ name = $name; url = $opUrl }
            }
        } catch {
            Write-Warning "  $name -- FAILED: $_"
        }
    }

    if ($operations.Count -eq 0) {
        Write-Host "  All $Label notebooks dispatched (no async ops to poll)."
        return
    }

    Write-Host "`n  Waiting for $($operations.Count) $Label operations..." -ForegroundColor Gray
    $pending = [System.Collections.ArrayList]::new($operations)
    $maxWait = 60
    $elapsed = 0
    $interval = 5

    while ($pending.Count -gt 0 -and $elapsed -lt $maxWait) {
        Start-Sleep -Seconds $interval
        $elapsed += $interval
        $token = Get-FabricToken
        $headers = @{ Authorization = "Bearer $token" }
        $done = @()

        foreach ($op in $pending) {
            try {
                $poll = Invoke-WebRequest -Uri $op.url -Method GET -Headers $headers -UseBasicParsing
                if ($poll.Content -and $poll.Content -ne "null") {
                    $body = $poll.Content | ConvertFrom-Json
                    if ($body.PSObject.Properties.Match("status").Count -gt 0) {
                        if ($body.status -eq "Succeeded" -or $body.status -eq "Completed") {
                            Write-Host "  $($op.name) -- done" -ForegroundColor Green
                            $done += $op
                        } elseif ($body.status -eq "Failed") {
                            $emsg = if ($body.PSObject.Properties.Match("error").Count -gt 0) { $body.error.message } else { "unknown" }
                            Write-Warning "  $($op.name) -- FAILED: $emsg"
                            $done += $op
                        }
                    }
                }
            } catch { <# transient, retry next cycle #> }
        }

        foreach ($d in $done) { $pending.Remove($d) | Out-Null }
        if ($pending.Count -gt 0) {
            $names = ($pending | ForEach-Object { $_.name }) -join ", "
            Write-Host "    [$elapsed`s] waiting: $names" -ForegroundColor Gray
        }
    }

    if ($pending.Count -gt 0) {
        $names = ($pending | ForEach-Object { $_.name }) -join ", "
        Write-Warning "  Timed out waiting for: $names -- will complete in background."
    }
}

# ─────────────────────────────────────────────────────────────────
# MAIN FLOW
# ─────────────────────────────────────────────────────────────────

$projectFolderName = $config.naming.project_folder
if ([string]::IsNullOrWhiteSpace($projectFolderName)) { $projectFolderName = "IBP Forecast" }

# 1. Folders
Write-Host "`n[1/8] Creating project folder '$projectFolderName'..." -ForegroundColor Yellow
# Semantic model is created by notebook 15, not here -- infra only creates the folder
$projectFolderId   = Ensure-FabricFolder -WorkspaceId $workspaceId -FolderName $projectFolderName
Write-Host "`n[2/8] Creating sub-folders..." -ForegroundColor Yellow
$dataFolderId      = Ensure-FabricFolder -WorkspaceId $workspaceId -FolderName "data"      -ParentFolderId $projectFolderId
$notebooksFolderId   = Ensure-FabricFolder -WorkspaceId $workspaceId -FolderName "notebooks"   -ParentFolderId $projectFolderId
$pipelinesFolderId   = Ensure-FabricFolder -WorkspaceId $workspaceId -FolderName "pipelines"   -ParentFolderId $projectFolderId
$experimentsFolderId = Ensure-FabricFolder -WorkspaceId $workspaceId -FolderName "experiments" -ParentFolderId $projectFolderId
$semanticFolderId    = Ensure-FabricFolder -WorkspaceId $workspaceId -FolderName "semantic_models" -ParentFolderId $projectFolderId
$mainFolderId        = Ensure-FabricFolder -WorkspaceId $workspaceId -FolderName "main"        -ParentFolderId $notebooksFolderId
$modulesFolderId     = Ensure-FabricFolder -WorkspaceId $workspaceId -FolderName "modules"     -ParentFolderId $notebooksFolderId

# 2b. MLflow Experiment (infrastructure -- must exist in experiments/ before notebooks run)
$experimentName = $config.mlflow.experiment_name
if (-not [string]::IsNullOrWhiteSpace($experimentName)) {
    $existingExp = Get-FabricItems -WorkspaceId $workspaceId -Type "MLExperiment" | Where-Object { $_.displayName -eq $experimentName }
    $inFolder = $null
    foreach ($e in $existingExp) {
        $detail = Invoke-RestMethod -Uri "https://api.fabric.microsoft.com/v1/workspaces/$workspaceId/items/$($e.id)" -Method GET -Headers @{ Authorization = "Bearer $(Get-FabricToken)" }
        if ($detail.folderId -eq $experimentsFolderId) {
            $inFolder = $e
            Write-Host "  Experiment '$experimentName' -- exists in experiments/: $($e.id)"
        } else {
            Write-Host "  Deleting stale experiment '$experimentName' at wrong location (folder: $($detail.folderId))..."
            try { Invoke-RestMethod -Uri "https://api.fabric.microsoft.com/v1/workspaces/$workspaceId/items/$($e.id)" -Method DELETE -Headers @{ Authorization = "Bearer $(Get-FabricToken)" } } catch {}
        }
    }
    if (-not $inFolder) {
        try {
            $body = @{ displayName = $experimentName; type = "MLExperiment"; folderId = $experimentsFolderId }
            Invoke-FabricApi -Method "POST" -Uri "https://api.fabric.microsoft.com/v1/workspaces/$workspaceId/items" -Body $body | Out-Null
            Write-Host "  Experiment '$experimentName' -- created in experiments/"
        } catch {
            Write-Warning "  Experiment creation failed: $_ -- notebooks will handle it"
        }
    }
}

# 3. Lakehouses
Write-Host "`n[3/8] Creating lakehouses..." -ForegroundColor Yellow
$lakehouseIds = @{}
foreach ($entry in @(
    @{ key = "source";  id = $config.source.source_lakehouse_id;  name = $config.source.source_lakehouse_name },
    @{ key = "landing"; id = $config.lakehouses.landing_id; name = $config.lakehouses.landing_name },
    @{ key = "bronze";  id = $config.lakehouses.bronze_id;  name = $config.lakehouses.bronze_name },
    @{ key = "silver";  id = $config.lakehouses.silver_id;  name = $config.lakehouses.silver_name },
    @{ key = "gold";    id = $config.lakehouses.gold_id;    name = $config.lakehouses.gold_name }
)) {
    try {
        $lakehouseIds[$entry.key] = Ensure-FabricLakehouse -WorkspaceId $workspaceId -LakehouseId $entry.id -LakehouseName $entry.name -FolderId $dataFolderId
    } catch {
        Write-Warning "  Lakehouse '$($entry.name)' -- ERROR: $_"
        $lakehouseIds[$entry.key] = "error"
    }
}
$sourceId = $lakehouseIds["source"]; $landingId = $lakehouseIds["landing"]; $bronzeId = $lakehouseIds["bronze"]; $silverId = $lakehouseIds["silver"]; $goldId = $lakehouseIds["gold"]

# 3a. Convert notebooks (plain .py → Fabric format, output to build/)
$buildDir = Join-Path (Join-Path $PSScriptRoot "build") "notebooks"
Write-Host "`n[4/8] Converting notebooks with lakehouse bindings..." -ForegroundColor Yellow
$convertScript = Join-Path $PSScriptRoot "convert_notebooks.py"
python $convertScript `
    --workspace-id $workspaceId `
    --source-id $sourceId --source-name $config.source.source_lakehouse_name `
    --landing-id $landingId --landing-name $config.lakehouses.landing_name `
    --bronze-id $bronzeId --bronze-name $config.lakehouses.bronze_name `
    --silver-id $silverId --silver-name $config.lakehouses.silver_name `
    --gold-id $goldId --gold-name $config.lakehouses.gold_name `
    --output-dir $buildDir

# 4. Notebooks -- PARALLEL (deploy from build/ directory)
Write-Host "`n[5/8] Deploying module notebooks (parallel)..." -ForegroundColor Yellow
$modulesDir = Join-Path $buildDir "modules"
Deploy-NotebooksParallel -WorkspaceId $workspaceId -FolderId $modulesFolderId -LocalDir $modulesDir -Label "module"

Write-Host "`n[6/8] Deploying main notebooks (parallel)..." -ForegroundColor Yellow
$mainDir = Join-Path $buildDir "main"
Deploy-NotebooksParallel -WorkspaceId $workspaceId -FolderId $mainFolderId -LocalDir $mainDir -Label "main"

# 6. Generate pipeline JSON definitions
Write-Host "`n[7/8] Generating pipeline definitions..." -ForegroundColor Yellow
$genScript = Join-Path $PSScriptRoot "generate_pipelines.py"
python $genScript

# 7. Deploy Data Pipelines into the pipelines/ folder
Write-Host "`n[8/8] Deploying Fabric Data Pipelines..." -ForegroundColor Yellow
$pipelinesDir = Join-Path (Join-Path $PSScriptRoot "assets") "pipelines"
if (Test-Path $pipelinesDir) {
    $allNotebooks = Get-FabricItems -WorkspaceId $workspaceId -Type "Notebook"
    $nbMap = @{}
    foreach ($nb in $allNotebooks) { $nbMap[$nb.displayName] = $nb.id }

    foreach ($pFile in (Get-ChildItem $pipelinesDir -Filter "*.json" | Sort-Object Name)) {
        $pName = [System.IO.Path]::GetFileNameWithoutExtension($pFile.Name)
        $template = Get-Content $pFile.FullName -Raw -Encoding UTF8

        $template = $template.Replace('{{WORKSPACE_ID}}', $workspaceId)
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

        $existing = Get-FabricItems -WorkspaceId $workspaceId -Type "DataPipeline" | Where-Object { $_.displayName -eq $pName } | Select-Object -First 1
        try {
            if (-not $existing) {
                $body = @{ displayName = $pName; type = "DataPipeline"; definition = $definition }
                if ($pipelinesFolderId) { $body.folderId = $pipelinesFolderId }
                Invoke-FabricApi -Method "POST" -Uri "https://api.fabric.microsoft.com/v1/workspaces/$workspaceId/items" -Body $body | Out-Null
                Write-Host "  Pipeline '$pName' -- created (in pipelines/ folder)"
            } else {
                Invoke-FabricApi -Method "POST" -Uri "https://api.fabric.microsoft.com/v1/workspaces/$workspaceId/items/$($existing.id)/updateDefinition" -Body @{ definition = $definition } | Out-Null
                Write-Host "  Pipeline '$pName' -- updated"
            }
        } catch {
            Write-Warning "  Pipeline '$pName' -- FAILED: $_"
        }
    }
} else {
    Write-Host "  No pipelines directory found, skipping."
}

# ── Summary ─────────────────────────────────────────────────────
Write-Host "`n=== Deployment Complete ===" -ForegroundColor Green
Write-Host "`nLakehouse IDs:" -ForegroundColor Cyan
Write-Host "  source:  $sourceId"
Write-Host "  landing: $landingId"
Write-Host "  bronze:  $bronzeId"
Write-Host "  silver:  $silverId"
Write-Host "  gold:    $goldId"
Write-Host "`nFolder structure:" -ForegroundColor Cyan
Write-Host "  $projectFolderName/"
Write-Host "    data/            (5 lakehouses)"
Write-Host "    experiments/     (MLflow experiments)"
Write-Host "    notebooks/"
Write-Host "      main/          (23 notebooks)"
Write-Host "      modules/       (14 modules)"
Write-Host "    pipelines/       (6 data pipelines)"
Write-Host "    semantic_models/ (created by notebook 15 at runtime)"
