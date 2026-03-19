<#
.SYNOPSIS
    Idempotent deployment of IBP Forecast lakehouses, folders, and notebooks
    to a Microsoft Fabric workspace.
.DESCRIPTION
    Reads deploy.config.toml, creates a top-level project folder, nests
    lakehouses and notebook folders inside it, then deploys all notebooks.
    Existing items are reused or updated -- safe to re-run.
#>
param(
    [string]$ConfigPath = "$PSScriptRoot/deploy.config.toml"
)
Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

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

# ── Fabric API helpers ──────────────────────────────────────────
function Get-FabricToken {
    return az account get-access-token --resource "https://api.fabric.microsoft.com" --query accessToken -o tsv
}

function Invoke-FabricApi {
    param([string]$Method, [string]$Uri, [object]$Body = $null)
    $token = Get-FabricToken
    $headers = @{ Authorization = "Bearer $token" }
    if ($Body) {
        $jsonBody = $Body | ConvertTo-Json -Depth 20 -Compress
        return Invoke-RestMethod -Uri $Uri -Method $Method -Headers $headers -Body $jsonBody -ContentType "application/json"
    } else {
        return Invoke-RestMethod -Uri $Uri -Method $Method -Headers $headers
    }
}

function Get-FabricItems {
    param([string]$WorkspaceId, [string]$Type)
    $items = @()
    $uri = "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/items?type=$Type"
    while ($uri) {
        $resp = Invoke-FabricApi -Method "GET" -Uri $uri
        $items += $resp.value
        $uri = $resp.continuationUri
    }
    return $items
}

# ── Resolve workspace ID (by name if no ID provided) ───────────
$workspaceId = $config.fabric.workspace_id
if ([string]::IsNullOrWhiteSpace($workspaceId)) {
    $wsName = $config.fabric.workspace_name
    if ([string]::IsNullOrWhiteSpace($wsName)) {
        throw "Either fabric.workspace_id or fabric.workspace_name must be set in config."
    }
    Write-Host "Looking up workspace '$wsName'..."
    $allWs = Invoke-FabricApi -Method "GET" -Uri "https://api.fabric.microsoft.com/v1/workspaces"
    $match = $allWs.value | Where-Object { $_.displayName -eq $wsName } | Select-Object -First 1
    if (-not $match) { throw "Workspace '$wsName' not found." }
    $workspaceId = $match.id
    Write-Host "  Resolved: $workspaceId"
}
Write-Host "Workspace: $workspaceId"

# ── Folder helpers ──────────────────────────────────────────────
function Ensure-FabricFolder {
    param([string]$WorkspaceId, [string]$FolderName, [string]$ParentFolderId)
    $folders = Invoke-FabricApi -Method "GET" -Uri "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/folders"
    $match = $folders.value | Where-Object {
        $_.displayName -eq $FolderName -and
        ((-not $ParentFolderId -and -not $_.parentFolderId) -or ($_.parentFolderId -eq $ParentFolderId))
    } | Select-Object -First 1

    if ($match) {
        Write-Host "  Folder '$FolderName' -- exists: $($match.id)"
        return $match.id
    }

    $body = @{ displayName = $FolderName }
    if ($ParentFolderId) { $body.parentFolderId = $ParentFolderId }
    $created = Invoke-FabricApi -Method "POST" -Uri "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/folders" -Body $body
    Write-Host "  Folder '$FolderName' -- created: $($created.id)"
    return $created.id
}

# ── Lakehouse helpers ───────────────────────────────────────────
function Ensure-FabricLakehouse {
    param([string]$WorkspaceId, [string]$LakehouseId, [string]$LakehouseName, [string]$FolderId)
    if (-not [string]::IsNullOrWhiteSpace($LakehouseId)) {
        Write-Host "  Lakehouse '$LakehouseName' -- using provided ID: $LakehouseId"
        return $LakehouseId
    }
    $existing = Get-FabricItems -WorkspaceId $WorkspaceId -Type "Lakehouse" | Where-Object { $_.displayName -eq $LakehouseName } | Select-Object -First 1
    if ($existing) {
        Write-Host "  Lakehouse '$LakehouseName' -- exists: $($existing.id)"
        return $existing.id
    }
    $body = @{ displayName = $LakehouseName; type = "Lakehouse" }
    if (-not [string]::IsNullOrWhiteSpace($FolderId)) { $body.folderId = $FolderId }
    $created = Invoke-FabricApi -Method "POST" -Uri "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/items" -Body $body
    Write-Host "  Lakehouse '$LakehouseName' -- created: $($created.id)"
    return $created.id
}

# ── Notebook helpers ────────────────────────────────────────────
function Ensure-FabricNotebook {
    param([string]$WorkspaceId, [string]$DisplayName, [string]$FolderId, [string]$SourceFilePath)
    $source = Get-Content -Path $SourceFilePath -Raw -Encoding UTF8
    $payloadBase64 = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($source))
    $definition = @{
        format = "fabricGitSource"
        parts = @(@{ path = "notebook-content.py"; payload = $payloadBase64; payloadType = "InlineBase64" })
    }

    $existing = Get-FabricItems -WorkspaceId $WorkspaceId -Type "Notebook" | Where-Object { $_.displayName -eq $DisplayName } | Select-Object -First 1
    if (-not $existing) {
        $body = @{ displayName = $DisplayName; type = "Notebook"; definition = $definition }
        if ($FolderId) { $body.folderId = $FolderId }
        $created = Invoke-FabricApi -Method "POST" -Uri "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/items" -Body $body
        Write-Host "  Notebook '$DisplayName' -- created"
        return $created.id
    }
    Invoke-FabricApi -Method "POST" -Uri "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/items/$($existing.id)/updateDefinition?updateMetadata=true" -Body @{ definition = $definition }
    Write-Host "  Notebook '$DisplayName' -- updated"
    return $existing.id
}

# ─────────────────────────────────────────────────────────────────
# MAIN FLOW
# ─────────────────────────────────────────────────────────────────

$projectFolderName = $config.naming.project_folder
if ([string]::IsNullOrWhiteSpace($projectFolderName)) { $projectFolderName = "IBP Forecast" }

# 1. Top-level project folder (everything nests under this)
Write-Host "`n[1/5] Creating project folder '$projectFolderName'..." -ForegroundColor Yellow
$projectFolderId = Ensure-FabricFolder -WorkspaceId $workspaceId -FolderName $projectFolderName

# 2. Sub-folders under the project folder
Write-Host "`n[2/5] Creating sub-folders..." -ForegroundColor Yellow
$dataFolderId    = Ensure-FabricFolder -WorkspaceId $workspaceId -FolderName "data"      -ParentFolderId $projectFolderId
$notebooksFolderId = Ensure-FabricFolder -WorkspaceId $workspaceId -FolderName "notebooks" -ParentFolderId $projectFolderId
$mainFolderId    = Ensure-FabricFolder -WorkspaceId $workspaceId -FolderName "main"      -ParentFolderId $notebooksFolderId
$modulesFolderId = Ensure-FabricFolder -WorkspaceId $workspaceId -FolderName "modules"   -ParentFolderId $notebooksFolderId

# 3. Lakehouses (under data/ folder)
Write-Host "`n[3/5] Creating lakehouses under '$projectFolderName/data/'..." -ForegroundColor Yellow
$sourceId  = Ensure-FabricLakehouse -WorkspaceId $workspaceId -LakehouseId $config.source.source_lakehouse_id  -LakehouseName $config.source.source_lakehouse_name -FolderId $dataFolderId
$landingId = Ensure-FabricLakehouse -WorkspaceId $workspaceId -LakehouseId $config.lakehouses.landing_id -LakehouseName $config.lakehouses.landing_name -FolderId $dataFolderId
$bronzeId  = Ensure-FabricLakehouse -WorkspaceId $workspaceId -LakehouseId $config.lakehouses.bronze_id  -LakehouseName $config.lakehouses.bronze_name  -FolderId $dataFolderId
$silverId  = Ensure-FabricLakehouse -WorkspaceId $workspaceId -LakehouseId $config.lakehouses.silver_id  -LakehouseName $config.lakehouses.silver_name  -FolderId $dataFolderId
$goldId    = Ensure-FabricLakehouse -WorkspaceId $workspaceId -LakehouseId $config.lakehouses.gold_id    -LakehouseName $config.lakehouses.gold_name    -FolderId $dataFolderId

# 4. Module notebooks (under notebooks/modules/)
Write-Host "`n[4/5] Deploying module notebooks..." -ForegroundColor Yellow
$modulesDir = Join-Path $PSScriptRoot "assets" "notebooks" "modules"
foreach ($file in (Get-ChildItem $modulesDir -Filter "*.py" | Sort-Object Name)) {
    $name = [System.IO.Path]::GetFileNameWithoutExtension($file.Name)
    Ensure-FabricNotebook -WorkspaceId $workspaceId -DisplayName $name -FolderId $modulesFolderId -SourceFilePath $file.FullName | Out-Null
}

# 5. Main notebooks (under notebooks/main/)
Write-Host "`n[5/5] Deploying main notebooks..." -ForegroundColor Yellow
$mainDir = Join-Path $PSScriptRoot "assets" "notebooks" "main"
foreach ($file in (Get-ChildItem $mainDir -Filter "*.py" | Sort-Object Name)) {
    $name = [System.IO.Path]::GetFileNameWithoutExtension($file.Name)
    Ensure-FabricNotebook -WorkspaceId $workspaceId -DisplayName $name -FolderId $mainFolderId -SourceFilePath $file.FullName | Out-Null
}

# ── Output summary ──────────────────────────────────────────────
Write-Host "`n=== Deployment Complete ===" -ForegroundColor Green
Write-Host "Everything deployed under: $projectFolderName/" -ForegroundColor Green

$output = @{
    workspace_id     = $workspaceId
    project_folder   = $projectFolderName
    source_id        = $sourceId
    landing_id       = $landingId
    bronze_id        = $bronzeId
    silver_id        = $silverId
    gold_id          = $goldId
}
$output | ConvertTo-Json | Write-Host

Write-Host "`nFolder structure in Fabric:" -ForegroundColor Cyan
Write-Host "  $projectFolderName/"
Write-Host "    data/"
Write-Host "      lh_ibp_source     ($sourceId)"
Write-Host "      lh_ibp_landing    ($landingId)"
Write-Host "      lh_ibp_bronze     ($bronzeId)"
Write-Host "      lh_ibp_silver     ($silverId)"
Write-Host "      lh_ibp_gold       ($goldId)"
Write-Host "    notebooks/"
Write-Host "      main/             (17 notebooks)"
Write-Host "      modules/          (12 notebooks)"
