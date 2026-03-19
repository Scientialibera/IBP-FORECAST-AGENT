<#
.SYNOPSIS
    Idempotent deployment of IBP Forecast lakehouses, folders, and notebooks
    to a Microsoft Fabric workspace.
.DESCRIPTION
    Reads deploy.config.toml, creates lakehouses if not present, then deploys
    all module and main notebooks. Matches the FABRIC-ANALYTICS pattern.
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
$workspaceId = $config.fabric.workspace_id
if ([string]::IsNullOrWhiteSpace($workspaceId)) { throw "fabric.workspace_id is required in config." }

Write-Host "`n=== IBP Forecast -- Fabric Deployment ===" -ForegroundColor Cyan
Write-Host "Workspace: $workspaceId"

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

# ── Folder helpers ──────────────────────────────────────────────
function Ensure-FabricFolder {
    param([string]$WorkspaceId, [string]$FolderName, [string]$ParentFolderId)
    $folders = Invoke-FabricApi -Method "GET" -Uri "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/folders"
    $match = $folders.value | Where-Object {
        $_.displayName -eq $FolderName -and
        ((-not $ParentFolderId -and -not $_.parentFolderId) -or ($_.parentFolderId -eq $ParentFolderId))
    } | Select-Object -First 1

    if ($match) { return $match.id }

    $body = @{ displayName = $FolderName }
    if ($ParentFolderId) { $body.parentFolderId = $ParentFolderId }
    $created = Invoke-FabricApi -Method "POST" -Uri "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/folders" -Body $body
    Write-Host "  Folder '$FolderName' -- created"
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

# 1. Folders
Write-Host "`n[1/4] Creating folders..." -ForegroundColor Yellow
$rootFolderId    = Ensure-FabricFolder -WorkspaceId $workspaceId -FolderName "ibp_forecast"
$mainFolderId    = Ensure-FabricFolder -WorkspaceId $workspaceId -FolderName "main"    -ParentFolderId $rootFolderId
$modulesFolderId = Ensure-FabricFolder -WorkspaceId $workspaceId -FolderName "modules" -ParentFolderId $rootFolderId

# 2. Lakehouses
Write-Host "`n[2/4] Creating lakehouses..." -ForegroundColor Yellow
$landingId = Ensure-FabricLakehouse -WorkspaceId $workspaceId -LakehouseId $config.lakehouses.landing_id -LakehouseName $config.lakehouses.landing_name
$bronzeId  = Ensure-FabricLakehouse -WorkspaceId $workspaceId -LakehouseId $config.lakehouses.bronze_id  -LakehouseName $config.lakehouses.bronze_name
$silverId  = Ensure-FabricLakehouse -WorkspaceId $workspaceId -LakehouseId $config.lakehouses.silver_id  -LakehouseName $config.lakehouses.silver_name
$goldId    = Ensure-FabricLakehouse -WorkspaceId $workspaceId -LakehouseId $config.lakehouses.gold_id    -LakehouseName $config.lakehouses.gold_name

# 3. Module notebooks
Write-Host "`n[3/4] Deploying module notebooks..." -ForegroundColor Yellow
$modulesDir = Join-Path $PSScriptRoot "assets" "notebooks" "modules"
foreach ($file in (Get-ChildItem $modulesDir -Filter "*.py" | Sort-Object Name)) {
    $name = [System.IO.Path]::GetFileNameWithoutExtension($file.Name)
    Ensure-FabricNotebook -WorkspaceId $workspaceId -DisplayName $name -FolderId $modulesFolderId -SourceFilePath $file.FullName | Out-Null
}

# 4. Main notebooks
Write-Host "`n[4/4] Deploying main notebooks..." -ForegroundColor Yellow
$mainDir = Join-Path $PSScriptRoot "assets" "notebooks" "main"
foreach ($file in (Get-ChildItem $mainDir -Filter "*.py" | Sort-Object Name)) {
    $name = [System.IO.Path]::GetFileNameWithoutExtension($file.Name)
    Ensure-FabricNotebook -WorkspaceId $workspaceId -DisplayName $name -FolderId $mainFolderId -SourceFilePath $file.FullName | Out-Null
}

# ── Output summary ──────────────────────────────────────────────
Write-Host "`n=== Deployment Complete ===" -ForegroundColor Green
$output = @{
    workspace_id = $workspaceId
    landing_id   = $landingId
    bronze_id    = $bronzeId
    silver_id    = $silverId
    gold_id      = $goldId
}
$output | ConvertTo-Json | Write-Host
