[CmdletBinding()]
param(
    [string]$InstallRoot = (Join-Path $env:LOCALAPPDATA 'MoonlightWeb\v2.10.0-low-latency'),
    [string]$RepoRoot,
    [switch]$PreflightOnly
)

$ErrorActionPreference = 'Stop'

# `$PSScriptRoot` is not reliably populated while default parameter values are
# bound by a nested Windows PowerShell process. Resolve it only after `param`
# has completed so launching this script from any working directory is safe.
if ([string]::IsNullOrWhiteSpace($RepoRoot)) {
    $RepoRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
}

$package = Join-Path $InstallRoot 'package'
$release = Join-Path $RepoRoot 'target\release'
$dist = Join-Path $RepoRoot 'dist'
$startScript = Join-Path $InstallRoot 'start-moonlight-web.ps1'
$serverPath = Join-Path $package 'web-server.exe'
$streamerPath = Join-Path $package 'streamer.exe'
$newServerPath = Join-Path $package '.moonlight-web-server.new'
$newStreamerPath = Join-Path $package '.moonlight-streamer.new'

foreach ($required in @(
    $serverPath,
    $streamerPath,
    (Join-Path $release 'web-server.exe'),
    (Join-Path $release 'streamer.exe'),
    $dist,
    $startScript
)) {
    if (-not (Test-Path -LiteralPath $required)) {
        throw "Required deployment file is missing: $required"
    }
}

if ($PreflightOnly) {
    Write-Host "Deployment preflight passed. Repo: $RepoRoot  Install: $InstallRoot"
    return
}

$backup = Join-Path $package ('backups\pre-user-settings-' + (Get-Date -Format 'yyyyMMdd-HHmmss'))
New-Item -ItemType Directory -Path $backup -Force | Out-Null
Copy-Item -LiteralPath $serverPath -Destination $backup -Force
Copy-Item -LiteralPath $streamerPath -Destination $backup -Force
Copy-Item -LiteralPath (Join-Path $package 'static') -Destination $backup -Recurse -Force
Copy-Item -LiteralPath (Join-Path $package 'server') -Destination $backup -Recurse -Force

# Stage both executables before stopping the working server. This makes a
# permissions or disk-space failure harmless to the currently running install.
Copy-Item -LiteralPath (Join-Path $release 'web-server.exe') -Destination $newServerPath -Force
Copy-Item -LiteralPath (Join-Path $release 'streamer.exe') -Destination $newStreamerPath -Force

$server = Get-Process -Name web-server -ErrorAction SilentlyContinue |
    Where-Object Path -EQ $serverPath
$streamer = Get-Process -Name streamer -ErrorAction SilentlyContinue |
    Where-Object Path -EQ $streamerPath

try {
    $streamer | Stop-Process -ErrorAction Stop
    $server | Stop-Process -ErrorAction Stop
    $server | Wait-Process -Timeout 15 -ErrorAction SilentlyContinue

    Copy-Item -LiteralPath $newServerPath -Destination $serverPath -Force
    Copy-Item -LiteralPath $newStreamerPath -Destination $streamerPath -Force

    # Overlay the browser build so the installation's custom tv-check.html is
    # retained.
    Copy-Item -Path (Join-Path $dist '*') -Destination (Join-Path $package 'static') -Recurse -Force

    & $startScript
    Start-Sleep -Seconds 2
    $status = (Invoke-WebRequest -UseBasicParsing 'http://127.0.0.1:8080/' -TimeoutSec 5).StatusCode
    if ($status -ne 200) {
        throw "Moonlight Web health check returned HTTP $status"
    }

    Write-Host "Moonlight Web updated successfully. Backup: $backup"
} catch {
    Get-Process -Name web-server -ErrorAction SilentlyContinue |
        Where-Object Path -EQ $serverPath |
        Stop-Process -ErrorAction SilentlyContinue

    Copy-Item -LiteralPath (Join-Path $backup 'web-server.exe') -Destination $serverPath -Force
    Copy-Item -LiteralPath (Join-Path $backup 'streamer.exe') -Destination $streamerPath -Force
    Remove-Item -LiteralPath (Join-Path $package 'static') -Recurse -Force
    Copy-Item -LiteralPath (Join-Path $backup 'static') -Destination $package -Recurse -Force
    # The new server may have migrated the persistent JSON schema before a
    # later health-check failure. Restore that data with the matching binaries.
    Remove-Item -LiteralPath (Join-Path $package 'server') -Recurse -Force
    Copy-Item -LiteralPath (Join-Path $backup 'server') -Destination $package -Recurse -Force
    & $startScript
    throw
} finally {
    Remove-Item -LiteralPath $newServerPath -Force -ErrorAction SilentlyContinue
    Remove-Item -LiteralPath $newStreamerPath -Force -ErrorAction SilentlyContinue
}
