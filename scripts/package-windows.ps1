<#
.SYNOPSIS
    Build, sign and package kessel for Windows.

.DESCRIPTION
    Produces two artifacts under target\dist:

      kessel-<version>-x86_64.exe          the app itself, signed if a
                                           certificate is configured
      kessel-<version>-x86_64.zip          the same executable, zipped
      …and a .sha256 next to each

    The bare .exe is what the in-app updater downloads and swaps in place
    (src/update.rs); the zip is the friendlier download for a browser. There is
    no installer: kessel is a single self-contained executable — assets, icons,
    migrations and config defaults are all compiled in — so "install" is
    "put it somewhere and run it".

    Signing is optional but strongly recommended: without it SmartScreen warns
    on first run. Point KESSEL_WIN_CERT at a PFX file (or set
    KESSEL_WIN_CERT_THUMBPRINT for a certificate already in the store).

.PARAMETER SkipBuild
    Package whatever is already in target\release instead of rebuilding.

.NOTES
    Environment:
      KESSEL_WIN_CERT             path to a .pfx code-signing certificate
      KESSEL_WIN_CERT_PASSWORD    its password
      KESSEL_WIN_CERT_THUMBPRINT  alternative: a cert already in the store
      KESSEL_WIN_TIMESTAMP_URL    RFC 3161 timestamp server
                                  (default: http://timestamp.digicert.com)
#>
[CmdletBinding()]
param(
    [switch]$SkipBuild
)

$ErrorActionPreference = 'Stop'
Set-Location (Join-Path $PSScriptRoot '..')

# CalVer, straight from Cargo.toml — the same source scripts/release.sh tags.
$version = (cargo metadata --no-deps --format-version 1 | ConvertFrom-Json).packages[0].version
$dist = 'target\dist'
$exeName = "kessel-$version-x86_64.exe"
$zipName = "kessel-$version-x86_64.zip"

if (-not $SkipBuild) {
    Write-Host "Building kessel $version (release)…"
    cargo build --release
    if ($LASTEXITCODE -ne 0) { throw 'cargo build failed' }
}

$built = 'target\release\kessel.exe'
if (-not (Test-Path $built)) { throw "missing $built — run without -SkipBuild" }

New-Item -ItemType Directory -Force -Path $dist | Out-Null
$exe = Join-Path $dist $exeName
Copy-Item $built $exe -Force

# ─── Sign ────────────────────────────────────────────────────────────────────
# signtool ships with the Windows SDK and isn't on PATH by default.
function Find-SignTool {
    $onPath = Get-Command signtool.exe -ErrorAction SilentlyContinue
    if ($onPath) { return $onPath.Source }
    $roots = "${env:ProgramFiles(x86)}\Windows Kits\10\bin"
    if (-not (Test-Path $roots)) { return $null }
    Get-ChildItem $roots -Recurse -Filter signtool.exe -ErrorAction SilentlyContinue |
        Where-Object { $_.FullName -match '\\x64\\' } |
        Sort-Object FullName -Descending |
        Select-Object -First 1 -ExpandProperty FullName
}

$timestamp = if ($env:KESSEL_WIN_TIMESTAMP_URL) { $env:KESSEL_WIN_TIMESTAMP_URL } else { 'http://timestamp.digicert.com' }
$signArgs = $null
if ($env:KESSEL_WIN_CERT) {
    $signArgs = @('/f', $env:KESSEL_WIN_CERT)
    if ($env:KESSEL_WIN_CERT_PASSWORD) { $signArgs += @('/p', $env:KESSEL_WIN_CERT_PASSWORD) }
} elseif ($env:KESSEL_WIN_CERT_THUMBPRINT) {
    $signArgs = @('/sha1', $env:KESSEL_WIN_CERT_THUMBPRINT)
}

if ($signArgs) {
    $signtool = Find-SignTool
    if (-not $signtool) { throw 'signtool.exe not found — install the Windows SDK signing tools' }
    Write-Host 'Signing the executable…'
    & $signtool sign /fd SHA256 /tr $timestamp /td SHA256 @signArgs $exe
    if ($LASTEXITCODE -ne 0) { throw 'signtool failed' }
    & $signtool verify /pa $exe
    if ($LASTEXITCODE -ne 0) { throw 'signature verification failed' }
} else {
    Write-Warning 'No signing certificate configured (KESSEL_WIN_CERT / KESSEL_WIN_CERT_THUMBPRINT) — SmartScreen will warn users on first run.'
}

# ─── Zip + checksums ─────────────────────────────────────────────────────────
# Zipped *after* signing so both artifacts carry the same signed bytes.
$zip = Join-Path $dist $zipName
Remove-Item $zip -ErrorAction SilentlyContinue
Compress-Archive -Path $exe -DestinationPath $zip

foreach ($artifact in @($exe, $zip)) {
    $hash = (Get-FileHash $artifact -Algorithm SHA256).Hash.ToLower()
    Set-Content -Path "$artifact.sha256" -Value $hash -NoNewline
    Write-Host "packaged: $artifact ($hash)"
}
