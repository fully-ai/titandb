$ErrorActionPreference = "Stop"
Set-Location (Join-Path $PSScriptRoot "..")
if (Test-Path "target") { Remove-Item -Recurse -Force "target" }
Write-Host "Removed target build outputs."
