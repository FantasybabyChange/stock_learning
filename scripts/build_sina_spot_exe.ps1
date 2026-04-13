# 在项目根目录执行: .\scripts\build_sina_spot_exe.ps1
$ErrorActionPreference = "Stop"
Set-Location (Resolve-Path (Join-Path $PSScriptRoot ".."))

Write-Host "Installing extras (exe) and building sina_batch_spot.exe ..." -ForegroundColor Cyan
uv sync --extra exe
uv run pyinstaller `
    --noconfirm `
    --clean `
    --onefile `
    --console `
    --name sina_batch_spot `
    --collect-all requests `
    --collect-all certifi `
    --collect-all urllib3 `
    run_sina_spot_exe.py

Write-Host "Done: dist\sina_batch_spot.exe" -ForegroundColor Green
Write-Host "Optional: copy watchlist.local next to the exe (comma-separated symbols)." -ForegroundColor Gray
