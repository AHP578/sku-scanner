Write-Host "============================================"
Write-Host "  SKU Scanner - Local Run"
Write-Host "============================================"
Write-Host ""

Set-Location $PSScriptRoot

# Pull latest checkpoint from GitHub
Write-Host "Pulling latest checkpoint from GitHub..."
git pull --quiet
Write-Host ""

# Create lock file to prevent GitHub Actions from running
Write-Host "Creating lock file..."
"Running locally since $(Get-Date)" | Out-File -FilePath running.lock
git add running.lock
git commit -m "Lock: local run starting" --quiet 2>$null
git push --quiet 2>$null
Write-Host "Lock file pushed to GitHub."
Write-Host ""

# Run the scanner
Write-Host "Starting SKU Scanner..."
Write-Host "Press Ctrl+C to stop safely."
Write-Host ""
python sku_scanner.py @args

# Remove lock and push checkpoint
Write-Host ""
Write-Host "Cleaning up..."
Remove-Item running.lock -ErrorAction SilentlyContinue
git add checkpoint.json
git rm --cached running.lock --quiet 2>$null
git commit -m "Unlock: local run complete, checkpoint updated" --quiet 2>$null
git push --quiet 2>$null
Write-Host "Checkpoint pushed to GitHub. Lock removed."
Write-Host ""
Write-Host "Done!"
