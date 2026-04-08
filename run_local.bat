@echo off
echo ============================================
echo   SKU Scanner — Local Run
echo ============================================
echo.

cd /d "%~dp0"

:: Pull latest checkpoint from GitHub
echo Pulling latest checkpoint from GitHub...
git pull --quiet
if errorlevel 1 (
    echo WARNING: git pull failed. Continuing with local checkpoint...
)
echo.

:: Create lock file to prevent GitHub Actions from running
echo Creating lock file...
echo Running locally since %date% %time% > running.lock
git add running.lock
git commit -m "Lock: local run starting" --quiet 2>nul
git push --quiet 2>nul
echo Lock file pushed to GitHub.
echo.

:: Run the scanner
echo Starting SKU Scanner...
echo Press Ctrl+C to stop safely.
echo.
python sku_scanner.py %*

:: Remove lock and push checkpoint
echo.
echo Cleaning up...
del running.lock 2>nul
git add checkpoint.json
git rm --cached running.lock --quiet 2>nul
git commit -m "Unlock: local run complete, checkpoint updated" --quiet 2>nul
git push --quiet 2>nul
echo Checkpoint pushed to GitHub. Lock removed.
echo.
echo Done!
pause
