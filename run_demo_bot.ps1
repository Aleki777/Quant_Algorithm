# run_demo_bot.ps1
# Usage: Right-click -> Run with PowerShell OR from terminal: .\run_demo_bot.ps1

$envFile = ".\.env"
if (Test-Path $envFile) {
    Get-Content $envFile | ForEach-Object {
        $line = $_.Trim()
        if ($line -eq "" -or $line.StartsWith("#")) { return }
        $parts = $line -split "=", 2
        if ($parts.Length -eq 2) {
            $name = $parts[0].Trim()
            $val = $parts[1].Trim()
            # remove surrounding quotes if present
            if ($val.StartsWith('"') -and $val.EndsWith('"')) { $val = $val.Trim('"') }
            if ($val.StartsWith("'") -and $val.EndsWith("'")) { $val = $val.Trim("'") }
            Write-Host "Setting env: $name"
            Set-Item -Path Env:$name -Value $val
        }
    }
} else {
    Write-Host ".env file not found. Make sure credentials are set in env or .env"
}

# Activate virtualenv if exists
$venvActivate = Join-Path -Path (Get-Location) -ChildPath "venv\Scripts\Activate.ps1"
if (Test-Path $venvActivate) {
    Write-Host "Activating venv..."
    & $venvActivate
} else {
    Write-Host "No venv Activate script found at $venvActivate. Make sure python and dependencies are installed."
}

# Run demobot
python demo_bot.py  # adjust if file named differently
