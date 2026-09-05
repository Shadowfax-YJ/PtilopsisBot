$ErrorActionPreference = 'Stop'
Set-Location -LiteralPath $PSScriptRoot
& "$PSScriptRoot\.venv\Scripts\python.exe" -X utf8 -m databot --config "$PSScriptRoot\config.toml" run
exit $LASTEXITCODE
