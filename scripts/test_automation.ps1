# Testa a automatizacao do ETL: executa run_etl_daily.bat e exibe OK (verde) ou Falhou (vermelho).
# Execute na raiz do projeto:  .\scripts\test_automation.ps1

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$ProjectRoot = Split-Path -Parent $ScriptDir
Set-Location $ProjectRoot

& "$ScriptDir\run_etl_daily.bat"
$exitCode = $LASTEXITCODE

if ($exitCode -eq 0) {
    Write-Host "OK" -ForegroundColor Green
} else {
    Write-Host "Falhou" -ForegroundColor Red
}
exit $exitCode
