@echo off
REM Testa a automatizacao do ETL: executa run_etl_daily.bat e exibe OK ou Falhou.
REM Execute na raiz do projeto:  scripts\test_automation.bat

set SCRIPT_DIR=%~dp0
set PROJECT_ROOT=%SCRIPT_DIR%..
cd /d "%PROJECT_ROOT%"

call scripts\run_etl_daily.bat
if %ERRORLEVEL% equ 0 (
    echo OK
) else (
    echo Falhou
)
exit /b %ERRORLEVEL%
