@echo off
REM Executa o ETL completo para atualizar os dados do dashboard.
REM Use este script no Agendador de Tarefas do Windows para atualização diária.
REM A pasta "scripts" fica dentro do projeto; sobe um nível para a raiz.

set SCRIPT_DIR=%~dp0
set PROJECT_ROOT=%SCRIPT_DIR%..
cd /d "%PROJECT_ROOT%"

if not exist "logs" mkdir logs
echo [%date% %time%] Iniciando ETL diario. >> logs\etl_daily.log

call .venv\Scripts\activate.bat
python run_etl.py --mode full >> logs\etl_daily.log 2>&1
set EXIT_CODE=%ERRORLEVEL%

echo [%date% %time%] ETL finalizado com codigo %EXIT_CODE%. >> logs\etl_daily.log
exit /b %EXIT_CODE%
