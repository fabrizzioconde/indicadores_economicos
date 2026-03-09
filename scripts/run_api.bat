@echo off
REM Sobe a API FastAPI. Execute a partir da raiz do projeto (macro_insights_mvp).
cd /d "%~dp0.."
if exist .venv\Scripts\activate.bat (
  call .venv\Scripts\activate.bat
)
uvicorn api.main:app --reload --host 0.0.0.0 --port 8000
