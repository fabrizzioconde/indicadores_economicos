#!/usr/bin/env bash
# Executa o ETL completo para atualizar os dados do dashboard.
# Use este script no cron (Linux/macOS) para atualização diária.
# A pasta "scripts" fica dentro do projeto; sobe um nível para a raiz.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_ROOT"

mkdir -p logs
echo "[$(date '+%Y-%m-%d %H:%M:%S')] Iniciando ETL diário." >> logs/etl_daily.log

source .venv/bin/activate
python run_etl.py --mode full >> logs/etl_daily.log 2>&1
EXIT_CODE=$?

echo "[$(date '+%Y-%m-%d %H:%M:%S')] ETL finalizado com código $EXIT_CODE." >> logs/etl_daily.log
exit $EXIT_CODE
