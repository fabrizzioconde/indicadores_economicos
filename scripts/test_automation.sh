#!/usr/bin/env bash
# Testa a automatização do ETL: executa run_etl_daily.sh e exibe OK ou Falhou.
# Execute na raiz do projeto:  ./scripts/test_automation.sh

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_ROOT"

"$SCRIPT_DIR/run_etl_daily.sh"
EXIT_CODE=$?

if [ $EXIT_CODE -eq 0 ]; then
    echo "OK"
else
    echo "Falhou"
fi
exit $EXIT_CODE
