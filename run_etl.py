"""
Ponto de entrada para atualizar os dados do dashboard.

Este script é o principal para rodar o ETL a partir da linha de comando.
Ele carrega as séries (BACEN e IBGE), persiste em data/gold e deixa
os arquivos prontos para o dashboard. Use --mode full para todas as
séries ou --mode minimal para um subconjunto (testes rápidos).
Com --incremental, baixa apenas os dados que faltam (a partir do último
parquet), evitando retrabalho e reduzindo consumo da API.

Exemplos:
    python run_etl.py --mode full
    python run_etl.py --mode full --incremental
    python run_etl.py --mode minimal
    python run_etl.py --incremental
"""
import argparse
import sys

from etl.pipeline import run_full_etl, run_minimal_etl


def main() -> int:
    """
    Interpreta argumentos, executa o ETL no modo escolhido e exibe o resultado.

    Returns:
        0 em sucesso, 1 se ocorrer erro não tratado.
    """
    parser = argparse.ArgumentParser(
        description="Atualiza os dados do dashboard (ETL BACEN e IBGE).",
    )
    parser.add_argument(
        "--mode",
        choices=["full", "minimal"],
        default="full",
        help="full = todas as séries; minimal = SELIC, câmbio e IPCA (padrão: full)",
    )
    parser.add_argument(
        "--incremental",
        action="store_true",
        help="busca apenas dados novos a partir do último parquet em data/gold (menos API)",
    )
    args = parser.parse_args()

    mode = args.mode
    incremental = args.incremental

    if mode == "full":
        print(
            "Modo escolhido: full (SELIC, câmbio USD/BRL, IBC-Br, IPCA, IPCA-15)"
            + (" [incremental]" if incremental else "") + "."
        )
        run_fn = lambda: run_full_etl(incremental=incremental)
    else:
        print(
            "Modo escolhido: minimal (SELIC, câmbio USD/BRL, IPCA)"
            + (" [incremental]" if incremental else "") + "."
        )
        run_fn = lambda: run_minimal_etl(incremental=incremental)

    print("Iniciando execução do ETL…")
    try:
        run_fn()
        print("Execução do ETL concluída.")
        return 0
    except Exception as e:
        print(f"Erro durante a execução do ETL: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
