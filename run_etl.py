"""
Ponto de entrada para atualizar os dados do dashboard.

Este script é o principal para rodar o ETL a partir da linha de comando.
Ele carrega as séries (BACEN e IBGE), persiste em data/gold e deixa
os arquivos prontos para o dashboard. Use --mode full para todas as
séries ou --mode minimal para um subconjunto (testes rápidos).

Exemplos:
    python run_etl.py --mode full
    python run_etl.py --mode minimal
    python run_etl.py
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
    args = parser.parse_args()

    mode = args.mode
    if mode == "full":
        print("Modo escolhido: full (SELIC, câmbio USD/BRL, IBC-Br, IPCA, IPCA-15).")
        run_fn = run_full_etl
    else:
        print("Modo escolhido: minimal (SELIC, câmbio USD/BRL, IPCA).")
        run_fn = run_minimal_etl

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
