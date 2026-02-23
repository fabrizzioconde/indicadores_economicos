"""
Pacote ETL (Extract, Transform, Load) do macro_insights_mvp.

Contém módulos para extrair séries do BACEN e IBGE,
transformar e carregar em data/processed e data/gold.
"""

from etl.bacen import run_bacen_etl
from etl.ibge_ipca import run_ipca_etl
from etl.pipeline import run_full_etl, run_full_pipeline, run_minimal_etl

__all__ = [
    "run_bacen_etl",
    "run_ipca_etl",
    "run_full_etl",
    "run_minimal_etl",
    "run_full_pipeline",
]
