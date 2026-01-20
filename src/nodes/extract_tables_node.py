"""
Nodo para extraer tablas del SQL.
"""

import logging
from typing import Dict, List, Literal

import sqlglot
from sqlglot import exp

from src.models.table_info import TableSourceInfo
from ..models.agent_state import AgentState
from ..utils.logging_config import setup_logger

logger: logging.Logger = setup_logger(__name__)

TABLE_CONFIG: Dict = {
    "dwh_thr_modelo_datos.dim_tiempo": Literal["iceberg"],
    "dwh_thr_reportes.fct_saldos_semanal_detallado": Literal["iceberg"],
    "stg_cap.stg_segmentacion_saldos_trad": Literal["iceberg"],
    "stg_cap.stg_segmentacion_saldos_pib": Literal["iceberg"]
}

def extract_tables_ast(sql_text: str) -> List[TableSourceInfo]:
    """
    Extrae información de las tablas desde el texto SQL usando sqlglot.

    Args:
        sql_text (str): Texto SQL a analizar.

    Returns:
        List[TableSourceInfo]: Lista de objetos TableSourceInfo con la información de las tablas.
    """
    tables: List[TableSourceInfo] = []
    parsed: sqlglot.Expression = sqlglot.parse_one(sql_text)

    for node in parsed.find_all(exp.Table):
        table_name: str = node.name
        schema_name: str = node.db
        catalog_name: str = node.catalog

        if schema_name and table_name not in [t.table for t in tables]:
            full_name: str = f"{schema_name}.{table_name}"
            table_info: TableSourceInfo = TableSourceInfo(
                full_name,
                "glue_catalog" if full_name in TABLE_CONFIG else "spark_catalog",
                catalog_name,
                schema_name,
                table_name,
                TABLE_CONFIG.get(full_name, Literal["unknown"]),
                f"tbl_{table_name} = self._get_table('{table_name}')",
                f"tlb_{table_name}"
            )
            tables.append(table_info)

    return tables


def extract_tables_node(state: AgentState) -> AgentState:
    """
    Extrae tablas fuente del SQL.
    
    Args:
        state: Estado actual del agente
        
    Returns:
        Estado actualizado con source_tables
    """
    cleaned_sql: str = state.get("cleaned_sql") # pyright: ignore[reportAssignmentType]
    tables: List[TableSourceInfo] = extract_tables_ast(cleaned_sql)

    logger.info("Tablas detectadas: %s", len(tables))
    for t in tables:
        logger.info("Tabla detectada: %s", t.full_name)

    state["tables"] = tables
    return state
