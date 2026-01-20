"""
Nodo para extraer CTEs (Common Table Expressions) del SQL.
"""

import logging
from typing import List

import sqlglot
from sqlglot import exp

from ..models.cte_info import CTESourceInfo
from ..models.agent_state import AgentState
from ..nodes.extract_tables_node import extract_tables_ast
from ..utils.logging_config import setup_logger

logger: logging.Logger = setup_logger(__name__)


def extract_ctes_ast(sql_text: str) -> List[CTESourceInfo]:
    """
    Parsea el SQL y extrae las definiciones de CTEs, su contenido y orden.

    Args:
        sql_text (str): Texto SQL limpio.
        tables (List[TableSourceInfo]): Lista de tablas conocidas.

    Returns:
        List[CTESourceInfo]: Lista de objetos con información de las CTEs.
    """
    ctes: List[CTESourceInfo] = []
    parsed: sqlglot.Expression = sqlglot.parse_one(sql_text)

    for index, node in enumerate(parsed.find_all(exp.CTE)):
        cte_name = node.alias
        cte_sql: str = node.this.sql()
        cte_info: CTESourceInfo = CTESourceInfo(
            name=cte_name,
            inner_sql=cte_sql,
            new_sql="",
            position=index,
            python_method="",
            python_create="",
            tables=extract_tables_ast(cte_sql)
        )
        ctes.append(cte_info)

    return ctes


def extract_ctes_node(state: AgentState) -> AgentState:
    """
    Extrae las CTEs del SQL para análisis intermedio.

    Args:
        state: Estado actual del agente

    Returns:
        Estado actualizado con ctes_extracted
    """
    cleaned_sql: str = state.get("cleaned_sql") # pyright: ignore[reportAssignmentType]
    logger.info("Iniciando extracción de CTEs...")

    ctes_found: List[CTESourceInfo] = extract_ctes_ast(cleaned_sql)
    logger.info("CTEs detectadas: %s", len(ctes_found))

    for cte in ctes_found:
        logger.info("CTE: %s (Posición: %s) -> SQL: %s", cte.name, cte.position, cte.inner_sql)

    state["ctes"] = ctes_found
    return state
