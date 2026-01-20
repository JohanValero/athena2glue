"""
Nodo para extraer el Select final (Main Query) excluyendo las CTEs.
"""

import logging

import sqlglot

from ..models.agent_state import AgentState

logger: logging.Logger = logging.getLogger(__name__)


def extract_final_query_ast(sql_text: str) -> str:
    """
    Parsea el SQL y elimina la cláusula WITH (CTEs) para devolver
    únicamente la consulta principal (Last Select).

    :param sql_text: Texto SQL completo (con CTEs)
    :type sql_text: str
    :return: SQL de la consulta principal sin CTEs
    :rtype: str
    """
    parsed: sqlglot.Expression = sqlglot.parse_one(sql_text)
    if "with_" in parsed.args:
        del parsed.args["with_"]
    return parsed.sql()


def extract_last_select_node(state: AgentState) -> AgentState:
    """
    Extrae la consulta principal (Last Select) del SQL limpio, ignorando las CTEs.

    Args:
        state: Estado actual del agente

    Returns:
        Estado actualizado con 'last_select'
    """
    cleaned_sql: str = state.get("cleaned_sql")  # pyright: ignore[reportAssignmentType]
    logger.info("Iniciando extracción del Last Select (Main Query)...")

    final_query: str = extract_final_query_ast(cleaned_sql)

    preview: str = (final_query[:100] + '...') if len(final_query) > 100 else final_query
    logger.info("Last Select extraído: %s", preview)

    state["last_select"] = final_query
    return state
