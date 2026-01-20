"""
Definición del grafo LangGraph para el agente.
"""

import logging

from langgraph.graph import StateGraph, END
from langgraph.graph.state import CompiledStateGraph

from .nodes import extract_tables_node, parse_sql_node, extract_ctes_node, extract_dates_node
from .nodes import extract_last_select_node, convert_syntax_node, generate_code_node
from .models.agent_state import AgentState


logger: logging.Logger = logging.getLogger(__name__)


def create_agent_graph() -> CompiledStateGraph:
    """
    Crea el grafo de LangGraph para el agente de migración.

    Flujo:
    1. parse_sql: Lee y limpia el SQL.
    2. extract_tables_node: Identifica tablas origen.
    3. extract_ctes_node: Identifica CTEs.
    4. extract_last_select_node: Identifica query principal.
    5. extract_dates_node: Identifica fechas hardcodeadas.
    6. convert_syntax_node: Transpila a Spark.
    7. generate_code_node: Genera script final.

    Returns:
        CompiledStateGraph: El grafo compilado listo para ejecutar.
    """
    logger.info("Creando grafo del agente...")
    graph = StateGraph(AgentState)

    graph.add_node("parse_sql", parse_sql_node)
    graph.add_node("extract_tables_node", extract_tables_node)
    graph.add_node("extract_ctes_node", extract_ctes_node)
    graph.add_node("extract_last_select_node", extract_last_select_node)
    graph.add_node("extract_dates_node", extract_dates_node)
    graph.add_node("convert_syntax_node", convert_syntax_node)
    graph.add_node("generate_code_node", generate_code_node)

    graph.set_entry_point("parse_sql")
    graph.add_edge("parse_sql", "extract_tables_node")
    graph.add_edge("extract_tables_node", "extract_ctes_node")
    graph.add_edge("extract_ctes_node", "extract_last_select_node")
    graph.add_edge("extract_last_select_node", "extract_dates_node")
    graph.add_edge("extract_dates_node", "convert_syntax_node")
    graph.add_edge("convert_syntax_node", "generate_code_node")
    graph.add_edge("generate_code_node", END)

    return graph.compile()
