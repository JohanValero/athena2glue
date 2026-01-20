"""
Estado del agente LangGraph durante el flujo de migración.
"""


from typing import Dict, List, TypedDict

from src.models.table_info import TableSourceInfo

from ..models.cte_info import CTESourceInfo


class AgentState(TypedDict, total=False):
    """
    Estado del agente durante el flujo de migración.
    
    Usa TypedDict para compatibilidad con LangGraph.
    """
    sql_file_path: str
    business_name: str
    output_dir: str
    raw_sql: str
    cleaned_sql: str
    last_select: str
    new_last_select: str
    tables: List[TableSourceInfo]
    ctes: List[CTESourceInfo]
    date_replacements: Dict[str, str]


def create_initial_state(
    sql_file_path: str,
    business_name: str,
    output_dir: str = "./output"
) -> AgentState:
    """
    Crea un estado inicial del agente.
    
    Args:
        sql_file_path: Ruta al archivo SQL de entrada
        business_name: Nombre del negocio (para nomenclatura)
        output_dir: Directorio de salida
        
    Returns:
        AgentState inicial
    """
    return AgentState(
        sql_file_path=sql_file_path,
        business_name=business_name,
        output_dir=output_dir,
        raw_sql="",
        cleaned_sql="",
        last_select="",
        new_last_select="",
        tables=[],
        ctes=[],
        date_replacements={}
    )
