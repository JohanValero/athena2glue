"""
Nodo para parsear el archivo SQL de entrada.
"""

import logging
from pathlib import Path
from typing import Optional

from ..models.agent_state import AgentState
from ..utils.sql_parser import SQLParser
from ..utils.logging_config import setup_logger

logger: logging.Logger = setup_logger(__name__)

def parse_sql_node(state: AgentState) -> AgentState:
    """
    Lee y parsea el archivo SQL de entrada.
    
    Acciones:
    - Lee archivo desde state['sql_file_path']
    - Limpia comentarios y normaliza espacios
    - Crea SQLMetadata inicial
    - Actualiza logs
    
    Args:
        state: Estado actual del agente
        
    Returns:
        Estado actualizado con raw_sql y cleaned_sql
    """
    sql_file_path: Optional[str] = state.get('sql_file_path', '')
    sql_path: Path = Path(sql_file_path)

    logger.info("Leyendo SQL: %s", sql_file_path)

    # TODO Verificar que la variable no sea nula.
    # TODO Verificar que el archivo exista.
    # TODO Capturar error de lectura.

    with open(sql_path, 'r', encoding='utf-8') as f:
        raw_sql: str = f.read()
        logger.info("Archivo leído: %s (%s caracteres)", sql_path.name, len(raw_sql))

    parser: SQLParser = SQLParser()
    cleaned_sql: str = parser.clean_sql(raw_sql)
    logger.info("SQL limpiado (%s caracteres)", len(cleaned_sql))

    state['raw_sql'] = raw_sql
    state['cleaned_sql'] = cleaned_sql
    return state
