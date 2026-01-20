"""
Nodo para parsear el archivo SQL de entrada.
"""

import logging
from pathlib import Path
from typing import Optional

from src.config import DEFAULT_SQL_ENCODING
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

    Raises:
        ValueError: Si el path del archivo SQL no es válido.
        FileNotFoundError: Si el archivo no existe.
        IOError: Si hay errores de lectura.
    """
    sql_file_path: Optional[str] = state.get('sql_file_path')
    
    if not sql_file_path:
        error_msg = "La ruta del archivo SQL no está definida en el estado."
        logger.error(error_msg)
        raise ValueError(error_msg)

    sql_path: Path = Path(sql_file_path)
    logger.info("Leyendo SQL: %s", sql_path)

    if not sql_path.exists():
        error_msg = f"El archivo SQL no existe: {sql_path}"
        logger.error(error_msg)
        raise FileNotFoundError(error_msg)

    try:
        with open(sql_path, 'r', encoding=DEFAULT_SQL_ENCODING) as f:
            raw_sql: str = f.read()
            logger.info("Archivo leído: %s (%s caracteres)", sql_path.name, len(raw_sql))
    except Exception as e:
        error_msg = f"Error leyendo el archivo SQL {sql_path}: {e}"
        logger.error(error_msg)
        raise IOError(error_msg) from e

    parser: SQLParser = SQLParser()
    cleaned_sql: str = parser.clean_sql(raw_sql)
    logger.info("SQL limpiado (%s caracteres)", len(cleaned_sql))

    state['raw_sql'] = raw_sql
    state['cleaned_sql'] = cleaned_sql
    return state
