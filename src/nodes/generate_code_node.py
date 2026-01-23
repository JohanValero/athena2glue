"""
Nodo para generar código PySpark final.
"""

import logging
from typing import List

from pathlib import Path

from src.config import JOB_PREFIX, TEMPLATE_PATH, DEFAULT_SQL_ENCODING
from ..models.cte_info import CTESourceInfo
from ..models.table_info import TableSourceInfo
from ..models.agent_state import AgentState
from ..utils.logging_config import setup_logger

logger: logging.Logger = setup_logger(__name__)


def generate_code_node(state: AgentState) -> AgentState:
    """
    Genera código PySpark final usando plantilla.

    Acciones:
    - Carga plantilla_glue_job_estandar.py
    - Genera SQLQueryRepository con métodos para cada CTE
    - Genera ViewCreator con orden de CTEs
    - Genera lista de tablas fuente
    - Construye archivo .py completo

    Args:
        state: Estado actual del agente.

    Returns:
        AgentState: Estado actualizado con generated_code.

    Raises:
        FileNotFoundError: Si no se encuentra el template.
        IOError: Si hay errores escribiendo el archivo.
    """
    logger.info("Generating code template.")

    tables: List[TableSourceInfo] = state["tables"] # pyright: ignore[reportTypedDictNotRequiredAccess]
    ctes: List[CTESourceInfo] = state["ctes"] # pyright: ignore[reportTypedDictNotRequiredAccess]
    # main_select: str = state["last_select"]  <-- This was unused in original code, actually

    template_path = Path(TEMPLATE_PATH)
    if not template_path.exists():
        error_msg = f"Template file not found at {template_path}"
        logger.error(error_msg)
        raise FileNotFoundError(error_msg)

    try:
        with open(template_path, "r", encoding=DEFAULT_SQL_ENCODING) as file:
            code_template: str = file.read()
    except Exception as e:
        error_msg = f"Error reading template {template_path}: {e}"
        logger.error(error_msg)
        raise IOError(error_msg) from e

    main_tables: str = "\n        ".join([f'{tb.python_var} = self._get_table("{tb.table}")' for tb in tables])
    main_select: str = state["new_last_select"] # pyright: ignore[reportTypedDictNotRequiredAccess]
    list_table: str = ",".join([tb.full_name for tb in tables])
    list_cte_methods: List[str] = [cte.python_method for cte in ctes]
    list_cte_create: List[str] = [cte.python_create for cte in ctes]

    code_template = code_template.replace("#SETI_TAG_LISTA_TABLAS", list_table)
    code_template = code_template.replace("#SETI_TAG_METHODS_QUERY_REPLACE", "\n".join(list_cte_methods))
    code_template = code_template.replace("#SETI_TAG_CREATE_VIEWS", "\n        ".join(list_cte_create))
    code_template = code_template.replace("#SETI_TAG_FINAL_TABLES", main_tables)
    code_template = code_template.replace("#SETI_TAG_F_STRING_FINAL_QUERY", main_select)

    output_dir: str = state["output_dir"] # pyright: ignore[reportTypedDictNotRequiredAccess]
    business_name: str = state["business_name"] # pyright: ignore[reportTypedDictNotRequiredAccess]

    output_filename = f"{JOB_PREFIX}{business_name}.py"
    output_path = Path(output_dir) / output_filename
    
    try:
        with open(output_path, "w", encoding=DEFAULT_SQL_ENCODING) as file:
            file.write(code_template)
        logger.info("Generated Job file at: %s", output_path)
    except Exception as e:
        error_msg = f"Error writing output file {output_path}: {e}"
        logger.error(error_msg)
        raise IOError(error_msg) from e

    return state
