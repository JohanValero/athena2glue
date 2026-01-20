"""
Nodo para generar código PySpark final.
"""

import logging
from typing import List

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
    """
    logger.info("Generating code template.")

    tables: List[TableSourceInfo] = state["tables"] # pyright: ignore[reportTypedDictNotRequiredAccess]
    ctes: List[CTESourceInfo] = state["ctes"] # pyright: ignore[reportTypedDictNotRequiredAccess]
    main_select: str = state["last_select"] # pyright: ignore[reportTypedDictNotRequiredAccess]

    with open("./documents/glue_template.py", "r", encoding="utf-8") as file:
        code_template: str = file.read()

    main_tables: str = "\n        ".join([f'{tb.python_var} = self._get_table("{tb.table}")' for tb in tables])
    main_select: str = state["new_last_select"] # pyright: ignore[reportTypedDictNotRequiredAccess]
    list_table: str = ",".join([tb.full_name for tb in tables])
    list_cte_methods: List[str] = [cte.python_method for cte in ctes]
    list_cte_create: List[str] = [cte.python_create for cte in ctes]

    code_template: str = code_template.replace("#SETI_TAG_LISTA_TABLAS", list_table)
    code_template: str = code_template.replace("#SETI_TAG_METHODS_QUERY_REPLACE", "\n        ".join(list_cte_methods))
    code_template: str = code_template.replace("#SETI_TAG_CREATE_VIEWS", "\n        ".join(list_cte_create))
    code_template: str = code_template.replace("#SETI_TAG_FINAL_TABLES", main_tables)
    code_template: str = code_template.replace("#SETI_TAG_F_STRING_FINAL_QUERY", main_select)

    output_dir: str = state["output_dir"] # pyright: ignore[reportTypedDictNotRequiredAccess]
    business_name: str = state["business_name"] # pyright: ignore[reportTypedDictNotRequiredAccess]

    with open(f"{output_dir}/GL_HD_SAS_THR_{business_name}.py", "w", encoding="utf-8") as file:
        file.write(code_template)

    return state
