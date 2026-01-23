"""
Nodo para convertir sintaxis Athena SQL a Spark SQL.
"""

import logging
from typing import Dict, List, Optional, Tuple

import sqlglot
from sqlglot import exp

from src.models.cte_info import CTESourceInfo
from src.models.table_info import TableSourceInfo
from src.config import SOURCE_DIALECT, TARGET_DIALECT
from ..models.agent_state import AgentState
from ..utils.logging_config import setup_logger

logger: logging.Logger = setup_logger(__name__)


def clean_table_reference(node: exp.Table, known_tables: List[TableSourceInfo]) -> None:
    """Limpia referencias de catálogo/eschema si la tabla es conocida."""
    current_schema = node.db
    current_table = node.name

    table_var: Optional[str] = None
    for t in known_tables:
        if t.table.lower() == current_table.lower() and t.database.lower() == current_schema.lower():
            table_var = t.python_var
            break

    if table_var:
        node.set("db", None)
        node.set("catalog", None)


def apply_date_replacements(sql_text: str, replacements: Dict[str, str]) -> str:
    """
    Aplica reemplazos directos de texto para las fechas.
    Se hace sobre el string final para asegurar que la inyección de f-strings
    de Python ({time_config...}) no sea alterada por el parser SQL.
    """
    converted = sql_text
    for original, replacement in replacements.items():
        converted = converted.replace(original, replacement)
    return converted


def convert_syntax(sql_text: str, tables: List[TableSourceInfo], date_replacements: Dict[str, str]) -> str:
    """
    Parsea, limpia tablas, transpila a Spark y aplica reemplazos de fechas.

    Args:
        sql_text (str): SQL original.
        tables (List[TableSourceInfo]): Lista de tablas conocidas.
        date_replacements (Dict[str, str]): Mapa de reemplazos de fechas.

    Returns:
        str: SQL convertido a dialeco Spark.
    """
    expression = sqlglot.parse_one(sql_text, read=SOURCE_DIALECT)

    # 1. Limpieza de referencias de tablas en el AST
    for node in expression.find_all(exp.Table):
        clean_table_reference(node, tables)

    # 2. Transpilación a Spark
    converted_sql = expression.sql(dialect=TARGET_DIALECT, pretty=True)

    # 3. Inyección de variables de Tablas (f-strings)
    for tb in tables:
        converted_sql = converted_sql.replace(tb.table, '{' + f'{tb.python_var}' + '}')

    # 4. Inyección de variables de Fechas (f-strings)
    # Esto inyecta inicialmente "{time_config.fecha_corte...}"
    converted_sql = apply_date_replacements(converted_sql, date_replacements)

    return converted_sql


def configure_method_params(sql_text: str) -> Tuple[str, str, str]:
    """
    Analiza el SQL para determinar qué parámetros necesita el método Python
    y cómo debe ser llamado.

    Returns:
        Tuple[str, str, str]: (SQL ajustado con vars locales, Def de Args, Call de Args)
    """
    adjusted_sql = sql_text
    args_def_list = ["self"]
    args_call_list = []

    # Detectar y procesar fecha_corte_iso
    # El placeholder viene de extract_dates_node como "{time_config.fecha_corte_iso}"
    if "{time_config.fecha_corte_iso}" in adjusted_sql:
        # Cambiamos la referencia de objeto a variable local para el método
        adjusted_sql = adjusted_sql.replace("{time_config.fecha_corte_iso}", "'{fecha_corte_iso}'")
        args_def_list.append("fecha_corte_iso: str")
        args_call_list.append("self.time_config.fecha_corte_iso")

    # Detectar y procesar fecha_corte (entero/compacto)
    elif "{time_config.fecha_corte}" in adjusted_sql:
        adjusted_sql = adjusted_sql.replace("{time_config.fecha_corte}", "{fecha_corte}")
        args_def_list.append("fecha_corte: str")
        args_call_list.append("self.time_config.fecha_corte")

    args_def = ", ".join(args_def_list)
    args_call = ", ".join(args_call_list)

    return adjusted_sql, args_def, args_call


def convert_syntax_node(state: AgentState) -> AgentState:
    """
    Convierte sintaxis de Athena SQL a Spark SQL y configura parámetros de métodos.
    """
    tables: List[TableSourceInfo] = state["tables"]  # pyright: ignore[reportTypedDictNotRequiredAccess]
    ctes: List[CTESourceInfo] = state["ctes"] # pyright: ignore[reportTypedDictNotRequiredAccess]
    main_select: str = state["last_select"] # pyright: ignore[reportTypedDictNotRequiredAccess]
    date_replacements: Dict[str, str] = state.get("date_replacements", {})

    logger.info(
        "Iniciando conversión de sintaxis y configuración de parámetros...")

    # Procesar CTEs
    for cte in ctes:
        # 1. Conversión base
        temp_sql = convert_syntax(cte.inner_sql, tables=cte.tables, date_replacements=date_replacements)

        # 2. Configuración de parámetros dinámicos
        final_sql, args_def, args_call = configure_method_params(temp_sql)
        cte.new_sql = final_sql

        table_vars: str = "\n        ".join(
            [f'{tb.python_var} = self._get_table("{tb.table}")' for tb in cte.tables])
        if len(table_vars) > 0:
            table_vars: str = f"\n        {table_vars}"

        new_sql: str = cte.new_sql.replace('\n', '\n        ')
        # 3. Construcción del método Python con firma dinámica
        cte.python_method = f"""
    def get_cte_{cte.name}({args_def}) -> str:
        \"\"\"
        Descripción: Vista autogenerada en migración
        Vista resultado: {cte.name}
        \"\"\"{table_vars}
        return f\"\"\"
        {new_sql}
        \"\"\""""

        # 4. Construcción de la llamada con argumentos dinámicos
        cte.python_create = f"""self._create_view(self.sql_repo.get_cte_{cte.name}({args_call}), "{cte.name}")"""

        logger.info("CTE %s: Args detectados -> (%s)", cte.name, args_def)

    # Procesar Main Query
    temp_main = convert_syntax(main_select, tables=tables, date_replacements=date_replacements)
    final_main, _, _ = configure_method_params(temp_main)

    # Nota: El main query en el template actual suele ser llamado con fecha_corte_iso por defecto
    # pero aquí actualizamos el SQL para que use la variable local correcta.
    state["new_last_select"] = final_main

    logger.info("Conversión finalizada.")
    return state
