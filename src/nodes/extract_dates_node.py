"""
Nodo para extraer y clasificar fechas hardcodeadas candidatas a ser fecha_corte.
"""

import logging
import re
from datetime import datetime
from collections import Counter
from typing import List, Dict, Tuple, Optional

import sqlglot
from sqlglot import exp

from ..models.agent_state import AgentState
from ..models.cte_info import CTESourceInfo
from ..utils.logging_config import setup_logger

logger: logging.Logger = setup_logger(__name__)

# Regex para formatos comunes
REGEX_ISO = re.compile(r"^\d{4}-\d{2}-\d{2}$")  # 2025-01-01
REGEX_INT = re.compile(r"^\d{8}$")             # 20250101


def normalize_date(value: str) -> Optional[datetime]:
    """
    Intenta convertir un string a datetime object.

    Soporta formatos:
    - YYYY-MM-DD
    - YYYYMMDD

    Args:
        value (str): String de fecha.

    Returns:
        Optional[datetime]: Objeto datetime si es válido, None si no.
    """
    try:
        if REGEX_ISO.match(value):
            return datetime.strptime(value, "%Y-%m-%d")
        if REGEX_INT.match(value):
            return datetime.strptime(value, "%Y%m%d")
    except ValueError:
        return None
    return None


def find_literals_in_sql(sql_text: str) -> List[Tuple[str, datetime]]:
    """
    Recorre el AST buscando literales que parezcan fechas.
    Retorna lista de tuplas (valor_original, fecha_objeto).
    """
    found = []
    try:
        # Usamos sqlglot para no parsear falsos positivos en comentarios o nombres de col
        parsed = sqlglot.parse_one(sql_text)

        # Buscar literales de texto y numéricos
        for node in parsed.find_all(exp.Literal):
            value = node.this
            if isinstance(value, str):
                dt = normalize_date(value)
                if dt:
                    found.append((value, dt))
    except Exception as e:
        logger.warning("Error parseando fechas en fragmento SQL: %s", e)

    return found


def extract_dates_node(state: AgentState) -> AgentState:
    """
    Analiza CTEs y Query Principal para encontrar la fecha de corte hardcodeada.

    Estrategia:
    1. Recolectar todos los literales de fecha.
    2. Identificar la fecha más frecuente (Moda).
    3. Generar un mapa de reemplazo para convertir esa fecha en variable Python.
       - YYYY-MM-DD -> {time_config.fecha_corte_iso}
       - YYYYMMDD   -> {time_config.fecha_corte}
    """
    logger.info("Iniciando análisis de fechas hardcodeadas...")

    ctes: List[CTESourceInfo] = state.get("ctes", [])
    main_query: str = state.get("last_select", "")

    all_candidates: List[Tuple[str, datetime]] = []

    # 1. Buscar en CTEs
    for cte in ctes:
        candidates = find_literals_in_sql(cte.inner_sql)
        all_candidates.extend(candidates)

    # 2. Buscar en Main Query
    main_candidates = find_literals_in_sql(main_query)
    all_candidates.extend(main_candidates)

    if not all_candidates:
        logger.info("No se encontraron fechas hardcodeadas.")
        state["date_replacements"] = {}
        return state

    # 3. Determinar la fecha dominante (La que asumiremos como fecha de corte)
    date_objects = [dt for _, dt in all_candidates]
    most_common_date, count = Counter(date_objects).most_common(1)[0]

    logger.info("Fecha dominante detectada: %s (Aparece %s veces)", most_common_date.date(), count)

    # 4. Construir mapa de reemplazos solo para la fecha dominante
    replacements: Dict[str, str] = {}

    for original_str, dt in all_candidates:
        if dt == most_common_date:
            # Determinamos el formato de salida según el formato de entrada
            # Esto asume que el template tiene un objeto 'time_config' disponible
            if "-" in original_str:
                # Formato '2025-11-07' -> variable ISO
                # Para strings SQL
                replacements[f"'{original_str}'"] = "{time_config.fecha_corte_iso}"
                # Para usos sin comillas (raro en ISO)
                replacements[original_str] = "{time_config.fecha_corte_iso}"
            else:
                # Formato 20251107 -> variable numérica/string compacto
                replacements[original_str] = "{time_config.fecha_corte}"

    logger.info("Reemplazos generados: %s", replacements)

    state["date_replacements"] = replacements
    return state
