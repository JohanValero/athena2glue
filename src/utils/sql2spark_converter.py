"""
Conversor de sintaxis Athena SQL a Spark SQL.
"""

import re
import logging
from typing import List, Dict
from ..models.table_info import TableSourceInfo

logger = logging.getLogger(__name__)


class AthenaToSparkConverter:
    """
    Convierte sintaxis de Athena SQL a Spark SQL.

    Aplica transformaciones como:
    - VARCHAR → STRING
    - CURRENT_DATE → CURRENT_DATE()
    - date_add('day', N, fecha) → date_add(fecha, N)
    - Agrega prefijos de catálogo
    """

    # Diccionario de conversiones de tipos
    TYPE_CONVERSIONS = {
        r'\bVARCHAR\(\d+\)': 'STRING',
        r'\bVARCHAR\b': 'STRING',
        r'\bCHAR\(\d+\)': 'STRING',
        r'\bCHAR\b': 'STRING',
    }

    # Conversiones de funciones
    FUNCTION_CONVERSIONS = {
        # CURRENT_DATE sin paréntesis → CURRENT_DATE()
        r'\bCURRENT_DATE\b(?!\s*\()': 'CURRENT_DATE()',
        # CURRENT_TIMESTAMP sin paréntesis → CURRENT_TIMESTAMP()
        r'\bCURRENT_TIMESTAMP\b(?!\s*\()': 'CURRENT_TIMESTAMP()',
    }

    def __init__(self):
        """Inicializa el conversor."""
        pass

    def convert(self, athena_sql: str) -> str:
        """
        Convierte SQL de Athena a Spark.

        Args:
            athena_sql: SQL en sintaxis Athena

        Returns:
            SQL en sintaxis Spark
        """
        spark_sql = athena_sql

        # Aplicar conversiones de tipos
        for pattern, replacement in self.TYPE_CONVERSIONS.items():
            spark_sql = re.sub(pattern, replacement,
                               spark_sql, flags=re.IGNORECASE)

        # Aplicar conversiones de funciones
        for pattern, replacement in self.FUNCTION_CONVERSIONS.items():
            spark_sql = re.sub(pattern, replacement,
                               spark_sql, flags=re.IGNORECASE)

        # Convertir date_add de Athena a Spark
        # Athena: date_add('day', N, fecha)
        # Spark: date_add(fecha, N)
        spark_sql = self._convert_date_add(spark_sql)

        # Convertir CAST AS VARCHAR a CAST AS STRING
        spark_sql = re.sub(
            r'CAST\s*\((.*?)\s+AS\s+VARCHAR(?:\(\d+\))?\)',
            r'CAST(\1 AS STRING)',
            spark_sql,
            flags=re.IGNORECASE
        )

        logger.debug("Conversión Athena → Spark completada")
        return spark_sql

    def _convert_date_add(self, sql: str) -> str:
        """
        Convierte date_add de Athena a Spark.

        Athena: date_add('day', N, fecha)
        Spark: date_add(fecha, N)

        Args:
            sql: SQL con sintaxis Athena

        Returns:
            SQL con sintaxis Spark
        """
        # Patrón para date_add de Athena
        pattern = r"date_add\s*\(\s*['\"]day['\"]\s*,\s*(\d+|[\w.]+)\s*,\s*([^)]+)\)"

        def replacer(match):
            interval = match.group(1)
            date_expr = match.group(2).strip()
            return f"date_add({date_expr}, {interval})"

        result = re.sub(pattern, replacer, sql, flags=re.IGNORECASE)
        return result

    def add_catalog_prefix(self, sql: str, tables: List[TableSourceInfo]) -> str:
        """
        Agrega prefijos de catálogo a las tablas (glue_catalog. o spark_catalog.).

        Args:
            sql: SQL sin prefijos de catálogo
            tables: Lista de tablas con información de catálogo

        Returns:
            SQL con prefijos de catálogo
        """
        result = sql

        # Crear diccionario de mapeo tabla -> full_name
        table_map: Dict[str, str] = {}

        for table in tables:
            # Mapear database.table → catalog.database.table
            table_map[table.short_name] = table.full_name
            # Mapear solo table → catalog.database.table
            table_map[table.table] = table.full_name

        # Reemplazar referencias a tablas
        for short_name, full_name in table_map.items():
            # Patrón para buscar tabla en FROM o JOIN
            # Evitar reemplazar si ya tiene prefijo de catálogo
            pattern = r'\b(?<!\.){}\b(?!\.)'.format(re.escape(short_name))

            # Solo reemplazar en contextos FROM/JOIN
            # Hacemos un reemplazo más cuidadoso
            result = re.sub(
                r'(\bFROM\s+)' + pattern,
                r'\1' + full_name,
                result,
                flags=re.IGNORECASE
            )
            result = re.sub(
                r'(\bJOIN\s+)' + pattern,
                r'\1' + full_name,
                result,
                flags=re.IGNORECASE
            )

        return result

    def convert_cte(self, cte_sql: str) -> str:
        """
        Convierte un CTE individual de Athena a Spark.

        Args:
            cte_sql: SQL del CTE en Athena

        Returns:
            SQL del CTE en Spark
        """
        return self.convert(cte_sql)
