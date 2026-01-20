"""
Parser de consultas SQL para extraer CTEs y estructura.
"""

import re
import logging
from typing import List, Tuple

logger = logging.getLogger(__name__)


class SQLParser:
    """
    Parser de consultas SQL de Athena.

    Extrae CTEs, tablas, y estructura de la consulta.
    """

    @staticmethod
    def clean_sql(sql: str) -> str:
        """
        Limpia el SQL removiendo comentarios y normalizando espacios.

        Args:
            sql: SQL original

        Returns:
            SQL limpio
        """
        # Remover comentarios de línea --
        sql = re.sub(r'--[^\n]*', '', sql)
        # Remover comentarios multi-línea /* */
        sql = re.sub(r'/\*.*?\*/', '', sql, flags=re.DOTALL)
        # Normalizar espacios
        sql = re.sub(r'\s+', ' ', sql)
        sql = sql.strip()
        return sql

    @staticmethod
    def extract_ctes(sql: str) -> Tuple[List[Tuple[str, str]], str]:
        """
        Extrae CTEs de una consulta SQL usando regex.

        Este método usa regex en lugar de sqlparse para manejar consultas grandes
        que exceden el límite de 10,000 tokens de sqlparse.

        Args:
            sql: SQL completo

        Returns:
            Tupla de (lista de (nombre_cte, query_cte), query_final)
        """
        # Buscar pattern WITH al inicio
        with_pattern = r'^\s*WITH\s+'

        if not re.search(with_pattern, sql, re.IGNORECASE | re.MULTILINE):
            logger.info("No se encontraron CTEs en el SQL")
            return [], sql

        # Encontrar posición de WITH
        with_match = re.search(with_pattern, sql, re.IGNORECASE)
        if not with_match:
            return [], sql

        # SQL después del WITH
        after_with = sql[with_match.end():]

        ctes = []
        pos = 0

        while pos < len(after_with):
            # Buscar patrón: nombre_cte AS (
            cte_pattern = r'\s*(\w+)\s+AS\s*\('
            match = re.search(cte_pattern, after_with[pos:], re.IGNORECASE)

            if not match:
                # No hay más CTEs, el resto es el SELECT final
                final_select = after_with[pos:].strip()
                # Remover la última coma si existe
                if ctes and after_with[pos:].strip().startswith(','):
                    final_select = after_with[pos+1:].strip()
                break

            cte_name = match.group(1)
            start_paren = pos + match.end() - 1  # Posición del '('

            # Encontrar el paréntesis de cierre correspondiente
            paren_count = 1
            pos_scan = start_paren + 1

            while pos_scan < len(after_with) and paren_count > 0:
                if after_with[pos_scan] == '(':
                    paren_count += 1
                elif after_with[pos_scan] == ')':
                    paren_count -= 1
                pos_scan += 1

            if paren_count != 0:
                logger.warning("Paréntesis no balanceados para CTE '%s'", cte_name)
                break

            # Extraer query del CTE (sin paréntesis externos)
            cte_query = after_with[start_paren + 1:pos_scan - 1].strip()
            ctes.append((cte_name, cte_query))
            logger.debug("CTE extraído: %s (%s caracteres)", cte_name, len(cte_query))

            # Mover posición después del paréntesis de cierre
            pos = pos_scan

            # Buscar coma o SELECT inicial (fin de CTEs)
            remaining = after_with[pos:].lstrip()
            if remaining.startswith(','):
                pos += len(after_with[pos:]) - len(remaining) + 1
            elif remaining.upper().startswith('SELECT'):
                # Encontramos el SELECT final
                final_select = remaining
                break
        else:
            # Si salimos del loop sin break, no hay SELECT final explícito
            final_select = ""

        logger.info("Extraídos %s CTEs usando regex", len(ctes))
        return ctes, final_select # pyright: ignore[reportPossiblyUnboundVariable]

    @staticmethod
    def _parse_cte_definitions(cte_text: str) -> List[Tuple[str, str]]:
        """
        Parsea definiciones de CTEs del bloque WITH.

        Args:
            cte_text: Texto del bloque WITH (sin la palabra WITH)

        Returns:
            Lista de (nombre_cte, query_cte)
        """
        ctes = []

        # Patrón para identificar CTEs: nombre AS (query)
        # Usamos regex para encontrar cada CTE

        # Buscar todas las apariciones de "nombre AS ("
        pattern = r'(\w+)\s+AS\s*\('

        matches = list(re.finditer(pattern, cte_text, re.IGNORECASE))

        for _, match in enumerate(matches):
            cte_name = match.group(1)
            start_pos = match.end()  # Posición después de "("

            # Encontrar el paréntesis de cierre correspondiente
            paren_count = 1
            pos = start_pos

            while pos < len(cte_text) and paren_count > 0:
                if cte_text[pos] == '(':
                    paren_count += 1
                elif cte_text[pos] == ')':
                    paren_count -= 1
                pos += 1

            if paren_count == 0:
                cte_query = cte_text[start_pos:pos-1].strip()
                ctes.append((cte_name, cte_query))
                logger.debug("CTE extraído: %s (%s caracteres)", cte_name, len(cte_query))
        return ctes

    @staticmethod
    def extract_table_names(sql: str) -> List[str]:
        """
        Extrae nombres de tablas FROM/JOIN del SQL.

        Args:
            sql: Query SQL

        Returns:
            Lista de nombres de tablas (puede incluir alias)
        """
        tables = []

        # Patrón para FROM y JOIN
        from_pattern = r'\bFROM\s+([^\s,;()]+)'
        join_pattern = r'\bJOIN\s+([^\s,;()]+)'

        # Buscar FROM
        for match in re.finditer(from_pattern, sql, re.IGNORECASE):
            table = match.group(1).strip()
            if table and table.upper() not in ('SELECT', 'WHERE', 'GROUP', 'ORDER'):
                tables.append(table)

        # Buscar JOINs
        for match in re.finditer(join_pattern, sql, re.IGNORECASE):
            table = match.group(1).strip()
            if table and table.upper() not in ('SELECT', 'WHERE', 'GROUP', 'ORDER'):
                tables.append(table)

        # Limpiar duplicados manteniendo orden
        seen = set()
        unique_tables = []
        for table in tables:
            if table not in seen:
                seen.add(table)
                unique_tables.append(table)

        logger.info("Tablas extraídas: %s", unique_tables)
        return unique_tables

    @staticmethod
    def detect_date_literals(sql: str) -> List[str]:
        """
        Detecta fechas hardcodeadas en el SQL.

        Args:
            sql: Query SQL

        Returns:
            Lista de fechas encontradas
        """
        dates = []

        # Patrón para DATE 'YYYY-MM-DD'
        date_pattern = r"DATE\s*['\"](\d{4}-\d{2}-\d{2})['\"]"
        dates.extend(re.findall(date_pattern, sql, re.IGNORECASE))

        # Patrón para 'YYYY-MM-DD' solo
        simple_date_pattern = r"['\"](\d{4}-\d{2}-\d{2})['\"]"
        dates.extend(re.findall(simple_date_pattern, sql))

        # Patrón para YYYYMMDD
        numeric_date_pattern = r'\b(\d{8})\b'
        numeric_dates = re.findall(numeric_date_pattern, sql)

        # Convertir YYYYMMDD a YYYY-MM-DD
        for nd in numeric_dates:
            if nd not in dates:
                formatted = f"{nd[:4]}-{nd[4:6]}-{nd[6:8]}"
                dates.append(formatted)

        # Eliminar duplicados
        dates = list(set(dates))

        if dates:
            logger.info("Fechas detectadas: %s", dates)
        return dates
