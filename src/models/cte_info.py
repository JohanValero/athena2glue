"""
Información sobre CTEs de la consulta.
"""

from dataclasses import dataclass
from typing import List

from src.models.table_info import TableSourceInfo

@dataclass
class CTESourceInfo:
    """
    Información de una tabla fuente.
    
    Attributes:
        name: Nombre de la CTE
        inner_sql: SQL que contiene la CTE.
        position: Indice de la CTE
    """
    name: str
    inner_sql: str
    new_sql: str
    position: int
    python_method: str
    python_create: str
    tables: List[TableSourceInfo]
