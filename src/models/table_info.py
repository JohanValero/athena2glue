"""
Información sobre tablas fuente del catálogo de Glue.
"""

from dataclasses import dataclass
from typing import Literal, Optional

TableType = Literal["iceberg", "parquet", "delta", "hive", "unknown"]
CatalogType = Literal["glue_catalog", "spark_catalog"]


@dataclass
class TableSourceInfo:
    """
    Información de una tabla fuente.
    
    Attributes:
        full_name: Nombre completo (catalog.database.table)
        catalog: Catálogo (glue_catalog, spark_catalog)
        database: Nombre de la base de datos
        table: Nombre de la tabla
        alias: Alias para referencia en código
        table_type: Tipo de tabla (iceberg, parquet, delta, hive, unknown)
    """

    full_name: str
    catalog: Optional[CatalogType]
    catalog_name: str
    database: str
    table: str
    table_type: TableType
    python_get: str
    python_var: str

    @property
    def short_name(self) -> str:
        """Retorna database.table"""
        return f"{self.database}.{self.table}"

    def __str__(self) -> str:
        return f"{self.full_name} ({self.table_type})"
