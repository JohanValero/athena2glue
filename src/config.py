"""
Configuración centralizada para el agente de migración Athena2Glue.
"""

from pathlib import Path

# Configuración de dialectos SQL
SOURCE_DIALECT = "trino"
TARGET_DIALECT = "spark"

# Configuración por defecto
DEFAULT_OUTPUT_DIR = "./output"
DEFAULT_SQL_ENCODING = "utf-8"
TEMPLATE_PATH = "./documents/glue_template.py"
JOB_PREFIX = "GL_HD_SAS_THR_"

# Configuración de flags
USE_LLM = False

# Configuración de tablas (hardcoded por ahora, idealmente dinámico)
TABLE_CONFIG = {
    "dwh_thr_modelo_datos.dim_tiempo": "iceberg",
    "dwh_thr_reportes.fct_saldos_semanal_detallado": "iceberg",
    "stg_cap.stg_segmentacion_saldos_trad": "iceberg",
    "stg_cap.stg_segmentacion_saldos_pib": "iceberg"
}
