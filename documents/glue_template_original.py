# -*- coding: utf-8 -*-
"""
Plantilla Estándar para AWS Glue Jobs con PySpark e Iceberg
===========================================================
Versión: 3.0
Fecha: Enero 2026

Características:
- Estructura modular con interfaces ABC
- Manejo automático de fecha_corte
- DELETE + INSERT para tablas Iceberg
- Creación automática de tabla si no existe
- Columnas de auditoría (fecha_proceso, particiones)
- Parámetro table_source_list para tablas fuente dinámicas

Parámetros del Job:
--JOB_NAME: Nombre del job (automático)
--db_name: Base de datos principal de consulta
--db_output: Base de datos de destino para tablas Iceberg
--bucket_target: Bucket S3 donde se guardarán los archivos y tablas
--folder_target: Carpeta destino en S3 para organización de archivos
--env: Ambiente de ejecución (dev, test, prod)
--fecha_corte: Fecha de corte (1900-01-01 = fecha actual Colombia)
--table_source_list: Lista de tablas fuente separadas por coma
                     Formato: database1.tabla1,database2.tabla2
"""

# ============================================================================
# IMPORTS - Nativos de Python
# ============================================================================
import datetime
import time
import logging
from typing import Dict, List, Optional, Any, NamedTuple
from dataclasses import dataclass, field
from abc import ABC, abstractmethod
from sys import argv, stdout, exc_info

# ============================================================================
# IMPORTS - Terceros
# ============================================================================
from pytz import timezone
import awswrangler as wr

# ============================================================================
# IMPORTS - AWS Glue
# ============================================================================
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext

# ============================================================================
# IMPORTS - PySpark
# ============================================================================
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, lit, when, trim, expr, to_date, coalesce,
    row_number, current_timestamp, concat_ws,
    sum as spark_sum, max as spark_max, min as spark_min, 
    to_timestamp, year, month, dayofmonth
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DecimalType, DateType, TimestampType
)
from pyspark.sql.window import Window


# ============================================================================
# CONSTANTES Y CONFIGURACIÓN
# ============================================================================

ERROR_MSG_LOG_FORMAT = "{} (línea: {}, {}): {}."
LOG_FORMAT = "%(asctime)s [%(levelname)s] [%(filename)s](%(funcName)s): %(message)s"
CATALOG = "glue_catalog"

# Constantes para fecha por defecto
FECHA_DEFAULT = "1900-01-01"
FECHA_DEFAULT_YYYYMMDD = "19000101"


# ============================================================================
# DATACLASSES DE CONFIGURACIÓN
# ============================================================================

class TableSource(NamedTuple):
    """Representa una tabla fuente con su database y nombre."""
    database: str
    table: str
    alias: str  # Alias para usar en queries (ej: 'tbl_clientes')
    
    @property
    def full_name(self) -> str:
        """Retorna nombre completo: glue_catalog.database.tabla"""
        return f"glue_catalog.{self.database}.{self.table}"
    
    @property
    def short_name(self) -> str:
        """Retorna nombre corto: database.tabla"""
        return f"{self.database}.{self.table}"


@dataclass
class TimeConfig:
    """
    Configuración de tiempo para el job.
    
    Maneja dos escenarios:
    1. fecha_corte = '1900-01-01' → Calcula fecha actual de Colombia
    2. fecha_corte = 'YYYY-MM-DD' → Usa la fecha proporcionada
    """
    start_time: float
    date_time: datetime.datetime
    timer_date: str
    previous_date: str
    year: str
    month: str
    day: str
    today_date: str
    fecha_corte: str          # Formato YYYYMMDD
    fecha_corte_iso: str      # Formato YYYY-MM-DD
    fecha_calculada: bool     # True si se calculó automáticamente

    @classmethod
    def create(cls, fecha_corte_param: str = None) -> 'TimeConfig':
        """Factory method para crear configuración de tiempo."""
        start_time = time.time()
        date_time = datetime.datetime.now(timezone("America/Bogota"))
        timer_date = date_time.strftime("%Y-%m-%dT%H:%M:%S-05:00")
        date_previous = date_time - datetime.timedelta(days=1)
        
        fecha_corte, fecha_corte_iso, fecha_calculada = cls._resolver_fecha_corte(
            fecha_corte_param, 
            date_time
        )
        
        fecha_corte_date = cls._parse_fecha(fecha_corte_iso)
        
        return cls(
            start_time=start_time,
            date_time=date_time,
            timer_date=timer_date,
            previous_date=date_previous.strftime("%Y%m%d"),
            year=fecha_corte_date.strftime("%Y"),
            month=fecha_corte_date.strftime("%m"),
            day=fecha_corte_date.strftime("%d"),
            today_date=date_time.strftime("%Y-%m-%d"),
            fecha_corte=fecha_corte,
            fecha_corte_iso=fecha_corte_iso,
            fecha_calculada=fecha_calculada
        )
    
    @staticmethod
    def _resolver_fecha_corte(fecha_param: str, fecha_actual: datetime.datetime) -> tuple:
        """Resuelve la fecha de corte a usar."""
        if fecha_param is None:
            fecha_iso = fecha_actual.strftime("%Y-%m-%d")
            fecha_yyyymmdd = fecha_actual.strftime("%Y%m%d")
            return (fecha_yyyymmdd, fecha_iso, True)
        
        fecha_param = fecha_param.strip()
        
        if fecha_param in (FECHA_DEFAULT, FECHA_DEFAULT_YYYYMMDD, ''):
            fecha_iso = fecha_actual.strftime("%Y-%m-%d")
            fecha_yyyymmdd = fecha_actual.strftime("%Y%m%d")
            return (fecha_yyyymmdd, fecha_iso, True)
        
        if '-' in fecha_param:
            fecha_iso = fecha_param
            fecha_yyyymmdd = fecha_param.replace('-', '')
        else:
            fecha_yyyymmdd = fecha_param
            fecha_iso = f"{fecha_param[:4]}-{fecha_param[4:6]}-{fecha_param[6:8]}"
        
        return (fecha_yyyymmdd, fecha_iso, False)
    
    @staticmethod
    def _parse_fecha(fecha_iso: str) -> datetime.date:
        """Parsea una fecha ISO a objeto date."""
        return datetime.datetime.strptime(fecha_iso, "%Y-%m-%d").date()
    
    def get_fecha_display(self) -> str:
        """Retorna string descriptivo de la fecha para logs."""
        if self.fecha_calculada:
            return f"{self.fecha_corte_iso} (calculada automáticamente)"
        return f"{self.fecha_corte_iso} (proporcionada por parámetro)"


@dataclass
class JobConfig:
    """Configuración del Job."""
    db_name: str
    db_output: str
    bucket_target: str
    folder_target: str
    env: str
    table_sources: List[TableSource] = field(default_factory=list)
    catalog: str = CATALOG
    output_table_name: str = "tabla_resultado"
    
    def get_table(self, alias: str) -> Optional[TableSource]:
        """
        Obtiene una tabla fuente por su alias.
        
        Args:
            alias: Alias de la tabla (ej: 'homologacion', 'calendario')
        
        Returns:
            TableSource si existe, None si no
        """
        for table in self.table_sources:
            if table.alias == alias:
                return table
        return None
    
    def get_table_full_name(self, alias: str) -> str:
        """
        Obtiene el nombre completo de una tabla por su alias.
        
        Args:
            alias: Alias de la tabla
        
        Returns:
            Nombre completo (glue_catalog.database.tabla) o string vacío
        """
        table = self.get_table(alias)
        return table.full_name if table else ""
    
    @classmethod
    def parse_table_source_list(cls, table_source_list: str) -> List[TableSource]:
        """
        Parsea el parámetro table_source_list y crea lista de TableSource.
        
        Formato esperado: "database1.tabla1,database2.tabla2"
        El alias se genera automáticamente del nombre de la tabla.
        
        Args:
            table_source_list: String con tablas separadas por coma
        
        Returns:
            Lista de TableSource
        
        Ejemplo:
            Input: "stg_catalogo.homologacion,stg_catalogo.calendario"
            Output: [
                TableSource(database='stg_catalogo', table='homologacion', alias='homologacion'),
                TableSource(database='stg_catalogo', table='calendario', alias='calendario')
            ]
        """
        if not table_source_list or table_source_list.strip() == '':
            return []
        
        tables = []
        for item in table_source_list.split(','):
            item = item.strip()
            if '.' in item:
                parts = item.split('.', 1)  # Solo dividir en el primer punto
                database = parts[0].strip()
                table = parts[1].strip()
                alias = table  # Usar nombre de tabla como alias por defecto
                tables.append(TableSource(database=database, table=table, alias=alias))
        
        return tables


# ============================================================================
# CONFIGURACIÓN DE LOGGING
# ============================================================================

class LoggerSetup:
    """Configuración de logging."""
    
    @staticmethod
    def setup_logging(log_level: int = logging.INFO) -> logging.Logger:
        """Configura el logger principal."""
        logger = logging.getLogger()
        logger.handlers.clear()
        handler = logging.StreamHandler(stdout)
        handler.setFormatter(logging.Formatter(LOG_FORMAT))
        logger.addHandler(handler)
        logger.setLevel(log_level)
        return logger

    @staticmethod
    def create_log_msg(log_msg: str) -> str:
        """Crea un mensaje de log con detalles de excepción si aplica."""
        exception_type, exception_value, traceback = exc_info()
        if not exception_type:
            return f"{log_msg}."
        return ERROR_MSG_LOG_FORMAT.format(
            log_msg, 
            traceback.tb_lineno, 
            exception_type.__name__, 
            exception_value
        )


# ============================================================================
# INTERFACES (ABSTRACT BASE CLASSES)
# ============================================================================

class ISparkService(ABC):
    """Interface para operaciones de Spark."""
    
    @abstractmethod
    def sql(self, query: str) -> DataFrame:
        """Ejecuta una consulta SQL."""
        pass
    
    @abstractmethod
    def create_temp_view(self, df: DataFrame, view_name: str) -> None:
        """Crea una vista temporal."""
        pass


class IS3Service(ABC):
    """Interface para operaciones de S3/Iceberg."""
    
    @abstractmethod
    def table_exists(self, table: str, database: str) -> bool:
        """Verifica si una tabla existe."""
        pass
    
    @abstractmethod
    def create_iceberg_table_from_df(
        self, 
        df: DataFrame, 
        table: str, 
        database: str,
        location: str,
        partition_cols: List[str] = None
    ) -> None:
        """Crea una tabla Iceberg con datos usando CTAS."""
        pass
    
    @abstractmethod
    def delete_insert_by_date(
        self, 
        df: DataFrame, 
        table: str, 
        database: str,
        date_column: str,
        date_value: str,
        location: str = None,
        partition_cols: List[str] = None
    ) -> None:
        """Elimina registros por fecha e inserta nuevos datos."""
        pass


# ============================================================================
# IMPLEMENTACIONES DE SERVICIOS
# ============================================================================

class SparkService(ISparkService):
    """Implementación de operaciones Spark."""
    
    def __init__(self, spark: SparkSession, logger: logging.Logger):
        self.spark = spark
        self.logger = logger
    
    def sql(self, query: str) -> DataFrame:
        """Ejecuta una consulta SQL."""
        return self.spark.sql(query)
    
    def create_temp_view(self, df: DataFrame, view_name: str) -> None:
        """Crea una vista temporal."""
        df.createOrReplaceTempView(view_name)
        self.logger.info(f"Vista temporal creada: {view_name}")


class S3Service(IS3Service):
    """Implementación de operaciones S3/Iceberg con creación automática de tabla."""
    
    def __init__(self, spark: SparkSession, logger: logging.Logger):
        self.spark = spark
        self.logger = logger
    
    def table_exists(self, table: str, database: str) -> bool:
        """Verifica si una tabla Iceberg existe en el catálogo."""
        full_table_name = f"glue_catalog.{database}.{table}"
        try:
            self.spark.sql(f"DESCRIBE TABLE {full_table_name}")
            self.logger.info(f"Tabla {full_table_name} existe")
            return True
        except Exception as e:
            error_msg = str(e)
            if "Table or view not found" in error_msg or "TABLE_OR_VIEW_NOT_FOUND" in error_msg:
                self.logger.info(f"Tabla {full_table_name} no existe")
                return False
            self.logger.warning(f"Error verificando tabla {full_table_name}: {e}")
            return False
    
    def create_iceberg_table_from_df(
        self, 
        df: DataFrame, 
        table: str, 
        database: str,
        location: str,
        partition_cols: List[str] = None
    ) -> None:
        """Crea una tabla Iceberg usando CREATE TABLE AS SELECT (CTAS)."""
        full_table_name = f"glue_catalog.{database}.{table}"
        
        try:
            self.logger.info(f"Creando tabla Iceberg: {full_table_name}")
            self.logger.info(f"Ubicación: {location}")
            
            temp_view = f"temp_create_{table}"
            df.createOrReplaceTempView(temp_view)
            
            partition_clause = ""
            if partition_cols:
                partition_clause = f"PARTITIONED BY ({', '.join(partition_cols)})"
                self.logger.info(f"Particiones: {partition_cols}")
            
            create_query = f"""
            CREATE TABLE IF NOT EXISTS {full_table_name}
            USING iceberg
            LOCATION '{location}'
            {partition_clause}
            TBLPROPERTIES (
                'format-version' = '2',
                'write.format.default' = 'parquet'
            )
            AS SELECT * FROM {temp_view}
            """
            
            self.spark.sql(create_query)
            self.logger.info(f"Tabla {full_table_name} creada exitosamente")
            
            self.spark.catalog.dropTempView(temp_view)
            
        except Exception as e:
            self.logger.error(f"Error creando tabla {full_table_name}: {e}")
            raise
    
    def delete_insert_by_date(
        self, 
        df: DataFrame, 
        table: str, 
        database: str,
        date_column: str,
        date_value: str,
        location: str = None,
        partition_cols: List[str] = None
    ) -> None:
        """Elimina registros por fecha e inserta nuevos datos."""
        full_table_name = f"glue_catalog.{database}.{table}"
        
        try:
            if not self.table_exists(table, database):
                self.logger.info(f"Tabla {full_table_name} no existe. Creándola...")
                
                if location is None:
                    raise ValueError(
                        f"Se requiere 'location' para crear la tabla {full_table_name}"
                    )
                
                self.create_iceberg_table_from_df(
                    df=df,
                    table=table,
                    database=database,
                    location=location,
                    partition_cols=partition_cols
                )
                
                record_count = df.count()
                self.logger.info(f"Tabla creada con {record_count} registros iniciales")
                return
            
            self.logger.info(f"DELETE FROM {full_table_name} WHERE {date_column} = '{date_value}'")
            
            delete_query = f"""
            DELETE FROM {full_table_name}
            WHERE CAST({date_column} AS DATE) = DATE('{date_value}')
            """
            self.spark.sql(delete_query)
            self.logger.info("DELETE completado")
            
            record_count = df.count()
            self.logger.info(f"Insertando {record_count} registros en {full_table_name}")
            
            df.writeTo(full_table_name).append()
            self.logger.info(f"INSERT completado - {record_count} registros")
            
        except Exception as e:
            self.logger.error(f"Error en delete_insert_by_date: {e}")
            raise


# ============================================================================
# SQL QUERIES REPOSITORY
# ============================================================================

class SQLQueryRepository:
    """
    Repositorio centralizado de consultas SQL.
    
    Las tablas se reciben dinámicamente a través del JobConfig.
    Usar los métodos get_table_full_name() para obtener nombres de tablas.
    
    IMPORTANTE: 
    - Usar sintaxis Spark SQL
    - VARCHAR → STRING
    - Tablas Iceberg → glue_catalog.database.tabla
    """
    
    def __init__(self, config: JobConfig):
        """
        Inicializa el repositorio con la configuración del job.
        
        Args:
            config: JobConfig con las tablas fuente parseadas
        """
        self.config = config
    
    def _get_table(self, alias: str) -> str:
        """
        Obtiene el nombre completo de una tabla por su alias.
        
        Args:
            alias: Alias de la tabla (debe coincidir con el nombre en table_source_list)
        
        Returns:
            Nombre completo de la tabla (glue_catalog.database.tabla)
        
        Raises:
            ValueError: Si el alias no existe en la configuración
        """
        full_name = self.config.get_table_full_name(alias)
        if not full_name:
            available = [t.alias for t in self.config.table_sources]
            raise ValueError(
                f"Tabla con alias '{alias}' no encontrada. "
                f"Tablas disponibles: {available}"
            )
        return full_name
    
    # =========================================================================
    # EJEMPLO DE QUERIES USANDO TABLAS DINÁMICAS
    # =========================================================================
    
    def get_ejemplo_dinamico_query(self) -> str:
        """
        Ejemplo de query usando tablas del parámetro table_source_list.
        
        Uso:
            --table_source_list "stg_catalogo.homologacion,stg_catalogo.calendario"
            
            En el query usar:
            self._get_table('homologacion') → glue_catalog.stg_catalogo.homologacion
            self._get_table('calendario') → glue_catalog.stg_catalogo.calendario
        """
        # Obtener nombres de tablas dinámicamente
        # tbl_homolog = self._get_table('homologacion')
        # tbl_calendario = self._get_table('calendario')
        
        return """
        -- Query de ejemplo
        -- Reemplazar con las tablas dinámicas usando self._get_table('alias')
        SELECT 1 AS dummy
        """
    
    def get_query_con_tablas(self, fecha_corte_iso: str) -> str:
        """
        Ejemplo completo de query con tablas dinámicas y fecha de corte.
        
        Args:
            fecha_corte_iso: Fecha de corte en formato YYYY-MM-DD
        """
        # Ejemplo: si table_source_list = "stg_catalogo.clientes,stg_catalogo.productos"
        # tbl_clientes = self._get_table('clientes')
        # tbl_productos = self._get_table('productos')
        
        return f"""
        SELECT 
            1 AS id,
            'ejemplo' AS descripcion,
            DATE('{fecha_corte_iso}') AS fecha_corte
        """


# ============================================================================
# VIEW CREATOR - Creación de Vistas
# ============================================================================

class ViewCreator:
    """Crea vistas temporales a partir de los queries del repositorio."""
    
    def __init__(
        self, 
        spark_service: ISparkService, 
        logger: logging.Logger,
        config: JobConfig,
        time_config: TimeConfig = None
    ):
        self.spark = spark_service
        self.logger = logger
        self.config = config
        self.time_config = time_config
        self.sql_repo = SQLQueryRepository(config)
    
    def _create_view(self, query: str, view_name: str) -> None:
        """Crea una vista temporal."""
        self.logger.info(f"Creando vista: {view_name}")
        try:
            df = self.spark.sql(query)
            self.spark.create_temp_view(df, view_name)
        except Exception as e:
            self.logger.error(f"Error creando vista {view_name}: {e}")
            raise
    
    def create_all_views(self) -> None:
        """Ejecuta la creación de todas las vistas en orden."""
        self.logger.info("Iniciando creación de vistas...")
        self.logger.info(f"Tablas fuente disponibles: {[t.alias for t in self.config.table_sources]}")
        
        # Implementar según necesidad usando self.sql_repo
        # Ejemplo:
        # self._create_view(
        #     self.sql_repo.get_mi_query(),
        #     "vw_mi_vista"
        # )
        
        self.logger.info("Vistas creadas exitosamente")


# ============================================================================
# DATA PROCESSOR - Procesamiento de Datos
# ============================================================================

class DataProcessor:
    """Procesa los datos después de crear las vistas."""
    
    def __init__(
        self, 
        spark_service: ISparkService, 
        logger: logging.Logger,
        config: JobConfig,
        time_config: TimeConfig
    ):
        self.spark = spark_service
        self.logger = logger
        self.config = config
        self.time_config = time_config
        self.sql_repo = SQLQueryRepository(config)
    
    def get_final_result(self) -> DataFrame:
        """Obtiene el resultado final."""
        self.logger.info("Generando resultado final...")
        query = self.sql_repo.get_query_con_tablas(self.time_config.fecha_corte_iso)
        return self.spark.sql(query)
    
    def add_audit_columns(self, df: DataFrame) -> DataFrame:
        """Agrega columnas de auditoría al DataFrame."""
        fecha_proceso_str = self.time_config.date_time.strftime("%Y-%m-%d %H:%M:%S")
        fecha_corte_iso = self.time_config.fecha_corte_iso
        
        df_audit = (
            df
            .withColumn("fecha_proceso", to_timestamp(lit(fecha_proceso_str), "yyyy-MM-dd HH:mm:ss"))
            .withColumn("fecha_corte", to_date(lit(fecha_corte_iso), "yyyy-MM-dd"))
        )
        
        return (
            df_audit
            .withColumn("anio_particion", year(col("fecha_corte")).cast("int"))
            .withColumn("mes_particion", month(col("fecha_corte")).cast("int"))
            .withColumn("dia_particion", dayofmonth(col("fecha_corte")).cast("int"))
        )


# ============================================================================
# JOB ORCHESTRATOR - Orquestación Principal
# ============================================================================

class JobOrchestrator:
    """Orquesta la ejecución completa del job."""
    
    def __init__(
        self,
        config: JobConfig,
        time_config: TimeConfig,
        spark_service: ISparkService,
        s3_service: IS3Service,
        logger: logging.Logger
    ):
        self.config = config
        self.time_config = time_config
        self.spark = spark_service
        self.s3 = s3_service
        self.logger = logger
        
        self.view_creator = ViewCreator(spark_service, logger, config, time_config)
        self.data_processor = DataProcessor(spark_service, logger, config, time_config)
    
    def run(self) -> None:
        """Ejecuta el pipeline completo del job."""
        try:
            self.logger.info("=" * 60)
            self.logger.info("INICIANDO JOB")
            self.logger.info(f"Ambiente: {self.config.env}")
            self.logger.info(f"Fecha corte: {self.time_config.get_fecha_display()}")
            self.logger.info(f"Tabla destino: {self.config.db_output}.{self.config.output_table_name}")
            self.logger.info("=" * 60)
            
            # Log de tablas fuente
            self.logger.info("TABLAS FUENTE:")
            for table in self.config.table_sources:
                self.logger.info(f"  - {table.alias}: {table.full_name}")
            
            # Paso 1: Crear vistas
            self.logger.info("PASO 1: Creando vistas temporales...")
            self.view_creator.create_all_views()
            
            # Paso 2: Procesar datos
            self.logger.info("PASO 2: Procesando datos...")
            df_result = self.data_processor.get_final_result()
            df_result = self.data_processor.add_audit_columns(df_result)
            
            # Paso 3: Escribir resultados
            self.logger.info("PASO 3: Escribiendo resultados...")
            self._write_with_delete_insert(df_result)
            
            # Métricas finales
            elapsed_time = time.time() - self.time_config.start_time
            self.logger.info("=" * 60)
            self.logger.info("JOB COMPLETADO EXITOSAMENTE")
            self.logger.info(f"Tiempo de ejecución: {elapsed_time:.2f} segundos")
            self.logger.info("=" * 60)
            
        except Exception as e:
            self.logger.error(f"ERROR EN JOB: {e}")
            self.logger.error(LoggerSetup.create_log_msg(str(e)))
            raise
    
    def _write_with_delete_insert(self, df: DataFrame) -> None:
        """Escribe los datos usando DELETE + INSERT."""
        output_location = (
            f"s3://{self.config.bucket_target}/"
            f"{self.config.folder_target}/"
            f"{self.config.output_table_name}"
        )
        
        partition_cols = ["anio_particion", "mes_particion", "dia_particion"]
        
        self.s3.delete_insert_by_date(
            df=df,
            table=self.config.output_table_name,
            database=self.config.db_output,
            date_column="fecha_corte",
            date_value=self.time_config.fecha_corte_iso,
            location=output_location,
            partition_cols=partition_cols
        )


# ============================================================================
# FUNCIONES DE INICIALIZACIÓN
# ============================================================================

def get_job_parameters() -> Dict[str, str]:
    """Obtiene los parámetros del job de Glue."""
    parameter_list = [
        'JOB_NAME',
        'db_name',
        'db_output',
        'bucket_target',
        'folder_target',
        'env',
        'fecha_corte',
        'table_source_list'  # NUEVO: Lista de tablas fuente
    ]
    return getResolvedOptions(argv, parameter_list)


def create_spark_session(params: Dict[str, str]) -> SparkSession:
    """Crea y configura la sesión de Spark para Iceberg."""
    spark = SparkSession.builder \
        .appName("Glue Job - Spark Session") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.glue_catalog.warehouse", f"s3://{params['bucket_target']}/") \
        .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
        .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
        .config("spark.sql.defaultCatalog", "glue_catalog") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .config("spark.sql.execution.arrow.enabled", "true") \
        .enableHiveSupport() \
        .getOrCreate()
    
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
    
    return spark


# ============================================================================
# MAIN - Punto de Entrada
# ============================================================================

def main():
    """Función principal del job."""
    logger = LoggerSetup.setup_logging(logging.INFO)
    logger.info("Iniciando configuración del job...")
    
    try:
        # Obtener parámetros
        params = get_job_parameters()
        logger.info(f"Job: {params['JOB_NAME']}")
        logger.info(f"Parámetro fecha_corte: {params.get('fecha_corte', 'NO DEFINIDO')}")
        logger.info(f"Parámetro table_source_list: {params.get('table_source_list', 'NO DEFINIDO')}")
        
        # Crear configuración de tiempo
        time_config = TimeConfig.create(params.get('fecha_corte'))
        logger.info(f"Fecha de corte a usar: {time_config.get_fecha_display()}")
        
        # Parsear lista de tablas fuente
        table_sources = JobConfig.parse_table_source_list(
            params.get('table_source_list', '')
        )
        logger.info(f"Tablas fuente parseadas: {len(table_sources)}")
        for ts in table_sources:
            logger.info(f"  - {ts.alias}: {ts.full_name}")
        
        # Crear configuración del job
        job_config = JobConfig(
            db_name=params['db_name'],
            db_output=params['db_output'],
            bucket_target=params['bucket_target'],
            folder_target=params['folder_target'],
            env=params['env'],
            table_sources=table_sources
        )
        
        # Crear sesión de Spark
        spark = create_spark_session(params)
        spark_service = SparkService(spark, logger)
        s3_service = S3Service(spark, logger)
        
        # Ejecutar job
        orchestrator = JobOrchestrator(
            config=job_config,
            time_config=time_config,
            spark_service=spark_service,
            s3_service=s3_service,
            logger=logger
        )
        orchestrator.run()
        
    except Exception as e:
        logger.error(f"Error fatal: {e}")
        raise


if __name__ == "__main__":
    main()