"""
CLI principal del agente de migración Athena a PySpark.

Este módulo actúa como punto de entrada para el agente. Se encarga de parsear
los argumentos de línea de comandos, configurar el logging y orquestar
la ejecución del grafo del agente.
"""

import argparse
import logging
import sys
import traceback

from pathlib import Path
from dotenv import load_dotenv

from .graph import create_agent_graph
from .models.agent_state import AgentState, create_initial_state
from .utils.logging_config import setup_logger

logger: logging.Logger = setup_logger(__name__)


def main(sql_path: Path, business_name: str, output_dir: str) -> None:
    """
    Función principal que ejecuta el flujo de migración.

    Inicializa el estado del agente y ejecuta el grafo de LangGraph.

    Args:
        sql_path (Path): Ruta al archivo SQL de origen.
        business_name (str): Nombre del negocio o contexto para el script generado.
        output_dir (str): Ruta al directorio de salida para los archivos generados.
    
    Raises:
        FileNotFoundError: Si el archivo SQL no existe.
        Exception: Si ocurre un error inesperado durante la ejecución.
    """
    logger.info("Iniciando Agente de Migración Athena2Glue")
    logger.info(f"SQL Input: {sql_path}")
    logger.info(f"Negocio: {business_name}")
    logger.info(f"Output: {output_dir}")

    if not sql_path.exists():
        logger.error(f"El archivo SQL no existe: {sql_path}")
        raise FileNotFoundError(f"Archivo no encontrado: {sql_path}")

    # Asegurar que el directorio de salida existe
    Path(output_dir).mkdir(parents=True, exist_ok=True)

    try:
        initial_state: AgentState = create_initial_state(
            sql_file_path=str(sql_path.absolute()),
            business_name=business_name,
            output_dir=output_dir
        )
        graph = create_agent_graph()
        
        # Ejecución del grafo
        final_state = graph.invoke(initial_state)
        
        logger.info("Agente ejecutado exitosamente.")
        
    except Exception as e:
        logger.error(f"Error durante la ejecución del agente: {e}")
        logger.debug(traceback.format_exc())
        raise


def cli() -> None:
    """
    Punto de entrada para la Línea de Comandos (CLI).
    
    Parsea argumentos y llama a la función main.
    """
    parser = argparse.ArgumentParser(
        description="Agente de migración de consultas Athena (SQL) a PySpark (AWS Glue)."
    )
    
    parser.add_argument(
        "sql_file",
        type=str,
        help="Ruta al archivo SQL de entrada que se desea migrar."
    )
    
    parser.add_argument(
        "--business-name",
        type=str,
        required=True,
        help="Nombre del negocio o identificador para nombrar el script generado (ej: FRN, VTA, etc)."
    )
    
    parser.add_argument(
        "--output-dir",
        type=str,
        default="./output",
        help="Directorio donde se guardará el script Python generado. Default: ./output"
    )
    
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Habilita logs detallados (DEBUG)."
    )

    args = parser.parse_args()

    # Configuración de nivel de log
    if args.verbose:
        logger.setLevel(logging.DEBUG)
        logging.getLogger().setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)

    try:
        main(Path(args.sql_file), args.business_name, args.output_dir)
    except Exception:
        sys.exit(1)


if __name__ == "__main__":
    load_dotenv()
    cli()
