"""
CLI principal del agente de migración Athena a PySpark.
"""

#import argparse
import logging
import sys

from pathlib import Path
from dotenv import load_dotenv

from .graph import create_agent_graph
from .models.agent_state import AgentState, create_initial_state


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger: logging.Logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

def main(sql_path: Path, business_name: str, output_dir: str) -> None:
    """
    Docstring for main
    
    :param sql_path: Description
    :type sql_path: str
    :param business_name: Description
    :type business_name: str
    """
    initial_state: AgentState = create_initial_state(
        sql_file_path=str(sql_path.absolute()),
        business_name=business_name,
        output_dir=output_dir
    )
    graph = create_agent_graph()
    final_state = graph.invoke(initial_state)
    logger.info("Agente ejecutado")

def cli() -> None:
    """
    Docstring for cli
    """
    main(Path("./documents/consulta_frn.sql"), "FRN", "./output")

if __name__ == "__main__":
    load_dotenv()
    cli()
