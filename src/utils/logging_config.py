import logging
import sys

def setup_logger(name: str = "Athena2Glue", level: int = logging.INFO) -> logging.Logger:
    """
    Configura y devuelve un logger con el nombre y nivel especificados.

    Args:
        name (str): El nombre del logger.
        level (int): El nivel de logging (por defecto: logging.INFO).

    Returns:
        logging.Logger: El logger configurado.
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)

    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(level)
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.propagate = False

    return logger
