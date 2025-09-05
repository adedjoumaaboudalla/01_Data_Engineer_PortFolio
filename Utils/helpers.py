
import logging


logging.basicConfig(
level=logging.INFO,
format="%(asctime)s - %(levelname)s - %(message)s")

def warning(msg: str):
    """
    Ecris le message passé en en warning 
    """
    logging.warning(msg)

def error(msg: str):
    """
    Ecris le message passé en en error 
    """
    logging.error(msg)

def info(msg: str):
    """
    Ecris le message passé en en info 
    """
    logging.info(msg)

def debug(msg: str):
    """
    Ecris le message passé en en debug 
    """
    logging.debug(msg)