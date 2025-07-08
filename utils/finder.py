from typing import List, Optional

from sqlalchemy import Engine
from sqlalchemy.orm import Session

from config import biller_engine, log_console_level, log_file_level
from config.logger_config import setup_logger
from models import QUERY_BILLER_MAX_RESOLUTION_ID


logger = setup_logger(__name__, console_level=log_console_level, file_level=log_file_level)


def get_max_resolution_id(engine: Engine = biller_engine) -> Optional[int]:
    """Recupera el identificador máximo de la tabla `factura.resoluciones` en la base de datos del Facturador."""
    try:
        with Session(engine) as session:
            stmt = session.execute(QUERY_BILLER_MAX_RESOLUTION_ID).first()
            return stmt[0] if stmt else None
    except Exception as e:
        logger.error(f"An unexpected error occurred while loading the data: {e}")
    return None
