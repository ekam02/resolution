from datetime import datetime
from typing import Any, List, Optional

from sqlalchemy import Engine, TextClause
from sqlalchemy.orm import Session

from config import biller_engine, log_console_level, log_file_level
from config.logger_config import setup_logger
from models import QUERY_BILLER_MAX_RESOLUTION_ID, QUERY_BILLER_ID_RETURNED_RESOLUTION_BY_STORE


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


def get_months_validity(start_date: datetime, end_date: datetime) -> Optional[int]:
    """
    Calculate the number of months of validity between two dates.

    This function computes the approximate number of months between
    the given `start_date` and `end_date`. The number of months is
    calculated as the total number of days between the two dates
    divided by 30. If the inputs are not valid `datetime` instances
    or if `start_date` is later than `end_date`, the function will
    log appropriate exceptions and return `None`.

    :param start_date: The starting date of the period.
    :type start_date: datetime
    :param end_date: The ending date of the period.
    :type end_date: datetime
    :return: The approximate number of months between the two dates.
    :rtype: Optional[int]
    """
    try:
        if not isinstance(start_date, datetime) or not isinstance(end_date, datetime):
            raise TypeError("The 'start_date' and 'end_date' arguments must be instances of datetime.")
        if start_date > end_date:
            raise ValueError("The 'start_date' argument must be earlier than the 'end_date' argument.")
        return (end_date - start_date).days // 30
    except TypeError:
        logger.exception("The 'start_date' and 'end_date' arguments must be instances of datetime.")
    except ValueError:
        logger.exception("The 'start_date' argument must be earlier than the 'end_date' argument.")
    except Exception as e:
        logger.error(f"An unexpected error occurred while loading the data: {e}")
    return None


def get_resolution_message(
        resolution: int,
        start_date: datetime,
        end_date: datetime,
        prefix: str,
        start_consecutive: int,
        end_consecutive: int
) -> Optional[str]:
    try:
        if not isinstance(start_date, datetime) or not isinstance(end_date, datetime):
            raise TypeError("The 'start_date' and 'end_date' arguments must be instances of datetime.")
        if start_date > end_date:
            raise ValueError("The 'start_date' argument must be earlier than the 'end_date' argument.")
        if not isinstance(prefix, str):
            raise TypeError("The 'prefix' argument must be a string.")
        if not isinstance(start_consecutive, int) or not isinstance(end_consecutive, int):
            raise TypeError("The 'start_consecutive' and 'end_consecutive' arguments must be integers.")
        if start_consecutive > end_consecutive:
            raise ValueError("The 'start_consecutive' argument must be earlier than the 'end_consecutive' argument.")
        return (f"Resolución de Factura Electrónica Nro. {resolution}  "
                f"Fecha {start_date.strftime('%d/%m/%Y')}  "
                f"Prefijo {prefix}  "
                f"Rango {start_consecutive} al {end_consecutive} Vigencia {get_months_validity(start_date, end_date)} meses.")
    except TypeError:
        logger.exception("")
    except ValueError:
        logger.exception("")
    except Exception as e:
        logger.error(f"An unexpected error occurred while loading the data: {e}")
    return None


def find_id_returned_resolution_by_store(
        store: int,
        clause: TextClause = QUERY_BILLER_ID_RETURNED_RESOLUTION_BY_STORE,
        engine: Engine = biller_engine
) -> Optional[List[Any]]:
    try:
        with Session(engine) as session:
            stmt = session.execute(clause, params={"store": str(store)}).first()
            return (stmt[0], stmt[1]) if stmt else None
    except Exception as e:
        logger.error(f"An unexpected error occurred while loading the data: {e}")
    return None
