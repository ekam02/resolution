from pathlib import Path
from typing import List, Optional

from pandas import read_csv, read_excel, concat

from config import input_dir, log_console_level, log_file_level
from config.logger_config import setup_logger
from schemas import Resolution


logger = setup_logger(__name__, console_level=log_console_level, file_level=log_file_level)


def upload_resolutions(supply_file: Path) -> Optional[List[Resolution]]:
    """Carga las resoluciones desde los archivos CSV o XLSX en el directorio de entrada."""
    try:
        if not isinstance(supply_file, Path):
            raise TypeError("The 'supply_file' argument must be a Path object.")
        if not supply_file.exists():
            raise FileNotFoundError(f"No file found at {supply_file}")
        if supply_file.suffix == ".csv":
            resolutions_df = read_csv(supply_file, dtype=str)
        else:
            resolutions_df = read_excel(supply_file, dtype=str)
        return [Resolution(**row.to_dict()) for _, row in resolutions_df.iterrows()]
    except TypeError:
        logger.exception("The 'supply_file' argument must be a Path object.")
    except FileNotFoundError:
        logger.exception("No file found at the specified path.")
    except Exception as e:
        logger.error(f"An unexpected error occurred while loading the data: {e}")
    finally:
        del resolutions_df
    return None
