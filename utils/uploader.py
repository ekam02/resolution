from pathlib import Path
from typing import List, Optional

from pandas import read_csv, read_excel, concat

from config import input_dir, log_console_level, log_file_level
from config.logger_config import setup_logger
from schemas import Resolution


logger = setup_logger(__name__, console_level=log_console_level, file_level=log_file_level)


def upload_resolutions(supply: Path = input_dir) -> Optional[List[Resolution]]:
    """Carga las resoluciones desde los archivos CSV o XLSX en el directorio de entrada."""
    try:
        files = [file for file in supply.iterdir() if file.is_file()]
        files = [file for file in files if file.suffix in (".csv", ".xlsx")]
        resolutions_data = []
        if not files:
            raise FileNotFoundError("No CSV or XLSX files were found in the input folder.")
        for file in files:
            if file.suffix == ".csv":
                resolutions_data.append(read_csv(file, dtype=str))
            else:
                resolutions_data.append(read_excel(file, dtype=str))
        resolutions_df = concat(resolutions_data)
        return [Resolution(**row.to_dict()) for _, row in resolutions_df.iterrows()]
    except Exception as e:
        logger.exception(f"An unexpected error occurred while loading the data: {e}")
    finally:
        del files
        del resolutions_data
    return None
