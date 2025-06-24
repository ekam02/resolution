import logging
import logging.handlers
from pathlib import Path


def setup_logger(
        name: str,
        log_file: str = "app.log",
        log_dir: str = "logs",
        console_level: int = logging.INFO,
        file_level: int = logging.WARNING
) -> logging.Logger:
    """
    Configura un logger con manejadores para consola y archivo.

    Args:
        name: Nombre del logger (generalmente __name__ del módulo).
        log_file: Nombre del archivo de log.
        log_dir: Directorio donde se guardarán los logs.
        console_level: Nivel de registro para la consola.
        file_level: Nivel de registro para el archivo.

    Returns:
        Un logger configurado.
    """
    # Crear directorio de logs si no existe
    log_dir_path = Path(__file__).parent.parent / log_dir
    try:
        log_dir_path.mkdir(exist_ok=True)
    except PermissionError as e:
        print(f"Error al crear directorio de logs: {e}")
        log_dir_path = Path.cwd()  # Fallback al directorio actual

    # Crear el logger
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)  # Nivel base del logger

    # Evitar duplicación de manejadores
    if not logger.handlers:
        # Formato del log
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - [%(module)s:%(lineno)d] - %(message)s',
            datefmt="%Y-%m-%d %H:%M:%S"
        )

        # Manejador para consola
        console_handler = logging.StreamHandler()
        console_handler.setLevel(console_level)
        console_handler.setFormatter(formatter)

        # Manejador para archivo con rotación
        log_path = log_dir_path / log_file
        file_handler = logging.handlers.RotatingFileHandler(
            log_path, maxBytes=10*1024*1024, backupCount=5, encoding="utf-8"
        )
        file_handler.setLevel(file_level)
        file_handler.setFormatter(formatter)

        # Añadir manejadores al logger
        logger.addHandler(console_handler)
        logger.addHandler(file_handler)

    return logger
