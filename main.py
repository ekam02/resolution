from config import log_console_level, log_file_level, output_dir
from config.logger_config import setup_logger
from models import INSERT_BILLER_RESOLUTION
from utils.finder import get_max_resolution_id
from utils.uploader import upload_resolutions


logger = setup_logger(__name__, console_level=log_console_level, file_level=log_file_level)


def main():
    try:
        resolutions = upload_resolutions()
        logger.info(f"Se cargaron {len(resolutions)} resoluciones.")
        max_id = get_max_resolution_id() + 1
        logger.info(f"Se ha recuperador el ultimo id de la tabla `factura.resoluciones`: {max_id -1}.")
        for resolution in resolutions:
            resolution.id = max_id
            max_id += 1
        logger.info("Se han calculado y asignado los identificadores a las nuevas resoluciones.")
        resolutions = [resolution.values for resolution in resolutions]
        logger.info("Se han calculado los valores para la inserción de las nuevas resoluciones.")
        output_text = INSERT_BILLER_RESOLUTION.format(",\n".join(resolutions))
        logger.info("Se han agregado los valores sobre la inserción de las nuevas resoluciones.")
        with open(output_dir / "resolution.sql", "+w", encoding="utf-8") as file:
            file.write(output_text)
        logger.info("Se han generado un fichero SQL con la transacción para la inserción de las nuevas resoluciones.")
    except Exception as e:
        logger.exception(f"An unexpected error occurred: {e}")


if __name__ == "__main__":
    main()
