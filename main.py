from datetime import timedelta
from pathlib import Path

from config import log_console_level, log_file_level, output_dir, supply_file
from config.logger_config import setup_logger
from models import INSERT_BILLER_RESOLUTION, UPDATE_BILLER_RESOLUTION
from utils.finder import get_max_resolution_id
from utils.uploader import upload_resolutions


logger = setup_logger(__name__, console_level=log_console_level, file_level=log_file_level)


def main(_supply_file: Path = supply_file):
    try:
        resolutions = upload_resolutions(_supply_file)
        # Se cargaron resoluciones
        if resolutions:
            with open(output_dir / "resolution.sql", "+w", encoding="utf-8") as file:
                logger.info(f"Se cargaron {len(resolutions)} resoluciones.")
                max_id = get_max_resolution_id() + 1
                logger.info(f"Se ha recuperador el ultimo id de la tabla `factura.resoluciones`: {max_id -1}.")
                for resolution in resolutions:
                    resolution.id = max_id
                    max_id += 1
                logger.info("Se han calculado y asignado los identificadores a las nuevas resoluciones.")
                # Cada resolución puede tener su propia fecha de inicio
                logger.info("Inicia la escritura de instrucciones para actualizar las fechas de resoluciones anteriores.")
                for resolution in resolutions:
                    if resolution.previous_resolution_id:
                        file.write(UPDATE_BILLER_RESOLUTION.format(
                            resolution.start_date - timedelta(microseconds=1), resolution.previous_resolution_id
                        ))
                file.write("\n")
                logger.info("Finaliza la escritura de instrucciones para actualizar.")
                resolutions = [resolution.values for resolution in resolutions]
                logger.info("Se han calculado los valores para la inserción de las nuevas resoluciones.")
                output_text = INSERT_BILLER_RESOLUTION.format(",\n".join(resolutions))
                logger.info("Se han agregado los valores sobre la inserción de las nuevas resoluciones.")
                file.write(output_text)
                logger.info("Se han generado un fichero SQL con la transacción para la inserción de las nuevas resoluciones.")
        else:
            # Archivo inexistente o sin resoluciones
            logger.info("No se cargaron resoluciones.")
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")


if __name__ == "__main__":
    main()
