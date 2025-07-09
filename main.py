from datetime import timedelta
from pathlib import Path

from config import log_console_level, log_file_level, output_file, supply_file, output_dir
from config.logger_config import setup_logger
from models import INSERT_BILLER_RESOLUTION, UPDATE_BILLER_RESOLUTION, UPDATE_BILLER_RETURNED_RESOLUTION
from utils.finder import get_max_resolution_id, find_id_returned_resolution_by_store, get_resolution_message
from utils.uploader import upload_resolutions


logger = setup_logger(__name__, console_level=log_console_level, file_level=log_file_level)


def main(_supply_file: Path = supply_file):
    try:
        resolutions = upload_resolutions(_supply_file)
        # Se cargaron resoluciones
        if resolutions:
            with open(output_file, "+w", encoding="utf-8") as file:
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
                            resolution.start_date - timedelta(days=1), resolution.previous_resolution_id
                        ))
                file.write("\n")

                # Crea un conjunto con los números de las tiendas actuales
                logger.info("Inicia la escritura de instrucciones para actualizar las resoluciones de devoluciones en tiendas.")
                store_set = set([(r.store, r.resolution, r.start_date, r.end_date) for r in resolutions if resolution.doc_type == 5])
                store_list = list(store_set)
                del store_set
                for s in store_list:
                    resolution_id = find_id_returned_resolution_by_store(s[0])
                    if resolution_id:
                        description = get_resolution_message(
                            resolution = s[1],
                            start_date = s[2],
                            end_date = s[3],
                            prefix = resolution_id[1],
                            start_consecutive = 2000001,
                            end_consecutive = 9999999
                        )
                        file.write(
                            UPDATE_BILLER_RETURNED_RESOLUTION.format(
                                s[1], s[2], s[2], s[3], description, resolution_id[0]
                            )
                        )
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
