from sqlalchemy import text


# Usado para recuperar el número de la tienda asociado al prefijo.
QUERY_BILLER_STORE_BY_PREFIX = text(
    """SELECT
    tf.c_tipo_fac,
    tf.n_concepto_fact::INT8
FROM
    factura.tipos_fact tf
WHERE
    tf.c_abrev = :prefix"""
)


# Usado para recuperar el identificador más grande asignado en la tabla `factura.resoluciones`.
QUERY_BILLER_MAX_RESOLUTION_ID = text(
    """SELECT MAX(r.c_resolucion)
FROM factura.resoluciones AS r"""
)


QUERY_BILLER_PREVIOUS_RESOLUTION_ID = text(
    """SELECT r.c_resolucion
FROM factura.resoluciones AS r
WHERE
    r.c_prefijo = :c_prefijo
  AND r.f_vigencia_hasta > NOW()"""
)


QUERY_BILLER_ID_RETURNED_RESOLUTION_BY_STORE = text(
    """SELECT
    r.c_resolucion,
    tf.c_abrev
FROM
    factura.resoluciones AS r
    JOIN factura.tipos_fact AS tf ON r.c_prefijo = tf.c_tipo_fac
WHERE
    tf.n_concepto_fact = :store
  AND r.c_origen = '9'
  AND r.f_vigencia_hasta > NOW()
ORDER BY
    r.c_resolucion DESC"""
)


# Usado para construir la transacción de inserción sobre la tabla `factura.resoluciones`.
INSERT_BILLER_RESOLUTION = """INSERT INTO factura.resoluciones
(c_resolucion, c_empresa, c_origen, c_prefijo, n_resolucion, n_numero_inicial, n_numero_final, f_resolucion, f_vigencia_desde, f_vigencia_hasta, d_resolucion,llave_tecnica)
VALUES
{};"""


UPDATE_BILLER_RESOLUTION = """UPDATE factura.resoluciones SET f_vigencia_hasta = '{}' WHERE c_resolucion = {};
"""


UPDATE_BILLER_RETURNED_RESOLUTION = """UPDATE factura.resoluciones SET n_resolucion = {}, f_resolucion = '{}', f_vigencia_desde = '{}', f_vigencia_hasta = '{}', d_resolucion = '{}' WHERE c_resolucion = {};
"""
