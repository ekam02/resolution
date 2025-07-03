from config import repository
from schemas import Resolution
from utils.uploader import upload_resolutions
from utils.finder import get_max_resolution_id


class TestUploader:
    def test_upload_resolutions(self):
        """Comprueba que se cargan correctamente las resoluciones desde los archivos CSV o XLSX en el directorio de
        entrada.
        """
        resolutions = upload_resolutions()
        assert isinstance(resolutions, list)
        assert all([isinstance(resolution, Resolution) for resolution in resolutions])

    def test_dont_upload_resolutions(self):
        """Comprueba que no se cargan las resoluciones si no hay archivos CSV o XLSX en el directorio de entrada."""
        resolutions = upload_resolutions(repository / "logs")
        assert resolutions is None

class TestFinder:
    def test_get_max_resolution_id(self):
        """Comprueba que se pueda obtener el máximo id de la tabla `factura.resoluciones`."""
        max_id = get_max_resolution_id()
        assert isinstance(max_id, int)
