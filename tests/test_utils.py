from config import repository
from schemas import Resolution
from utils.uploader import upload_resolutions
from utils.finder import find_bill_type, find_bill_types


class TestUploader:
    # La información deberá estar en el directorio de entrada
    def test_upload_resolutions(self):
        resolutions = upload_resolutions()
        assert isinstance(resolutions, list)
        assert isinstance(resolutions[0], Resolution)

    def test_dont_upload_resolutions(self):
        resolutions = upload_resolutions(repository / "logs")
        assert resolutions is None

class TestFinder:
    def test_find_bill_type(self):
        bill_type = find_bill_type(prefix="", store="")
        pass

    def test_find_bill_types(self):
        bill_types = find_bill_types(resolutions=[])
        pass