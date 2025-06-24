from config import repository


class TestConfig:
    # Validar que los directorios de entrada y salida existan
    def test_input_output_dirs(self):
        input_dir = repository / "input"
        assert input_dir.exists()
        output_dir = repository / "output"
        assert output_dir.exists()
