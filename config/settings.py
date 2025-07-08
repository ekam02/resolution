import os
from pathlib import Path
import tomllib

from dotenv import load_dotenv
from sqlmodel import create_engine

from .logger_config import setup_logger


class AppSettings:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(AppSettings, cls).__new__(cls)
            cls._instance._load_settings()
        return cls._instance

    def _load_settings(self):
        load_dotenv()

        self.cfg_dir = Path(__file__).parent   # Config Directory
        self.repository = self.cfg_dir.parent  # Repository Directory
        self.cfg_file = self.cfg_dir / "config.toml"

        if not self.cfg_file.exists():
            raise FileNotFoundError(f"No config file found at {self.cfg_file}")

        with self.cfg_file.open("rb") as f:
            cfg = tomllib.load(f)

        self.log_console_level = cfg.get("log_console_level", 20)
        self.log_file_level = cfg.get("log_file_level", 30)
        engine_pool_size = cfg.get("engine_pool_size", 5)
        engine_max_overflow = cfg.get("engine_max_overflow", 10)
        self.output_file = cfg.get("output_file")
        self.stores = cfg.get("stores")
        self.data_translation = cfg.get("data_translation")
        self.document_type_translation = cfg.get("document_type_translation")

        self.output_dir = self.repository / "output"
        if not self.output_dir.exists():
            self.output_dir.mkdir(parents=True)

        self.input_dir = self.repository / "input"
        if not self.input_dir.exists():
            self.input_dir.mkdir(parents=True)

        self.supply_file = self.input_dir / cfg.get("supply_file")
        # El atributo 'supply_file' fue diligenciado en 'config.toml'
        if self.supply_file:
            # Se busca comprobar si el valor del atributo existe realmente en el directorio de entrada.
            if not self.supply_file.exists():
                raise FileNotFoundError(f"No file found at {self.supply_file}")

        logger = setup_logger(__name__, console_level=self.log_console_level, file_level=self.log_file_level)
        logger.debug("Loading config settings")

        biller_user = os.getenv("BILLER_USER")
        biller_pass = os.getenv("BILLER_PASS")
        biller_host = os.getenv("BILLER_HOST")
        biller_port = os.getenv("BILLER_PORT")
        biller_name = os.getenv("BILLER_NAME")
        biller_sche = os.getenv("BILLER_SCHE")

        if not all([biller_user, biller_pass, biller_host, biller_port, biller_name, biller_sche]):
            raise ValueError("Missing required environment variables")

        self.biller_engine = create_engine(
            f"postgresql://{biller_user}:{biller_pass}@{biller_host}:{biller_port}/{biller_name}",
            pool_size=engine_pool_size, max_overflow=engine_max_overflow,
            connect_args={"options": f"-csearch_path={biller_sche}"}
        )


settings = AppSettings()
