import re
from datetime import date
from typing import Any

from dateutil.parser import parse
from pydantic import model_validator
from sqlmodel import SQLModel
from typing import Optional

from config import stores


class Resolution(SQLModel):
    resolution: int
    doc_type: int
    start_date: date
    end_date: date
    store: int
    prefix: str
    start_consecutive: int
    end_consecutive: int
    technical_key: Optional[str] = None

    @classmethod
    def is_resolution(cls, resolution: str) -> bool:
        return bool(re.match(r"[0-9]{14,}", resolution))

    @classmethod
    def is_prefix(cls, prefix: str) -> bool:
        return bool(re.match(r"[0-9A-Z]{4}", prefix))

    @classmethod
    def is_uuid(cls, uuid: str) -> bool:
        return bool(re.match(r"[0-9a-z]{64}", uuid))

    @model_validator(mode='before')
    @classmethod
    def cast_date(cls, data: Any) -> Any:
        translated_data = {
            "Resolución": "resolution",
            "Tipo": "doc_type",
            "Válida desde": "start_date",
            "Válida hasta": "end_date",
            "Tienda": "store",
            "Prefijo": "prefix",
            "Desde": "start_consecutive",
            "Hasta": "end_consecutive",
            "Clave técnica": "technical_key"
        }
        doc_type_mapping = {
            "Factura de contingencia": 13,
            "Factura de venta": 5
        }
        if not len(data) == 9:
            raise ValueError(f"Se esperan 9 columnas, pero se recibieron {len(data)}")
        data = {translated_data[key]: value for key, value in data.items() if key in translated_data}
        if not cls.is_resolution(data["resolution"]):
            raise ValueError(f"El valor '{data['resolution']}' no pudo ser mapeado")
        data["resolution"] = int(data["resolution"])
        if not data["doc_type"] in doc_type_mapping:
            raise ValueError(f"El valor '{data['doc_type']}' no pudo ser mapeado")
        data["doc_type"] = doc_type_mapping.get(data["doc_type"], 9)
        data["start_date"] = parse(data["start_date"]).date()
        data["end_date"] = parse(data["end_date"]).date()
        for key, values in stores.items():
            if data["store"] in values:
                data["store"] = key
                break
        if not cls.is_prefix(data["prefix"]):
            raise ValueError(f"El valor '{data['prefix']}' no pudo ser mapeado")
        if not re.match(r"[9]{7,}", data["end_consecutive"]):
            raise ValueError(f"El valor '{data['end_consecutive']}' no pudo ser mapeado")
        data["end_consecutive"] = int(data["end_consecutive"])
        if not int(data["start_consecutive"]) < data["end_consecutive"]:
            raise ValueError(f"La secuencia de consecutivos no puede empezar en un valor mayor al {data['end_consecutive']}")
        data["start_consecutive"] = int(data["start_consecutive"])
        if data["technical_key"] and not isinstance(data["technical_key"], float):
            if not cls.is_uuid(data["technical_key"]):
                raise ValueError(f"El valor '{data["technical_key"]}' no pudo ser mapeado")
        else:
            data["technical_key"] = None

        return data
