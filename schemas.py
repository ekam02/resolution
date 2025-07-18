import re
from datetime import date, datetime
from hashlib import sha256
from typing import Any, Optional

from dateutil.parser import parse
from pydantic import model_validator
from sqlalchemy.orm import Session
from sqlmodel import SQLModel

from config import biller_engine, data_translation, document_type_translation
from models import QUERY_BILLER_STORE_BY_PREFIX, QUERY_BILLER_PREVIOUS_RESOLUTION_ID
from utils.finder import get_resolution_message


class StoreError(Exception):
    pass


class Resolution(SQLModel):
    id: Optional[int] = None
    resolution: int
    doc_type: int
    start_date: datetime
    end_date: datetime
    store: int
    prefix: str
    start_consecutive: int
    end_consecutive: int
    technical_key: Optional[str] = None
    bill_type_id: Optional[int] = None  # ForeignKey `tipos_fact`
    previous_resolution_id: Optional[int] = None

    @classmethod
    def is_resolution(cls, resolution: str) -> bool:
        return bool(re.match(r"^[0-9]{11,}$", resolution))

    @classmethod
    def is_prefix(cls, prefix: str) -> bool:
        return bool(re.match(r"^[0-9A-Z]{4}$", prefix))

    @classmethod
    def is_uuid(cls, uuid: str) -> bool:
        return bool(re.match(r"^[0-9a-z]{10,64}$", uuid))

    @model_validator(mode='before')
    @classmethod
    def cast_data(cls, data: Any) -> Any:
        aux = {}
        for key, value in data.items():
            if key in data_translation:
                aux[data_translation[key]] = value
            else:
                aux[key] = value

        data = aux.copy()
        del aux

        # Evalúa atributo `resolution`
        if not data.get("resolution"):
            raise KeyError(
                f"No fue posible encontrar le atributo 'resolution' entre los datos recibidos: {data.keys()}"
            )
        if not type(data["resolution"]) in (int, str):
            raise TypeError(
                f"Se espera que 'resolution' sea de tipo 'str' o 'int', pero se recibió el tipo "
                f"'{type(data["resolution"]).__name__}'."
            )
        if isinstance(data["resolution"], str):
            if not cls.is_resolution(data["resolution"]):
                raise ValueError(f"El valor '{data['resolution']}' no pudo ser mapeado.")
            data["resolution"] = int(data["resolution"])

        # Evalúa atributo `doc_type`
        if not data.get("doc_type"):
            raise KeyError(
                f"No fue posible encontrar le atributo 'doc_type' entre los datos recibidos: {data.keys()}"
            )
        if not type(data["doc_type"]) in (int, str):
            raise TypeError(
                f"Se espera que 'doc_type' sea de tipo 'str' o 'int', pero se recibió el tipo "
                f"'{type(data["resolution"]).__name__}'."
            )
        if isinstance(data["doc_type"], str):
            if not data["doc_type"] in document_type_translation:
                raise ValueError(
                    f"Se espera que 'doc_type' tenga uno de los valores {document_type_translation.keys()}, pero se "
                    f"recibió el valor '{data['doc_type']}'."
                )
            data["doc_type"] = document_type_translation[data["doc_type"]]
        else:
            if not data["doc_type"] in (4, 5, 8, 9, 13, 15):
                raise ValueError(
                    f"Se espera que 'doc_type' tenga uno de los números 4, 5, 8, 9, 13 o 15, pero se recibió el "
                    f"número {data['doc_type']}."
                )

        # Evalúa atributo `end_date`
        if not data.get("end_date"):
            raise KeyError(
                f"No fue posible encontrar le atributo 'end_date' entre los datos recibidos: {data.keys()}"
            )
        if not type(data.get("end_date")) in (date, datetime, str):
            raise TypeError(
                f"Se espera que 'end_date' sea de tipo 'date', 'datetime' o 'str', pero se recibió el tipo "
                f"'{type(data["end_date"]).__name__}'."
            )
        if isinstance(data["end_date"], str):
            data["end_date"] = parse(data["end_date"])

        # Evalúa atributo `start_date`
        if not data.get("start_date"):
            raise KeyError(
                f"No fue posible encontrar le atributo 'start_date' entre los datos recibidos: {data.keys()}"
            )
        if not type(data.get("start_date")) in (date, datetime, str):
            raise TypeError(
                f"Se espera que 'start_date' sea de tipo 'date', 'datetime' o 'str', pero se recibió el tipo "
                f"'{type(data["start_date"]).__name__}'."
            )
        if isinstance(data["start_date"], str):
            data["start_date"] = parse(data["start_date"])
        elif isinstance(data["start_date"], datetime):
            data["start_date"] = data["start_date"]
        if not data["start_date"] < data["end_date"]:
            raise ValueError(
                f"La fecha de inicio '{data['start_date']}' debe ser más antigua que la fecha final "
                f"'{data['end_date']}'."
            )

        # Evalúa atributo `prefix`
        if not data.get("prefix"):
            raise KeyError(
                f"No fue posible encontrar le atributo 'prefix' entre los datos recibidos: {data.keys()}"
            )
        if not isinstance(data["prefix"], str):
            raise TypeError(
                f"Se espera que 'prefix' sea de tipo 'str', pero se recibió el tipo '{type(data["prefix"]).__name__}'."
            )
        if not cls.is_prefix(data["prefix"]):
            raise ValueError(
                f"Se espera que 'prefix' tenga como patrón '[0-9A-Z]" "{4}', " f"pero se recibió el valor "
                f"'{data['prefix']}'."
            )

        # Evalúa atributo `store`
        if not type(data.get("store")) in (int, str):
            raise TypeError(
                f"Se espera que 'store' sea de tipo 'int' o 'str', pero se recibió el tipo "
                f"'{type(data["store"]).__name__}'."
            )
        if isinstance(data["store"], str):
            with Session(biller_engine) as session:
                stmt = session.execute(QUERY_BILLER_STORE_BY_PREFIX, {"prefix": data["prefix"]}).first()
                if not stmt:
                    raise StoreError(
                        f"No fue posible encontrar la tienda con el prefijo '{data['prefix']}' en la base de datos."
                    )
                data["store"] = stmt[1] if stmt else None
                data["bill_type_id"] = stmt[0] if stmt else None
                del stmt
                stmt = session.execute(QUERY_BILLER_PREVIOUS_RESOLUTION_ID, {"c_prefijo": data["bill_type_id"]}).first()
                if stmt:
                    data["previous_resolution_id"] = stmt[0]
                del stmt

        # Evalúa atributo `end_consecutive`
        if not type(data.get("end_consecutive")) in (int, str):
            raise TypeError(
                f"Se espera que 'end_consecutive' sea de tipo 'int' o 'str', pero se recibió el tipo "
                f"'{type(data["end_consecutive"]).__name__}'."
            )
        if isinstance(data["end_consecutive"], str):
            data["end_consecutive"] = data["end_consecutive"].strip()
            if not data["end_consecutive"].isdigit():
                raise ValueError(
                    f"Se espera que 'end_consecutive' tenga como patrón '^[0-9]+$ pero se recibió el valor "
                    f"'{data['end_consecutive']}'."
                )
            data["end_consecutive"] = int(data["end_consecutive"])
        if not data["end_consecutive"] > 0:
            raise ValueError(f"El valor '{data['end_consecutive']}' debe ser mayor a '0'.")

        # Evaluar atributo `start_consecutive`
        if not type(data.get("start_consecutive")) in (int, str):
            raise TypeError(
                f"Se espera que 'start_consecutive' sea de tipo 'int' o 'str', pero se recibió el tipo "
                f"'{type(data["start_consecutive"]).__name__}'."
            )
        if isinstance(data["start_consecutive"], str):
            data["start_consecutive"] = data["start_consecutive"].strip()
            if not data["start_consecutive"].isdigit():
                raise ValueError(
                    f"Se espera que 'start_consecutive' tenga como patrón '^[0-9]+$' pero se recibió el valor "
                    f"'{data['start_consecutive']}'."
                )
            data["start_consecutive"] = int(data["start_consecutive"])
        if not 0 < data["start_consecutive"] < data['end_consecutive']:
            raise ValueError(
                f"El valor '{data['start_consecutive']}' debe ser mayor a '0' y, menor a "
                f"'{data['end_consecutive']}'."
            )

        # Evalúa atributo `technical_key`
        if not data["technical_key"] or isinstance(data["technical_key"], float):
            data["technical_key"] = sha256(data["prefix"].encode()).hexdigest()
        else:
            if not isinstance(data.get("technical_key"), str):
                raise TypeError(
                    f"Se espera que 'technical_key' sea de tipo 'str', pero se recibió el tipo "
                    f"'{type(data["technical_key"]).__name__}'."
                )
            if not cls.is_uuid(data["technical_key"]):
                raise ValueError(
                    "Se espera que 'technical_key' tenga como patrón '^[0-9a-z]{64}$' pero se recibió el valor "
                    f"'{data['technical_key']}'."
                )

        return data

    @property
    def values(self) -> str:
        message = get_resolution_message(
            resolution=self.resolution,
            start_date=self.start_date,
            end_date=self.end_date,
            prefix=self.prefix,
            start_consecutive=self.start_consecutive,
            end_consecutive=self.end_consecutive
        )
        return (f"("
                f"{self.id}, "
                f"1, "
                f"{self.doc_type}, "
                f"{self.bill_type_id}, "
                f"{self.resolution}, "
                f"{self.start_consecutive}, "
                f"{self.end_consecutive}, "
                f"'{self.start_date}', "
                f"'{self.start_date}', "
                f"'{self.end_date}', "
                f"'{message}', "
                f"'{self.technical_key}'"
                f")")
