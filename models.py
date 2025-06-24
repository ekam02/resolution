from typing import Optional

from sqlmodel import SQLModel, Field


class BillType(SQLModel, table=True):
    __tablename__ = "tipos_fact"
    id: int = Field(alias="c_tipo_fac", primary_key=True)
    desc: str = Field(alias="d_tipo_fac")
    abbreviation: str = Field(alias="c_abrev")
    enabled: bool = Field(alias="s_activo", default=True)
    store: str = Field(alias="n_concepto_fact")
    invoice_seq: int = Field(alias="n_consecutivo", default=0)
    memo_seq: int = Field(alias="n_consecutivo_nc", default=0)
    pos: Optional[str] = Field(alias="nro_terminal", default=None)
