from dataclasses import dataclass
from typing import Optional

_DOMAIN: str = "logro"
_SUBDOMAIN: str = "pgm"
_PRODUCT_TYPE: str = "pragma"
_DATA_PRODUCT: str = "tabla_final"
_CAPACIDAD: str = "pragma"
_PAIS: str = "col"
_CATALOG_NAME: str = "table-catalog"
_INSERT_MODE: str = "append"
_PROCESS_TYPE: str = "INC"
_PRIMARY_KEY: str = "id"
_PRECOMBINE_KEY: str = "tstamp"

# pylint: disable = R0902

from typing import TypedDict

ConstantsDict = TypedDict(
    'ConstantsDict', {
        "domain": str,
        "subdomain": str,
        "product_type": str,
        "output_product": str,
        "data_product": str,
        "capacidad": str,
        "pais": str,
        "pais_proceso": str,
        "catalog_name": str,
        "insert_mode": str,
    },
    total=False
)

@dataclass
class Constants:
    """Clase que gestiona las constantes"""

    domain: str = _DOMAIN
    subdomain: str = _SUBDOMAIN
    product_type: str = _PRODUCT_TYPE
    data_product: str = _DATA_PRODUCT
    capacidad: str = _CAPACIDAD
    pais: str = _PAIS
    catalog_name: str = _CATALOG_NAME
    insert_mode: str = _INSERT_MODE
    process_type: str = _PROCESS_TYPE
    primary_key: str = _PRIMARY_KEY
    precombine_key: str = _PRECOMBINE_KEY

    @classmethod
    def from_dict(cls, config_dict: Optional[ConstantsDict] = None):
        """Inicializa la clase desde un diccionario, usando valores por defecto si faltan claves."""
        config_dict = config_dict or {}
        return cls(**{**cls().to_dict(), **config_dict})

    def to_dict(self) -> ConstantsDict:
        """Devuelve las constantes en formato diccionario."""
        return self.__dict__


default_constants = Constants()
