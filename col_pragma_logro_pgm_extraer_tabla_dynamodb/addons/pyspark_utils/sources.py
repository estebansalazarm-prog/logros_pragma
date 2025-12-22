from addons.pyspark_utils.catalog import Catalog
from addons.pyspark_utils.pre_extract_sources import PreExtractSources


class Sources:
    """
    _summary_
    """
    catalog: Catalog
    
    def __init__(self, catalog: Catalog, pre_load_sources: PreExtractSources):
        self.catalog = catalog

        # Si hay YAML configurado (elt_config_table), usar schema_name del YAML
        # De lo contrario, usar TABLE_SOURCES (modo legacy)
        if catalog.job_config.elt_config_table is not None:
            # Modo dinámico: usar schema_name del YAML como nombre del atributo
            table_name = catalog.job_config.elt_config_table.processing_config.schema_name
            _init_keys = [table_name]
            print(f"Usando configuración YAML - Tabla: {table_name}")
        else:
            # Modo legacy: usar TABLE_SOURCES
            _init_keys = list(catalog.job_config.table_sources.keys())
            print(f"Usando TABLE_SOURCES (modo legacy) - Tablas: {_init_keys}")

        for init_key in _init_keys:
            table_df = catalog.get_table(init_key)

            """Transformacion de fuente si es necesario"""
            if init_key in dir(pre_load_sources):
                print(f"Leyendo particion de {init_key}")
                table_df = pre_load_sources.__getattribute__(init_key)(table_df)

            self.__setattr__(init_key, table_df)
