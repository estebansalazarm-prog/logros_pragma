"""Esta clase tiene las transformaciones del job"""
from pyspark.sql import DataFrame, SparkSession
from addons.config.job_config import JobConfig
from col_pragma_logro_pgm_extraer_tabla_dynamodb.etl.transform.raw_transformations import get_table_transformations
from col_pragma_logro_pgm_extraer_tabla_dynamodb.etl.extract.sources import RawSources

class Transformations:
    """Esta clase tiene las transformaciones del job"""
    spark: SparkSession
    sources: RawSources
    job_config: JobConfig
    main_table_df: DataFrame

    def __init__(self, spark: SparkSession, sources: RawSources, job_config: JobConfig) -> None:
        self.sources = sources
        self.spark = spark
        self.job_config = job_config
        
    def get_main_table(self):
        # Obtener el nombre de la tabla desde el YAML (dinámico)
        if self.job_config.elt_config_table is None:
            raise ValueError("No se ha cargado configuración ETL desde YAML. Llamar load_etl_config_from_yaml() primero.")
        
        table_name = self.job_config.elt_config_table.processing_config.schema_name
        print(f"Usando tabla desde YAML: {table_name}")
        
        # Obtener el DataFrame dinámico usando getattr
        df_dinamic = getattr(self.sources, table_name, None)
        if df_dinamic is None:
            raise AttributeError(f"La tabla '{table_name}' no se encontró en sources. Tablas disponibles: {[attr for attr in dir(self.sources) if not attr.startswith('_')]}")
        
        # Pasar el DataFrame dinámico a las transformaciones
        self.main_table_df = get_table_transformations(
            df_dinamic=df_dinamic,
            job_config=self.job_config
        )
        return self.main_table_df
