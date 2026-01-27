import os
from collections.abc import Callable
from pyspark.sql import DataFrame
from awsglue.context import GlueContext

from addons.config.local_env import IS_LOCAL_ENV
from addons.config.job_config import JobConfig
from addons.pyspark_utils.table import S3File, Table
from addons.pyspark_utils.tables import get_source_options


class CatalogException(Exception):
    """Clase para manejar exceptionces de la clase Catalog"""


class Catalog:
    """Clase que obtiene las tablas
    """
    glue_context: GlueContext

    def __init__(self, glue_context: GlueContext, job_config: JobConfig, get_table_func: Callable[[str, JobConfig], Table | S3File] = get_source_options) -> None:
        self.glue_context = glue_context
        self.job_config = job_config
        self.get_table_func = get_table_func

    def get_table(self, table: str, ) -> DataFrame:
        try:
            source_meta = self.get_table_func(table, self.job_config)
            print(f"Se lee la data :{source_meta}")
            if isinstance(source_meta, S3File):
                s3_uri = source_meta.s3_uri
                if IS_LOCAL_ENV:
                    print("Ambiente Local")
                    current_path = os.getcwd()
                    s3_uri = s3_uri.replace("s3://", f"{current_path}/dev/catalog/")

                table_df = self.glue_context.spark_session.read.json([s3_uri])

            elif isinstance(source_meta, Table):
                dynamic_frame = self.get_dataframe_from_catalog(
                    database=source_meta.database,
                    table_name=source_meta.table_name
                )
                table_df: DataFrame = dynamic_frame.toDF()
            else:
                raise CatalogException("No se tiene otro tipo registrado de carga")
        except Exception as e:
            print('Error al procesar los archivos')
            raise e

        return table_df

    def get_dataframe_from_catalog(self, database, table_name):
        return self.glue_context.create_data_frame.from_catalog(database=database, table_name=table_name)

    def check_table_exists(self, database, table_name):
        return self.glue_context.spark_session.catalog.tableExists(table_name, database)
