"""
save_action.py carga el archivo
"""
import os

from pyspark.sql import DataFrame

from addons.config.local_env import IS_LOCAL_ENV
from addons.config.job_config import JobConfig
from addons.pyspark_utils.hudi_options import HudiOptions


def save(
    spark_df: DataFrame = None,
    hudi_options: HudiOptions = None,
    path: str = None,
    write_format: str = "parquet",
    mode: str = "overwrite",
):
    """Funcion para guardar datos en formato parquet en el catalogo de glue

    Args:
        spark_df (Dataframe): Dataframe que se va guardar
        hudi_options (dict): Diccionario con las opciones de Hudi
        destination_catalog_database_name (str): Base de datos del catálogo de glue
        destination_catalog_table_name (str): Tabla del catálogo de glue
        path (str): Ruta en S3 donde quedan los datos
        write_format (str): Formato para guardar los datos (hudi)
        mode (str): Modo de guardado de los datos (append, insert, overwrite...)
    """
    try:
        if IS_LOCAL_ENV:  # pragma: no cover
            current_path = os.getcwd()
            table = hudi_options['hoodie.table.name']
            database = hudi_options['hoodie.datasource.hive_sync.database']
            spark_df.sparkSession.sql(
                f'create database if not exists {database};').collect()

            print('saving', f'{database}.{table}')

            (
                spark_df
                .write
                .format('parquet')
                .mode('overwrite')
                .option('path', f"{current_path}/dev/catalog/{database}/{table}")
                .saveAsTable(f'{database}.{table}')
            )

        else:
            print(hudi_options)
            print('path', path)
            spark_df.write.format(write_format).mode(mode).options(**hudi_options).save(path)

    except Exception as write_error:
        raise RuntimeError from write_error


def save_action(
    spark_df: DataFrame = None,
    hudi_options: dict = None,
    path: str = None,
    mode: str = "overwrite",
):
    """Funcion para guardar datos en formato parquet en el catalogo de glue

    Args:
        spark_df (Dataframe): Dataframe que se va guardar
        hudi_options (dict): Diccionario con las opciones de Hudi
        destination_catalog_database_name (str): Base de datos del catálogo de glue
        destination_catalog_table_name (str): Tabla del catálogo de glue
        path (str): Ruta en S3 donde quedan los datos
        write_format (str): Formato para guardar los datos (hudi)
        mode (str): Modo de guardado de los datos (append, insert, overwrite...)
    """
    save(spark_df=spark_df, hudi_options=hudi_options, path=path, write_format="hudi", mode=mode)


def save_analytics(df: DataFrame, job_config: JobConfig):

    analitycs_writer_options = job_config.analytics_writer_options

    if IS_LOCAL_ENV:  # pragma: no cover
        current_path = os.getcwd()
        analitycs_writer_options['path'] = f"{current_path}/dev/catalog/{job_config.analytics_database}/{job_config.table_name}"
        df.sparkSession.sql(f'create database if not exists {job_config.analytics_database};').collect()
    print(f'Saving {job_config.analytics_database}.{job_config.table_name} at {analitycs_writer_options}')

    (
        df.write
        .format('parquet')
        .mode('overwrite')
        .options(**analitycs_writer_options)
        .saveAsTable(f"{job_config.analytics_database}.{job_config.table_name}")
    )
