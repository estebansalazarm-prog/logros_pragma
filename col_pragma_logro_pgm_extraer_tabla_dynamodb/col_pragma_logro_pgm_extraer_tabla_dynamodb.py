import sys
from typing import Type
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from addons.config.constants import default_constants
from addons.config.job_config import JobConfig
from addons.config.local_env import IS_LOCAL_ENV, DevGlueContext, prepare_locale
from addons.config.spark_config import spark_config
from addons.pyspark_utils.catalog import Catalog
from addons.pyspark_utils.pre_extract_sources import PreExtractSources
from col_pragma_logro_pgm_extraer_tabla_dynamodb.config.sources_dictionary import TABLE_SOURCES
from col_pragma_logro_pgm_extraer_tabla_dynamodb.config.report_config import load_etl_config_from_yaml
from col_pragma_logro_pgm_extraer_tabla_dynamodb.etl.extract.get_sources_options import get_sources_options_report
from col_pragma_logro_pgm_extraer_tabla_dynamodb.etl.transform.transformations import Transformations
from col_pragma_logro_pgm_extraer_tabla_dynamodb.etl.load.metadata import add_metadata
from col_pragma_logro_pgm_extraer_tabla_dynamodb.etl.load.save import save_action, save_analytics
from col_pragma_logro_pgm_extraer_tabla_dynamodb.etl.extract.sources import RawSources

# Job-parameters
args = getResolvedOptions(
    sys.argv, ["ACCOUNT", "ENV", "PROCESS_DATE", "PROCESS_TYPE", "CONFIG_TABLE"])

ACCOUNT = args.get("ACCOUNT", "")
ENV = args.get("ENV", "")
PROCESS_DATE = args.get("PROCESS_DATE", "")
DATE_FORMAT = "%Y-%m-%d"
PROCESS_TYPE = args.get("PROCESS_TYPE", "")  # FULL, INC
CONFIG_TABLE = args.get("CONFIG_TABLE", "")

def main(spark: SparkSession, glue_context: GlueContext):
    # Crear JobConfig inicial (sin data_product, se cargará del YAML)
    constants = default_constants.from_dict(
        {
            "insert_mode": "append",
            "process_type": PROCESS_TYPE
        }
    )

    job_config = JobConfig(
        process_date=PROCESS_DATE,
        date_format=DATE_FORMAT,
        account=ACCOUNT,
        env=ENV,
        constants=constants,
        table_sources=TABLE_SOURCES
    )
    
    # Cargar configuración ETL desde YAML
    # Esto actualizará job_config.constants.data_product y asignará elt_config_table
    elt_config = load_etl_config_from_yaml(
        table_name=CONFIG_TABLE,
        job_config=job_config
    )
    
    if elt_config is None:
        raise ValueError("No se pudo cargar la configuración ETL desde YAML")
    
    # Asignar configuración al job_config
    job_config.elt_config_table = elt_config

    # Usar get_sources_options_report que lee del YAML
    catalog = Catalog(glue_context=glue_context, job_config=job_config, get_table_func=get_sources_options_report)
    sources = RawSources(catalog=catalog, pre_load_sources=PreExtractSources(job_config=job_config))
    transformations = Transformations(spark=spark, job_config=job_config, sources=sources)

    df = transformations.get_main_table()

    if df is None:
        print("No hay datos para procesar")
        return

    df_final = add_metadata(df=df, job_config=job_config).coalesce(8).cache()

    df_final.show(truncate=False)
    df_final.printSchema()

    print("Registros a insertar: ", df_final.count())

    # Se guarda el dataframe para tener un punto de checkpoint y tambien para poder subir a redshift desde analytics en formato parquet
    save_analytics(
        df=df_final,
        job_config=job_config,
    )

    # Se lee nuevamente analytics para poder sobreescribir los datos de la ultimas particiones del proceso
    df_analytics = catalog.get_dataframe_from_catalog(job_config.analytics_database, job_config.table_name)

    print("Dataframe de analytics leido desde el catalogo")
    df_analytics.printSchema()

    # Se guarda el dataframe
    save_action(
        spark_df=df_analytics,
        hudi_options=job_config.hudi_options,
        path=job_config.curated_table_path,
        mode=job_config.constants.insert_mode
    )


if __name__ == "__main__":  # pragma: no cover
    sparkBuilder: SparkSession.Builder = SparkSession.builder

    _spark: SparkSession = (
        sparkBuilder
        .appName(f"col_pragma_logro_pgm_extraer_tabla_dynamodb_{CONFIG_TABLE}")
        .enableHiveSupport()
        .config(conf=spark_config)
        .getOrCreate()
    )

    if IS_LOCAL_ENV:
        _glue_context = DevGlueContext(spark=_spark)
        # pylint: disable = W0212
        prepare_locale(_spark._jvm)
        print(f"Usando DevGlueContext: {DevGlueContext}")
    else:
        _glue_context = GlueContext(sparkContext=_spark.sparkContext)
        print(f"Usando GlueContext {GlueContext}")

    main(_spark, _glue_context)
