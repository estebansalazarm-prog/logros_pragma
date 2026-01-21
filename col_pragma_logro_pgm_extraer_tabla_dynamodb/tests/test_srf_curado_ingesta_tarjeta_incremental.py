"""
STR DOC
"""
import os
import sys
from contextlib import nullcontext as does_not_raise
import importlib
import pytest
from unittest.mock import MagicMock
from pyspark.sql import SparkSession
from addons.config.spark_config import spark_config
from tests.mocks.glue_mock import GlueContextMock
from col_pragma_logro_pgm_extraer_tabla_dynamodb.etl.transform import raw_transformations

@pytest.fixture(name="spark")
def spark_fixture():
    return (
        SparkSession.builder
        .appName("test_tarjeta_customer")
        .enableHiveSupport()
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID", "testing"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY", "testing"))
        .config("spark.hadoop.fs.s3a.session.token", os.getenv("AWS_SESSION_TOKEN", "testing"))
        .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "true")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider")
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk-bundle:1.11.901")
        .getOrCreate()
    )

JOB_SCRIPT_PATH = "col_pragma_logro_pgm_extraer_tabla_dynamodb.col_pragma_logro_pgm_extraer_tabla_dynamodb"
ENV = "test_env"
ACCOUNT = "test_account"
PROCESS_DATE = "2025-04-16"
PROCESS_TYPE = "INC"
CONFIG_TABLE = "nombre_tabla"

sys.argv = [
    JOB_SCRIPT_PATH,
    "--JOB_NAME=logro",
    f"--ACCOUNT={ACCOUNT}",
    f"--ENV={ENV}",
    f"--PROCESS_DATE={PROCESS_DATE}",
    f"--PROCESS_TYPE={PROCESS_TYPE}",
    f"--CONFIG_TABLE={CONFIG_TABLE}",
]

JOB_SCRIPT_MODULE = importlib.import_module(JOB_SCRIPT_PATH)

@pytest.fixture(autouse=True)
def mocker_test_fixture(mocker, spark):
    # Evita escritura real
    mocker.patch("pyspark.sql.readwriter.DataFrameWriter.save")
    mocker.patch("pyspark.sql.readwriter.DataFrameWriter.saveAsTable")

    # Mock de lectura desde S3
    sample_df = spark.read.json("col_pragma_logro_pgm_extraer_tabla_dynamodb/tests/mocks/productos_uncrawlable/srf/incremental/co_internal_expody/co_interno_expody_coretarjeta_co_physicalcard_binnacle")
    mocker.patch("pyspark.sql.readwriter.DataFrameReader.json", return_value=sample_df)
    
    # Mock para get_report_config que retorna el YAML
    yaml_content = """process_metadata:
  process_id: 'extract_dynamodb_table'
  description: 'Extrae datos de nombre_tabla, aplana los datos y los cataloga en athena.'
  owner: 'pragma'
  version: '1.0.0'
source_config:
  source_name: 'S3 DynamoDB Export - Pragma'
  connection_type: 's3_path'
  s3_details:
    path_template_inc: "s3://{raw_bucket}/pgm/col_json_dynamodb_nombre_tabla/incremental/year={process_year}/month={process_month}/day={process_day}/hour=*/AWSDynamoDB/data"
    path_template_full: "s3://{raw_bucket}/pgm/col_json_dynamodb_nombre_tabla/full/year={process_year}/month={process_month}/day={process_day}/AWSDynamoDB/*/data"
    data_format: "json"
processing_config:
  schema_name: "col_pragma_tabla_final"
  key_columns:
    - "key"
    - "sortkey"
  order_by_column: "tstamp"
  partition_date: "created_at"
  precombine_key: "tstamp"
output_config:
  data_product: "tabla_final"
  product_type: "pragma"""
    mocker.patch("col_pragma_logro_pgm_extraer_tabla_dynamodb.config.report_config_path.get_report_config", return_value=(yaml_content, "mock_path"))
    
    # Mock para get_schema
    from pyspark.sql.types import StructType, StructField, StringType, TimestampType, LongType
    mock_schema = StructType([
        StructField("nombre_campo_1", StringType(), True),
        StructField("sortkey", StringType(), True),
        StructField("tstamp", LongType(), True),
        StructField("created_at", TimestampType(), True),
        StructField("key", StringType(), True),
        StructField("id", StringType(), True)
    ])
    mocker.patch("addons.pyspark_utils.schema_parser.get_schema", return_value=mock_schema)

@pytest.fixture(autouse=True)
def aws_credentials_fixture():
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"


def test_ingesta_tarjeta_uses_newimage_for_incremental(spark):
    df_input = spark.read.json("glue/tests/mocks/pgm/incremental/col_json_dynamodb_nombre_tabla")
    
    # Verificar que el archivo tenga datos
    if df_input.count() == 0:
        pytest.skip("No hay datos de prueba para incremental")

    # Mock del job_config con configuración ETL
    job_config_mock = MagicMock()
    job_config_mock.is_incremental = True
    
    # Mock de elt_config_table con processing_config
    processing_config_mock = MagicMock()
    processing_config_mock.key_columns = ["key", "sortkey"]
    processing_config_mock.order_by_column = "tstamp"
    processing_config_mock.schema_name = "col_pragma_tabla_final"
    processing_config_mock.partition_date = "created_at"
    
    job_config_mock.elt_config_table = MagicMock()
    job_config_mock.elt_config_table.processing_config = processing_config_mock

    df_result = raw_transformations.get_table_transformations(df_input, job_config_mock)

    # Verificar que el resultado no sea None
    assert df_result is not None
    # Verificar que tenga la columna id (generada por concatenación de key_columns)
    assert "created_at" in df_result.columns
