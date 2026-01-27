"""
Test para verificar el flujo completo de ingesta INCREMENTAL con configuración YAML
"""
import os
import sys
from contextlib import nullcontext as does_not_raise
import importlib
import pytest
from pyspark.sql import SparkSession
from addons.config.spark_config import spark_config
from tests.mocks.glue_mock import GlueContextMock

@pytest.fixture(name="spark")
def spark_fixture():
    return (
        SparkSession.builder
        .appName("test_dynamodb_table")
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

@pytest.fixture(scope="function", autouse=True)
def setup_module_fixture():
    """
    Fixture que configura sys.argv y recarga el módulo ANTES de cada test.
    Esto asegura que cada test tenga su propia configuración limpia.
    """
    # Configurar sys.argv con los valores correctos para este test (INC)
    sys.argv = [
        JOB_SCRIPT_PATH,
        "--JOB_NAME=logro",
        f"--ACCOUNT={ACCOUNT}",
        f"--ENV={ENV}",
        f"--PROCESS_DATE={PROCESS_DATE}",
        f"--PROCESS_TYPE={PROCESS_TYPE}",  # INC
        f"--CONFIG_TABLE={CONFIG_TABLE}",
    ]

    # Forzar recarga del módulo para que lea los nuevos valores de sys.argv
    # Esto es crítico porque el código a nivel de módulo lee sys.argv al importarse
    if JOB_SCRIPT_PATH in sys.modules:
        # Eliminar el módulo del cache para forzar recarga completa
        del sys.modules[JOB_SCRIPT_PATH]
    
    # Importar/recargar el módulo - esto ejecutará el código a nivel de módulo
    # que leerá sys.argv con los valores correctos
    importlib.import_module(JOB_SCRIPT_PATH)
    
    yield
    
    # Cleanup después del test
    pass

@pytest.fixture(autouse=True)
def mocker_test_fixture(mocker, spark):
    # Evita escritura real
    mocker.patch("pyspark.sql.readwriter.DataFrameWriter.save")
    mocker.patch("pyspark.sql.readwriter.DataFrameWriter.saveAsTable")

    # Mock de lectura desde S3 usando rutas relativas al directorio del test
    test_dir = os.path.dirname(os.path.abspath(__file__))
    mock_path = os.path.join(test_dir, "mocks", "pgm", "incremental", "col_json_dynamodb_nombre_tabla")
    sample_df = spark.read.json(mock_path)
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
  product_type: "pragma"
"""
    
    mocker.patch("col_pragma_logro_pgm_extraer_tabla_dynamodb.config.report_config_path.get_report_config", return_value=(yaml_content, "mock_path"))
    
    # Mock para get_schema - debe coincidir con el schema YAML real
    from pyspark.sql.types import StructType, StructField, StringType, TimestampType, LongType
    mock_schema = StructType([
        StructField("created_at", TimestampType(), True),
        StructField("id", StringType(), False),  # id se genera, no es nullable
        StructField("key", StringType(), True),
        StructField("nombre_campo_1", StringType(), True),
        StructField("nombre_campo_2", StringType(), True),
        StructField("nombre_campo_3", StringType(), True),
        StructField("sortkey", StringType(), True),
        StructField("tstamp", LongType(), True)
    ])
    mocker.patch("addons.pyspark_utils.schema_parser.get_schema", return_value=mock_schema)

@pytest.fixture(autouse=True)
def aws_credentials_fixture():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"


def test_main(spark: SparkSession, setup_module_fixture):
    """
    Test del flujo completo de ingesta INCREMENTAL.
    Verifica que el proceso completo se ejecute sin errores usando configuración YAML.
    """
    # Importar el módulo después de que el fixture haya configurado sys.argv
    job_script_module = importlib.import_module(JOB_SCRIPT_PATH)
    
    with does_not_raise():
        glue_context = GlueContextMock(spark=spark)
        job_script_module.main(spark=spark, glue_context=glue_context)