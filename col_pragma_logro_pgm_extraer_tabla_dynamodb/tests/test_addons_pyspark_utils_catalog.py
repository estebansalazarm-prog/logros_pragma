import pytest
from unittest.mock import Mock, patch
from awsglue.context import GlueContext
from addons.config.job_config import JobConfig
from addons.pyspark_utils.table import S3File, Table
from addons.pyspark_utils.catalog import Catalog, CatalogException


def test_get_table_s3():
    # Mocks necesarios
    glue_context = Mock(spec=GlueContext)
    spark_session_mock = Mock()
    json_reader_mock = Mock(return_value="mocked_dataframe")

    # Configurar el contexto de Spark
    glue_context.spark_session = spark_session_mock
    spark_session_mock.read.json = json_reader_mock

    # Crear el objeto S3File
    s3_file = S3File(
        bucket="test-bucket",
        prefix="some/prefix/path",
        origin="some-origin",
        format="json"
    )

    # Mock de la función que retorna S3File
    get_table_func = Mock(return_value=s3_file)

    # Instancia del catálogo
    job_config = Mock(spec=JobConfig)
    catalog = Catalog(glue_context, job_config, get_table_func)

    df = catalog.get_table("test_table")

    json_reader_mock.assert_called_once_with(["s3://test-bucket/some/prefix/path"])
    assert df == "mocked_dataframe"


def test_get_table_invalid_source():
    glue_context = Mock(spec=GlueContext)
    job_config = Mock(spec=JobConfig)
    get_table_func = Mock(return_value=None)

    catalog = Catalog(glue_context, job_config, get_table_func)
    with pytest.raises(CatalogException, match="No se tiene otro tipo registrado de carga"):
        catalog.get_table("invalid_table")


def test_check_table_exists():
    glue_context = Mock(spec=GlueContext)
    glue_context.spark_session = Mock()
    glue_context.spark_session.catalog = Mock()
    glue_context.spark_session.catalog.tableExists = Mock()

    glue_context.spark_session.catalog.tableExists.return_value = True

    catalog = Catalog(glue_context, Mock(spec=JobConfig))
    exists = catalog.check_table_exists("test_db", "test_table")

    glue_context.spark_session.catalog.tableExists.assert_called_once_with("test_table", "test_db")
    assert exists
