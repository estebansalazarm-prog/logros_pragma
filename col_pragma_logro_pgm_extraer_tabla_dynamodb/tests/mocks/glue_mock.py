"""_summary_

Returns:
    _type_: _description_
"""
import os
from pathlib import Path

from pyspark.sql import SparkSession

from addons.pyspark_utils.schema_parser import get_schema

# pylint: disable=all
# NOSONAR


class DataFrameReaderMock:
    """
    Clase mock para leer los esquemas del df
    """

    def __init__(self, spark: SparkSession) -> None:
        self.spark: SparkSession = spark

    def from_catalog(
        self,
        database=None,
        table_name=None,
        transformation_ctx="",
        additional_options=None,
        **kwargs
    ):
        schema = get_schema(table_name)
        current_path = os.getcwd()
<<<<<<< HEAD
        csv_path = f"{current_path}/tests/mocks/data/{table_name}.csv"
=======
        csv_path = f"{current_path}/col_pragma_logro_pgm_extraer_tabla_dynamodb/tests/mocks/data/{table_name}.csv"
>>>>>>> 1962a25100f2e5c1a632d3528f04b68d356c1b22
        print(csv_path)
        if os.path.exists(csv_path):
            table = self.spark.read.csv(csv_path, header=True, sep=',', schema=schema)
        else:
            table = self.spark.createDataFrame([], schema=schema)

        def to_df():
            return table

        table.toDF = to_df
        return table

    def from_options(self, **options):
        paths = options["connection_options"]["paths"]
        new_paths = []
<<<<<<< HEAD
        print("Original S3 paths:", paths)
=======
        print(paths)
        # converts s3 uris to local dev path and removing the bucket
        for path in paths:
            path = "/".join(path.split("/")[3:7])
            path = "col_pragma_logro_pgm_extraer_tabla_dynamodb/tests/mocks/"+path+"/"
            new_paths.append(path)
>>>>>>> 1962a25100f2e5c1a632d3528f04b68d356c1b22

        # Base path de los mocks (relativo a este archivo)
        mock_base_path = Path(__file__).resolve().parent.parent / "mocks"

        for path in paths:
            # Ejemplo: s3://bucket/pgm/col_json_dynamodb_nombre_tabla/full/year=2026/month=01/day=15/
            # Extraer todas las partes relevantes incluyendo el tipo de proceso (full/incremental)
            path_parts = path.split("/")[3:]  # Remover s3://bucket/
            relative_path = Path(*path_parts[:-3])  # Excluir year=2026/month=01/day=XX/
            full_path = mock_base_path / relative_path

            new_paths.append(str(full_path.resolve()))

        print("Resolved mock paths:", new_paths)
        try:
            table = self.spark.read.json(new_paths)
        except Exception as e:
            print("Error al procesar los archivos")
            raise e

        def to_df():
            return table

        table.toDF = to_df
        return table


class GlueContextMock:
    """
    DOC STRING
    """

    def __init__(self, spark):
        self.spark_session = spark
        self.create_data_frame = DataFrameReaderMock(spark)
        self.create_dynamic_frame = DataFrameReaderMock(spark)