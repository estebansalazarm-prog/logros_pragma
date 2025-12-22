"""_summary_

Returns:
    _type_: _description_
"""
import os

from pyspark.sql import SparkSession
from addons.pyspark_utils.schema_parser import get_schema

class DataFrameReaderMock:
    """
    Clase mock para leer los esquemas del df
    """

    def __init__(self, spark: SparkSession, ) -> None:
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
        csv_path = f"{current_path}/glue/tests/mocks/data/{table_name}.csv"
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
        print(paths)
        # converts s3 uris to local dev path and removing the bucket
        for path in paths:
            path = "/".join(path.split("/")[3:7])
            path = "glue/tests/mocks/"+path+"/"
            new_paths.append(path)

        table = self.spark.read.json(new_paths)

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