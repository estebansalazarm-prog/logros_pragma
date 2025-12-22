import os
from pyspark.sql import SparkSession

IS_LOCAL_ENV = os.environ.get('LOCAL_ENV', None) == "DEV"


class DevGlueReader:  # pragma: no cover
    """
    Clase mock para leer los esquemas del df
    """

    def __init__(self, spark: SparkSession, ) -> None:
        self.spark: SparkSession = spark

    def from_options(self, **options):
        paths = options["connection_options"]["paths"]
        new_paths = []
        # converts s3 uris to local dev path and removing the bucket
        for path in paths:
            if path.startswith("s3://"):
                path = path.replace("s3://", "dev/catalog/")
                new_paths.append(path)

        table = self.spark.read.json(new_paths)
        def to_df():
            return table

        table.toDF = to_df
        return table

    def from_catalog(
        self,
        database=None,
        table_name=None,
    ):
        table = self.spark.table(f'{database}.{table_name}')

        def to_df():
            return table

        table.toDF = to_df
        return table


class DevGlueContext:  # pragma: no cover
    """
    DOC STRING
    """

    def __init__(self, spark):
        self.create_data_frame = DevGlueReader(spark)
        self.create_dynamic_frame = DevGlueReader(spark)
        self.spark_session = spark
        prepare_locale(spark._jvm)


def prepare_locale(jvm):  # pragma: no cover
    locale_class = jvm.java.util.Locale

    # Obtiene el locale predeterminado
    colombian_locale = locale_class("es", "CO")  # Idioma: Español, País: Colombia

    # Establece el nuevo locale como el predeterminado
    locale_class.setDefault(colombian_locale)

    default_locale = locale_class.getDefault()

    # Imprime el valor del locale predeterminado
    print("Default Locale:", default_locale)
