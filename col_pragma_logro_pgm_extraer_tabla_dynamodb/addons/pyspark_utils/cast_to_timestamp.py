from pyspark.sql import DataFrame, functions as F, types as T

def cast_column_to_timestamp(df: DataFrame, column_name: str) -> DataFrame:
    if column_name not in df.columns:
        df = df.withColumn(column_name, F.lit(None))

    return df.withColumn(column_name, F.to_timestamp(column_name))

def cast_column_unix_to_timestamp(df: DataFrame, column_name: str) -> DataFrame:
    if column_name not in df.columns:
        df = df.withColumn(column_name, F.lit(None))

    return df.withColumn(column_name, F.from_unixtime(F.col(column_name).cast(T.LongType())/1000))


def cast_string_to_timestamp(df: DataFrame, column_name: str) -> DataFrame:
    if column_name not in df.columns:
        df = df.withColumn(column_name, F.lit(None))

    df = df.withColumn(
        column_name,
        F.when(
            # Si es numérico y tiene 13 o más dígitos → milisegundos
            (F.col(column_name).rlike(r"^\d{13,}$")),
            # Solución: Convertir directamente de Unix (milisegundos) a timestamp
            (F.col(column_name) / 1000).cast("timestamp")
        ).when(
            # Si es numérico y tiene 10 dígitos → segundos
            (F.col(column_name).rlike(r"^\d{10}$")),
            # Solución: Convertir directamente de Unix (segundos) a timestamp
            F.col(column_name).cast("timestamp")
        ).when(
            # Si termina en 'Z', es un string UTC
            F.col(column_name).rlike(r"Z$"),
            # Solución: Simplemente parsear el string a timestamp.
            # Spark entenderá la 'Z' como UTC.
            F.to_timestamp(F.col(column_name), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
        ).when(
            # Si tiene zona horaria con offset tipo -05:00 y texto entre corchetes
            F.col(column_name).rlike(r"\[UTC[-+]\d{2}:\d{2}\]$"),
            # Solución: Simplemente parsear el string.
            # El 'XXX' en el formato manejará el offset (ej: -05:00).
            F.to_timestamp(
                F.regexp_replace(F.col(column_name), r"\[UTC([-+]\d{2}:\d{2})\]", r"\1"),
                "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"
            )
        ).otherwise(
            # Lógica de fallback: intento de conversión estándar
            F.to_timestamp(F.col(column_name))
        )
    )

    return df