from pyspark.sql import DataFrame
import pyspark.sql.functions as F

from addons.config.job_config import JobConfig


def add_metadata(df: DataFrame, job_config: JobConfig):
    """
    Función agrega los metadatos de particion y ejecucion
    """
    # Obtener partition_date del YAML o usar created_at por defecto
    partition_date_col = "created_at"  # Default
    if job_config.elt_config_table is not None:
        processing_config = job_config.elt_config_table.processing_config
        if hasattr(processing_config, 'partition_date') and processing_config.partition_date:
            partition_date_col = processing_config.partition_date
            print(f"Usando columna de partición desde YAML: {partition_date_col}")
    
    # Usar la columna directamente (ya debe estar convertida a timestamp en raw_transformations.py)
    # Si la columna no existe, usar fecha actual como fallback
    if partition_date_col in df.columns:
        partition_timestamp = F.col(partition_date_col)

      
    return (
        df
        .withColumn("momento_ingestion", F.from_utc_timestamp(F.current_timestamp(), "America/Bogota"))
        .withColumn("job_process_date", F.lit(job_config.start_process_date))
        .withColumn("year", F.year(partition_timestamp))
        .withColumn("month", F.format_string("%02d", F.month(partition_timestamp)))
        .withColumn("day", F.format_string("%02d", F.dayofmonth(partition_timestamp)))
    )
