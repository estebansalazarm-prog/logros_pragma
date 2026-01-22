from pyspark.sql import DataFrame, functions as F, window as W
from addons.config.job_config import JobConfig
from addons.pyspark_utils.align_shema import align_schema
from addons.pyspark_utils.flatten_dynamodb_struct import flatten_dynamodb_struct
from addons.pyspark_utils.cast_to_timestamp import cast_column_unix_to_timestamp, cast_string_to_timestamp
from addons.pyspark_utils.schema_parser import get_schema


def get_table_transformations(
        df_dinamic: DataFrame,
        job_config: JobConfig
) -> DataFrame:

    print("Iniciando transformaciones")
    print(type(df_dinamic))
    if df_dinamic.count() == 0:
        return None
    
    # Validar que existe la configuración ETL desde YAML
    if job_config.elt_config_table is None:
        raise ValueError("No se ha cargado configuración ETL desde YAML. Llamar load_etl_config_from_yaml() primero.")
    
    processing_config = job_config.elt_config_table.processing_config
    
    # Leer key_columns del YAML con validación robusta
    key_columns = []
    if hasattr(processing_config, 'key_columns') and processing_config.key_columns:
        key_columns = processing_config.key_columns
    
    # Leer order_by_column del YAML con validación robusta
    order_by_column = None
    if hasattr(processing_config, 'order_by_column') and processing_config.order_by_column:
        order_by_column = processing_config.order_by_column
    
    # Leer schema_name del YAML con validación robusta
    schema_name = None
    if hasattr(processing_config, 'schema_name') and processing_config.schema_name:
        schema_name = processing_config.schema_name
    
    # Leer partition_date del YAML con validación robusta
    partition_date = None
    if hasattr(processing_config, 'partition_date') and processing_config.partition_date:
        partition_date = processing_config.partition_date

    column_to_validate = "Item"  # Si es una carga Full

    if job_config.is_incremental:
        column_to_validate = "NewImage"  # Si es una carga Incremental
        print("Carga incremental")
    else:
        print("Carga full")

    print("Schema del DataFrame")
    df_dinamic.printSchema()

    df_flattened = flatten_dynamodb_struct(df=df_dinamic, parent_col=column_to_validate)

    # Se adiciona un filtro cuando llegan valores NULLs (usando key_columns del YAML)
    if len(key_columns) > 0:
        filter_conditions = [F.col(col).isNotNull() for col in key_columns]
        combined_filter = filter_conditions[0]
        for condition in filter_conditions[1:]:
            combined_filter = combined_filter & condition
        df_flattened = df_flattened.filter(combined_filter)
        print(f"Filtros aplicados para columnas: {key_columns}")

    # Definir ventana para obtener el último estado por transacción (usando key_columns y order_by_column del YAML)
    # Solo aplicar ventana si tenemos key_columns y order_by_column configurados
    if len(key_columns) > 0 and order_by_column:
        window_spec = W.Window.partitionBy(*key_columns).orderBy(F.col(order_by_column).desc())
        print(f"Ventana configurada - Partición: {key_columns}, Orden: {order_by_column} desc")

        # Obtener el último estado por transacción
        df_latest_status = (
            df_flattened.withColumn("row_number", F.row_number().over(window_spec))
            .filter(F.col("row_number") == 1)
            .drop("row_number")
    )
    else:
        # Si no hay key_columns u order_by_column, usar el DataFrame sin ventana
        df_latest_status = df_flattened
        print("No se aplicó ventana: key_columns u order_by_column no configurados")

    # Usar schema_name del YAML (solo si está configurado)
    if schema_name:
        saved_schema = get_schema(schema_name)
        print(f"Schema usado: {schema_name}")
        df_aligned_schema = align_schema(df_latest_status, saved_schema)
    else:
        print("No se especificó schema_name, usando DataFrame sin alinear")
        df_aligned_schema = df_latest_status
    
    # Agregar columna de id DESPUÉS de align_schema (para que no se elimine)
    # La columna id será la llave primaria de la tabla
    # Solo crear id si hay al menos 2 key_columns configuradas
    if len(key_columns) >= 2:
        # Genera el nombre de la columna combinada a partir de todas las columnas key del YAML
        concat_col_name = "id"
        cols_to_concat = [F.col(k) for k in key_columns]
        df_aligned_schema = df_aligned_schema.withColumn(concat_col_name, F.concat_ws("-", *cols_to_concat))
        print(f"Columna '{concat_col_name}' creada a partir de: {key_columns}")

    print("Schema del json")
    df_latest_status.select(sorted(df_latest_status.columns)).printSchema()

    print("Schema del json actualizado")
    df_aligned_schema = df_aligned_schema.select(sorted(df_aligned_schema.columns))
    df_aligned_schema.printSchema()

    # Convertir partition_date a timestamp DESPUÉS de align_schema (puede revertir el tipo)
    # Esto asegura que la columna esté convertida cuando llegue a metadata.py
    if partition_date and partition_date in df_aligned_schema.columns:
        col_type_before = dict(df_aligned_schema.dtypes)[partition_date]
        
        # Solo convertir si es BIGINT/LONG (Unix timestamp en milisegundos)
        if col_type_before in ['bigint', 'long']:
            print(f" Convirtiendo '{partition_date}' de Unix timestamp (milisegundos) a timestamp")
            df_aligned_schema = cast_column_unix_to_timestamp(df=df_aligned_schema, column_name=partition_date)
        elif col_type_before in ['string']:
            # Si es string, intentar convertir a timestamp
            print(f" Convirtiendo '{partition_date}' de string a timestamp")
            df_aligned_schema = cast_string_to_timestamp(df=df_aligned_schema, column_name=partition_date)
        else:
            print(f"'{partition_date}' ya es tipo fecha/timestamp ({col_type_before}), no se requiere conversión")
    
    # Aplicar post_read_filters si están configurados
    if hasattr(processing_config, 'post_read_filters') and processing_config.post_read_filters:
        for filter_str in processing_config.post_read_filters:
            df_aligned_schema = df_aligned_schema.filter(filter_str)
            print(f"Filtro post-lectura aplicado: {filter_str}")

    # Aplicar rename_columns si están configurados
    if hasattr(processing_config, 'rename_columns') and processing_config.rename_columns:
        for rename in processing_config.rename_columns:
            df_aligned_schema = df_aligned_schema.withColumnRenamed(rename.old_name, rename.new_name)
            print(f"Columna renombrada: {rename.old_name} -> {rename.new_name}")

    # Aplicar sql_transformations si están configurados
    if hasattr(processing_config, 'sql_transformations') and processing_config.sql_transformations:
        for sql_transformation in processing_config.sql_transformations:
            df_aligned_schema = df_aligned_schema.withColumn(sql_transformation.name, F.expr(sql_transformation.sql))
            print(f"SQL Transformation: {sql_transformation.name}")
            print(f"SQL: {sql_transformation.sql}")

    return df_aligned_schema
