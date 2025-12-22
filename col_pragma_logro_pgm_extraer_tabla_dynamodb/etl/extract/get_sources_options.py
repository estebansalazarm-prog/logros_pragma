from addons.config.job_config import JobConfig
from addons.pyspark_utils.table import Table, S3File


def get_sources_options_report(table_name: str, job_config: JobConfig) -> Table | S3File:
    """
    Obtiene las opciones de la tabla desde elt_config_table.
    
    Convierte path_template de S3Details a bucket/prefix/format/origin para crear S3File.
    Soporta tanto 'catalog' como 's3_path'.
    
    Args:
        table_name (str): Nombre de la tabla.
        job_config (JobConfig): Configuración del trabajo con elt_config_table cargado.
    
    Returns:
        Table | S3File: Objeto con las opciones de la tabla.
    """
    if job_config.elt_config_table is None:
        raise ValueError(f"No se ha cargado configuración ETL para '{table_name}'. Llamar load_etl_config_from_yaml() primero.")
    
    source_config = job_config.elt_config_table.source_config
    
    if source_config.connection_type == 's3_path':
        # Para s3_path: convertir path_template a bucket/prefix/format/origin
        if source_config.s3_details is None:
            raise ValueError(f"s3_details no está configurado para connection_type='s3_path'")
        
        if job_config.is_incremental:
            path_template = source_config.s3_details.path_template_inc       
        else:
            path_template = source_config.s3_details.path_template_full
        
        # Parsear s3://bucket/prefix a bucket y prefix
        if path_template.startswith("s3://"):
            path_without_prefix = path_template[5:]  # Remover "s3://"
            parts = path_without_prefix.split("/", 1)
            bucket = parts[0]
            prefix = parts[1] if len(parts) > 1 else ""
        else:
            raise ValueError(f"path_template debe empezar con 's3://', recibido: {path_template}")
        
        # Crear S3File con los valores parseados
        return S3File(
            bucket=bucket,
            prefix=prefix,
            origin='s3',
            format=source_config.s3_details.data_format
        )
    
    elif source_config.connection_type == 'catalog':
        # Para catalog: usar catalog_details
        if source_config.catalog_details is None:
            raise ValueError(f"catalog_details no está configurado para connection_type='catalog'")
        
        table_class = Table(
            origin='catalog',  # O el que corresponda
            database=source_config.catalog_details.database_template,
            table_name=source_config.catalog_details.table_name,
        )

        if source_config.catalog_details.additional_options_templates:
            table_class.additional_options = source_config.catalog_details.additional_options_templates

        return table_class
    
    else:
        raise ValueError(f"connection_type '{source_config.connection_type}' no soportado")
