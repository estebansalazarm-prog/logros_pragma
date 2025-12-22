import yaml
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Any

# from addons.config.job_config import JobConfig
from col_pragma_logro_pgm_extraer_tabla_dynamodb.config.report_config_path import (
    get_report_config,
)

# --- Definición de Dataclasses que coinciden con la estructura YAML ---


@dataclass
class ProcessMetadata:
    """Metadatos del proceso ETL."""

    process_id: str
    description: str
    owner: str
    version: str
    default_metadata: bool = False


@dataclass
class CatalogDetails:
    """Detalles para conexión a fuente de tipo 'catalog'."""

    database_template: str
    table_name: str
    additional_options_templates: Dict[str, str] = field(default_factory=dict)
    push_down_predicate_template: Optional[str] = None


@dataclass
class S3Details:
    """Detalles para conexión a fuente de tipo 's3_path'."""

    path_template_inc: str
    path_template_full: str
    data_format: str
    format_options: Dict[str, Any] = field(default_factory=dict)


@dataclass
class SourceConfig:
    """Configuración de la fuente de datos."""

    source_name: str
    connection_type: str
    catalog_details: Optional[CatalogDetails] = None
    s3_details: Optional[S3Details] = None

    def __post_init__(self):
        """Validación y asignación post-inicialización."""
        if self.connection_type == "catalog" and self.catalog_details is None:
            # En un caso real, podrías levantar un error aquí si falta catalog_details
            print(
                "Advertencia: connection_type es 'catalog' pero catalog_details no está definido."
            )
        elif self.connection_type == "s3_path" and self.s3_details is None:
            print(
                "Advertencia: connection_type es 's3_path' pero s3_details no está definido."
            )
        # Puedes añadir más validaciones según sea necesario


@dataclass
class RenameColumnConfig:
    """Configuración para renombrar una columna."""

    old_name: str
    new_name: str


@dataclass
class SqlTransformationConfig:
    """Configuración para renombrar una columna."""

    name: str
    sql: str


@dataclass
class ProcessingConfig:
    """Configuración del procesamiento y transformación."""

    schema_name: str
    key_columns: List[str] = field(default_factory=list)  # Para DynamoDB: columnas clave para filtros NOT NULL
    order_by_column: Optional[str] = None  # Para DynamoDB: columna para ordenar en ventana
    partition_date: Optional[str] = None  # Columna para particionar (year, month, day) - si está vacío, usa created_at por defecto
    select_columns: List[str] = field(default_factory=list)
    rename_columns: List[RenameColumnConfig] = field(default_factory=list)
    sql_transformations: List[SqlTransformationConfig] = field(default_factory=list)
    post_read_filters: List[str] = field(default_factory=list)

@dataclass
class OutputOptions:
    """Opciones específicas para la escritura de la salida (ej. CSV)."""

    header: Optional[bool] = True
    delimiter: Optional[str] = ","
    quote: Optional[str] = '"'
    escape: Optional[str] = '"'
    encoding: Optional[str] = "UTF-8"
    date_format: Optional[str] = None
    timestamp_format: Optional[str] = None
    compression: Optional[str] = "none"
    null_value: Optional[str] = None
    # Se pueden añadir más opciones de Spark DataFrameWriter aquí


@dataclass
class OutputConfig:
    """Configuración de la salida de datos."""

    format: str
    path_template: str
    options: OutputOptions = field(default_factory=OutputOptions)
    save_mode: str = "overwrite"
    output_file_name: Optional[str] = None
    output_partitions: Optional[int] = None


@dataclass
class ETLTableConfig:
    """Dataclass principal para toda la configuración ETL de una tabla."""

    process_metadata: ProcessMetadata
    source_config: SourceConfig
    processing_config: ProcessingConfig
    output_config: OutputConfig

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ETLTableConfig":
        """Crea una instancia de ETLTableConfig desde un diccionario (obtenido del YAML)."""

        # Mapeo para ProcessMetadata
        process_metadata_data = data.get("process_metadata", {})
        process_metadata = ProcessMetadata(**process_metadata_data)

        # Mapeo para SourceConfig
        source_config_data = data.get("source_config", {})
        catalog_details_data = source_config_data.get("catalog_details")
        s3_details_data = source_config_data.get("s3_details")

        catalog_details = None
        if catalog_details_data:
            catalog_details = CatalogDetails(**catalog_details_data)

        s3_details = None
        if s3_details_data:
            s3_details = S3Details(**s3_details_data)

        source_config = SourceConfig(
            source_name=source_config_data.get("source_name", ""),
            connection_type=source_config_data.get("connection_type", ""),
            catalog_details=catalog_details,
            s3_details=s3_details,
        )

        # Mapeo para ProcessingConfig
        processing_config_data = data.get("processing_config", {})
        rename_columns_data = processing_config_data.get("rename_columns", [])
        rename_columns = [RenameColumnConfig(**rc) for rc in rename_columns_data]

        sql_transformations_data = processing_config_data.get("sql_transformations", [])
        sql_transformations = [
            SqlTransformationConfig(**st) for st in sql_transformations_data
        ]

        processing_config = ProcessingConfig(
            schema_name=processing_config_data.get("schema_name", ""),
            key_columns=processing_config_data.get("key_columns", []),
            order_by_column=processing_config_data.get("order_by_column"),
            partition_date=processing_config_data.get("partition_date"),
            select_columns=processing_config_data.get("select_columns", []),
            rename_columns=rename_columns,
            post_read_filters=processing_config_data.get("post_read_filters", []),
            sql_transformations=sql_transformations,
        )

        # Mapeo para OutputConfig
        output_config_data = data.get("output_config", {})
        output_options_data = output_config_data.get("options", {})
        output_options = OutputOptions(**output_options_data)
        output_config = OutputConfig(
            format=output_config_data.get("format", "csv"),
            path_template=output_config_data.get("path_template", ""),
            options=output_options,
            save_mode=output_config_data.get("save_mode", "overwrite"),
            output_partitions=output_config_data.get("output_partitions"),
            output_file_name=output_config_data.get("output_file_name", None),
        )

        return cls(
            process_metadata=process_metadata,
            source_config=source_config,
            processing_config=processing_config,
            output_config=output_config,
        )


# --- Función para Cargar el YAML ---


def load_etl_config_from_yaml(
    table_name: str,
    job_config: "JobConfig",
) -> Optional[ETLTableConfig]:
    """
    Carga la configuración ETL desde un archivo YAML basado en el table_name
    y la mapea a un dataclass ETLTableConfig.

    Args:
        table_name (str): El nombre de la tabla, usado para construir el nombre del archivo YAML
                          (ej. 'mi_tabla' -> 'mi_tabla.yml').
        config_base_path (str): Ruta base donde se encuentran los archivos YAML.
                                Por defecto es el directorio actual.

    Returns:
        Optional[ETLTableConfig]: Una instancia de ETLTableConfig si la carga es exitosa,
                                  None en caso de error.
    """

    try:
        # Primero se extrae el nombre del data_product del archivo yml
        raw_config_str, file_path = get_report_config(table_name)
        config_data_tmp = yaml.safe_load(raw_config_str)
        data_product = config_data_tmp["output_config"]["data_product"]
        precombine_key = config_data_tmp["processing_config"]["precombine_key"]
        product_type = config_data_tmp["output_config"]["product_type"]
        # Se actualiza el data_product en los constantes del job_config preservando process_type
        job_config.update_constants(
            job_config.constants.from_dict({
                "data_product": data_product,
                "process_type": job_config.constants.process_type,
                "primary_key": "id",
                "precombine_key": precombine_key,
                "product_type": product_type,
            })
        )

        # Reemplazar placeholders globales antes de parsear YAML
        # Esto es un reemplazo simple; para lógica más compleja, se necesitaría un motor de plantillas.
        # Ejemplo de placeholders que podrían venir de runtime_params:
        # {current_env}, {process_year}, {process_month}, {process_day}, {push_down_predicate}
        for key, value in job_config.runtime_params.items():
            placeholder = "{" + str(key) + "}"
            raw_config_str = raw_config_str.replace(placeholder, str(value))

        config_data = yaml.safe_load(raw_config_str)

        if not config_data:
            print(
                f"Error: El archivo YAML '{table_name}' está vacío o no es un YAML válido después del reemplazo."
            )
            return None

        return ETLTableConfig.from_dict(config_data)

    except FileNotFoundError:
        print(f"Error: Archivo de configuración no encontrado para '{table_name}'")
        return None
    except yaml.YAMLError as e:
        print(f"Error al parsear el archivo YAML '{table_name}': {e}")
        return None
    except Exception as e:
        print(
            f"Ocurrió un error inesperado al cargar la configuración '{table_name}': {e}"
        )
        return None
