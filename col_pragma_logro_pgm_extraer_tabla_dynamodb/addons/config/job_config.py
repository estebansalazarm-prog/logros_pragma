from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Optional, TYPE_CHECKING

from addons.config.constants import Constants
from addons.pyspark_utils.hudi_options import HudiOptions

from col_pragma_logro_pgm_extraer_tabla_dynamodb.config.report_config import ETLTableConfig

@dataclass
class JobConfig:
    """Configuracion de parametros del JOB"""
    account: str
    env: str
    process_date: str
    date_format: str
    start_process_date: datetime = field(init=False)
    constants: Constants
    table_sources: dict
    extra_params: Optional[dict] = None
    push_down_predicate: Optional[str] = None
    report_config_file: Optional[str] = None
    elt_config_table: Optional[ETLTableConfig] = None  # ETLTableConfig se carga dinámicamente desde YAML
    replace_whrere_condition: Optional[str] = None

    def __post_init__(self) -> str:
        self.start_process_date = datetime.strptime(self.process_date, self.date_format)

    @property
    def process_day_parts_str(self) -> str:
        return self.start_process_date.strftime("%Y-%m-%d").split('-')

    # RAW
    @property
    def raw_bucket(self) -> str:
        return f"{self.constants.pais}-{self.constants.capacidad}-{self.constants.domain}-raw-{self.account}-{self.env}".lower()

    @property
    def table_name(self) -> str:
        return f"{self.constants.pais}_{self.constants.product_type}_{self.constants.data_product}".lower()
    
    @property
    def data_folder(self) -> str:
        if self.is_incremental:
            return "data"
        return "*/data"
        
    
    @property
    def app_type_folder(self) -> str:
        return "AWSDynamoDB"
    
    @property
    def process_type_folder(self) -> str:
        if self.is_incremental:
            return "incremental"
        return "full"
    
    @property
    def process_hour(self) -> str:
        if self.is_incremental:
            return "hour=*/"
        return ""
    
    @property
    def is_incremental(self) -> str:
        return self.constants.process_type == "INC"

    # CURATED
    @property
    def curated_database(self) -> str:
        return f"{self.constants.pais}_{self.constants.capacidad}_{self.constants.domain}_curated_{self.env}_rl".lower()

    @property
    def curated_bucket(self) -> str:
        return f"{self.constants.pais}-{self.constants.capacidad}-{self.constants.domain}-curated-{self.account}-{self.env}".lower()

    @property
    def curated_table_path(self) -> str:
        return f"s3://{self.curated_bucket}/{self.constants.subdomain}/{self.constants.catalog_name}/{self.table_name}/"

    # ANALYTICS
    @property
    def analytics_database(self) -> str:
        return f"{self.constants.pais}_{self.constants.capacidad}_{self.constants.domain}_analytics_{self.env}_rl".lower()

    @property
    def analytics_bucket(self) -> str:
        return f"{self.constants.pais}-{self.constants.capacidad}-{self.constants.domain}-analytics-{self.account}-{self.env}".lower()

    @property
    def analytics_table_path(self) -> str:
        return f"s3://{self.analytics_bucket}/{self.constants.subdomain}/{self.constants.catalog_name}/{self.table_name}/"

    @property
    def analytics_writer_options(self):
        return {'path': self.analytics_table_path}
    
    @property
    def primary_key(self):
        """
        Devuelve el nombre del campo que se utiliza como clave primaria en Hudi.
        El valor predeterminado es el nombre del campo "id".
        """
        return self.constants.primary_key

    @property
    def precombine_key(self):
        """
        Devuelve el nombre del campo que se utiliza para precombinar filas en Hudi.
        El valor predeterminado es el nombre del campo "last_history_date".
        """
        return self.constants.precombine_key

    @property
    def hudi_options(self) -> HudiOptions:
        config: HudiOptions = {
            "hoodie.table.name": self.table_name,
            "hoodie.datasource.write.storage.type": "COPY_ON_WRITE",
            "hoodie.datasource.write.operation": "upsert",
            "hoodie.datasource.write.recordkey.field": self.primary_key,
            "hoodie.datasource.write.precombine.field": self.precombine_key,
            "hoodie.datasource.write.hive_style_partitioning": "true",
            "hoodie.metadata.enable": "false",
            "hoodie.datasource.hive_sync.enable": "true",
            "hoodie.datasource.hive_sync.database":  self.curated_database,
            "hoodie.datasource.hive_sync.table": self.table_name,
            "hoodie.datasource.hive_sync.partition_extractor_class": "org.apache.hudi.hive.MultiPartKeysValueExtractor",
            "hoodie.datasource.write.payload.class": "org.apache.hudi.common.model.DefaultHoodieRecordPayload",
            "hoodie.datasource.hive_sync.use_jdbc": "false",
            "hoodie.datasource.hive_sync.mode": "hms",
            "mode": self.constants.insert_mode,
            "hoodie.datasource.hive_sync.partition_fields": "year,month,day",
            "hoodie.datasource.write.partitionpath.field": "year,month,day",
        }
        return config
    
    @property
    def runtime_params(self) -> dict:
        """
        Devuelve un diccionario con los parametros de ejecucion del job.
        """
        year, month, day = self.process_day_parts_str
        return {
            "process_date": self.process_date,
            "process_year": year,
            "process_month": month,
            "process_day": day,
            "account": self.account,
            "current_env": self.env,
            "current_account": self.account,
            "current_subdomain": self.constants.subdomain,
            "table_sources": self.table_sources,
            "push_down_predicate": self.push_down_predicate,
            # "output_bucket": self.output_bucket,  # Comentado si no existe
            "table_name": self.table_name,
            "raw_bucket": self.raw_bucket,
            "curated_bucket": self.curated_bucket,
            "analytics_bucket": self.analytics_bucket,
            "current_process_type_folder": self.process_type_folder,
            "process_hour": self.process_hour,
            "current_app_type_folder": self.app_type_folder,
            "current_data_folder": self.data_folder,
            **(self.extra_params or {}),
        }
    
    def update_constants(self, constants: Constants) -> None:
        """
        Actualiza los constantes del job_config.
        """
        self.constants = constants
