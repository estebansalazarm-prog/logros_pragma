from dataclasses import dataclass
from pyspark.sql import DataFrame
from addons.config.job_config import JobConfig


@dataclass
class PreExtractSources:
    """Clase que tiene las modificacinoes basicas a los dataframe para ser usados en las fuentes"""
    job_config: JobConfig

