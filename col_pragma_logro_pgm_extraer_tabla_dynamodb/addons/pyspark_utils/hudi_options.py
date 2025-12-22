from typing import TypedDict

HudiOptions = TypedDict(
    'HudiOptions', {
        "hoodie.table.name": str,
        "hoodie.datasource.write.storage.type": str,
        "hoodie.datasource.write.operation": str,
        "hoodie.datasource.write.recordkey.field": str,
        "hoodie.datasource.write.precombine.field": str,
        "hoodie.datasource.write.hive_style_partitioning": str,
        "hoodie.metadata.enable": str,
        "hoodie.datasource.hive_sync.enable": str,
        "hoodie.datasource.hive_sync.database":  str,
        "hoodie.datasource.hive_sync.table": str,
        "hoodie.datasource.hive_sync.partition_extractor_class": str,
        "hoodie.datasource.hive_sync.use_jdbc": str,
        "hoodie.datasource.hive_sync.mode": str,
        "hoodie.datasource.hive_sync.partition_fields": str,
        "hoodie.datasource.write.partitionpath.field": str,
        "mode": str,
    },
    total=False
)
