import json
from addons.config.job_config import JobConfig
from addons.pyspark_utils.table import S3File, Table

class SourceMeta():
    """ TablesMeta """
    files = None

    def __init__(self, job_config: JobConfig) -> None:
        year, month, day = job_config.process_day_parts_str
        json_str = (
            json.dumps(job_config.table_sources)
            .replace('{raw_bucket}', job_config.raw_bucket)
            .replace('{current_subdomain}', job_config.constants.subdomain)
            .replace('{current_process_type_folder}', job_config.process_type_folder)
            .replace('{current_app_type_folder}', job_config.app_type_folder)
            .replace('{current_data_folder}', job_config.data_folder)
            .replace('{current_account}', job_config.account.lower())
            .replace('{current_env}', job_config.env.lower())
            .replace('{process_year}', year)
            .replace('{process_month}', month)
            .replace('{process_day}', day)
            .replace('{process_hour}', job_config.process_hour)
        )

        self.files = json.loads(json_str)

    def get(self, table_name: str):
        return self.files.get(table_name)


def get_source_options(table_name: str, job_config: JobConfig) -> Table | S3File:
    tables = SourceMeta(job_config=job_config)
    table_dict = tables.get(table_name)

    if table_dict["origin"] == "s3":
        return S3File(**table_dict)

    return Table(**table_dict)
