"""
Genera el schema en pyspark
"""

import os
import zipfile

from importlib import resources as impresources

import yaml
from col_pragma_logro_pgm_extraer_tabla_dynamodb.config import reports


def get_reports_path():
    reports_path = str(impresources.files(reports))
    print("Reports Path", reports_path)

    if ".whl" in str(reports_path):
        reports_path = get_unziped_reports_path(reports_path)

    return reports_path


def unzip(source_filename, dest_dir):
    with zipfile.ZipFile(source_filename) as zf:
        zf.extractall(dest_dir)


def get_unziped_reports_path(reports_path):
    zip_file_path = reports_path.split(".whl")[0] + ".whl"
    unzip_path = "/".join(zip_file_path.split("/")[:-1]) + "/unzip/"
    unzip(zip_file_path, unzip_path)
    reports_path = (
        unzip_path + "col_pragma_logro_pgm_extraer_tabla_dynamodb/config/reports"
    )
    return reports_path


def get_report_config(report_name, local_env=True):
    report_path = get_reports_path()
    print("Report Path:", report_path)
    filepath = f'{report_path}/{report_name}.yml'
    print("Report File:", filepath)

    with open(filepath, "r", encoding="utf-8") as f:
        raw_config_str = f.read()

    return raw_config_str, filepath
