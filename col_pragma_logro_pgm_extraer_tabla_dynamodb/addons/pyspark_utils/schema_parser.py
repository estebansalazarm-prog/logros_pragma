"""
Genera el schema en pyspark
"""
import zipfile

from importlib import resources as impresources

import yaml
import pyspark.sql.types as T

from addons.config import schemas


def get_schemas_path():
    inputs_path = str(impresources.files(schemas))
    print("Inputs Path", inputs_path)

    if ".whl" in str(inputs_path):
        inputs_path = get_unziped_path(inputs_path)

    return inputs_path


def get_unziped_path(input_path): # pragma: no cover
    zip_file_path = input_path.split(".whl")[0] + ".whl"
    unzip_path = "/".join(zip_file_path.split("/")[:-1]) + "/unzip/"
    unzip(zip_file_path, unzip_path)
    schemas_path = unzip_path + "addons/config/schemas"
    return schemas_path


def unzip(source_filename, dest_dir): # pragma: no cover
    with zipfile.ZipFile(source_filename) as zf:
        zf.extractall(dest_dir)

def get_spark_type(yaml_type: str) -> T.DataType:
    """
    Mapeo del tipo de dato en el yaml a uno de PySpark 
    
    Returns:
        pyspark.sql.types.DataType:.
    """
    if yaml_type == 'varchar':
        return T.StringType()
    else:
        raise TypeError(f"Unsupported SQL type: {yaml_type}")

def get_struct_field(config, table_name): # pragma: no cover
    col_name = config['Name']
    col_type = config['Type']
    if col_type == 'varchar':
        return T.StructField(col_name, T.StringType(), True)
    elif col_type == 'integer':
        return T.StructField(col_name, T.IntegerType(), True)
    elif col_type == 'bigint':
        return T.StructField(col_name, T.LongType(), True)
    elif col_type == 'float':
        return T.StructField(col_name, T.FloatType(), True)
    elif col_type == 'double':
        return T.StructField(col_name, T.DoubleType(), True)
    elif col_type == 'timestamp':
        return T.StructField(col_name, T.TimestampType(), True)
    elif col_type == 'date':
        return T.StructField(col_name, T.DateType(), True)
    elif col_type == 'decimal':
        return T.StructField(col_name, T.DecimalType(precision=config['Precision'], scale=config['Scale']), True)
    elif col_type == 'boolean':
        return T.StructField(col_name, T.BooleanType(), True)
    elif col_type == 'row': 
        print(col_name)
        return T.StructField(col_name, T.StructType([get_struct_field(col, f"{table_name}.{col_name}") for col in config['Columns']]))
    elif col_type == 'array': 
        if 'Columns' in config.keys():
            return T.StructField(col_name, T.ArrayType(T.StructType([get_struct_field(col, f"{table_name}.{col_name}") for col in config['Columns']])))
        elif 'ArrayType' in config.keys():
            return T.StructField(col_name, T.ArrayType(get_spark_type(config['ArrayType'])))
    else:
        raise TypeError(f"No se pudo procesar la tabla: {table_name}, config: {config}")

def get_schema(table_name):
    schemas_path = get_schemas_path()
    print("Schemas Path:", schemas_path)

    filepath = f'{schemas_path}/{table_name}.schema.yml'

    with open(filepath, 'r', encoding='utf-8') as file:
        schema_config = yaml.safe_load(file)

    schema = T.StructType()
    for config in schema_config:
        schema.add(get_struct_field(config, table_name))

    if len(schema) != len(schema_config):
        raise TypeError(f"No se pudo procesar la tabla {table_name}")

    return schema
