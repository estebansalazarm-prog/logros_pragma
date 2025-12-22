import re
from pyspark.sql import DataFrame, functions as F, types as T

dynamodb_types = {"M", "L", "S", "N", "BOOL", "NULL"}  # Tipos de DynamoDB a omitir
pattern = re.compile(r"(?<!^)(?=[A-Z])")


def extract_fields_from_array(schema, array_path):
    """
    Extrae automáticamente los campos dentro de una estructura de tipo ArrayType(StructType).

    :param schema: Esquema del DataFrame PySpark.
    :param array_path: Ruta de la columna array dentro del esquema.
    :return: Lista de nombres de los campos dentro del StructType de la lista.
    """
    parts = array_path.split(".")
    current_schema = schema

    for part in parts:
        field = next((f for f in current_schema.fields if f.name == part), None)
        if field is None:
            return []
        if isinstance(field.dataType, T.ArrayType):
            if isinstance(field.dataType.elementType, T.StructType):
                return [
                    (subfield.name, subfield.dataType)
                    for subfield in field.dataType.elementType["M"].dataType.fields
                ]
            else:
                return []
        elif isinstance(field.dataType, T.StructType):
            current_schema = field.dataType
        else:
            return []
    return []


def generate_transform_expr(array_path, fields):
    """
    Genera dinámicamente una expresión TRANSFORM de Spark SQL para mapear estructuras anidadas en arrays.

    :param array_path: Ruta de la columna array dentro del DataFrame.
    :param fields: Lista de nombres de los campos a extraer dentro de la estructura.
    :return: Expresión F.expr() con la transformación.
    """
    def get_suffix(data_type: T.DataType | None):
        """
        Retorna el sufijo DynamoDB (.S, .N, .BOOL) según el tipo de dato.
        Si el tipo de dato es None, se asume como '.S' por defecto.
        """
        if data_type is None:
            return ".S"  
        elif isinstance(data_type, T.StringType):
            return ".S"
        elif isinstance(data_type, T.BooleanType):
            return ".BOOL"
        elif isinstance(data_type, (T.IntegerType, T.LongType, T.DoubleType, T.FloatType, T.ShortType, T.DecimalType)):
            return ".N"
        else:
            return ""  

    struct_fields = ", ".join([f"'{field}', x.M.{field}{get_suffix(data_type)}" for field, data_type in fields])
    return F.expr(f"TRANSFORM({array_path}, x -> named_struct({struct_fields}))")


def get_path_list(column_name_str):
    """
    Recibe el path de una columna de esta forma:
    Column<'multialias(CAST(cardInfo.M.virtualIssueDate.N AS STRING) AS card_info_virtual_issue_date)'>
    y devuelve cardInfo.M.virtualIssueDate.N"""
    column_name_str = column_name_str.replace("Column<'multialias(CAST(", "")
    column_name_str = column_name_str.replace("Column<'alias(CAST(", "")
    column_name_str = column_name_str.split(" AS STRING")[0]
    return column_name_str


def flatten_dynamodb_struct(df: DataFrame, parent_col="Error"):
    """
    Aplana estructuras anidadas de un DataFrame PySpark que sigue el formato de DynamoDB.

    :param df: DataFrame de PySpark con estructuras anidadas de DynamoDB.
    :param parent_col: Nombre de la columna raíz que contiene los datos (por defecto, 'Item').
    :return: Lista de columnas con alias formateados.
    """
    df = df.select(F.col(f"{parent_col}.*"))

    flat_cols = []
    array_cols = []

    def recurse_schema(schema, prefix=""):
        for field in schema.fields:
            field_name = field.name
            full_path = f"{prefix}.{field_name}" if prefix else field_name
            full_path_parts = full_path.split(".")
            alias_name = "_".join(
                [part for part in full_path_parts if part not in dynamodb_types]
            )
            alias_name = pattern.sub("_", alias_name).lower()

            if isinstance(field.dataType, T.StructType):
                recurse_schema(field.dataType, full_path)
            elif isinstance(field.dataType, T.ArrayType):
                if isinstance(field.dataType.elementType, T.StructType):
                    array_cols.append((full_path, alias_name))
                else:
                    flat_cols.append(F.col(full_path).alias(alias_name))
            else:
                flat_cols.append(
                    F.col(full_path).cast(T.StringType()).alias(alias_name)
                )

    recurse_schema(df.schema, "")

    # flat_cols_str = [str(col.name()).split(" AS ")[2] for col in flat_cols]
    # Extraer los alias de las columnas de forma más robusta
    flat_cols_str = []
    for col in flat_cols:
        col_str = str(col.name())
        # Intentar extraer el alias desde diferentes formatos posibles
        if " AS " in col_str:
            parts = col_str.split(" AS ")
            # Tomar el último elemento después de " AS " como alias
            alias = parts[-1] if parts else col_str
            # Limpiar el alias removiendo caracteres especiales del formato de columna
            alias = alias.replace(")'>", "").replace("'", "").strip()
            flat_cols_str.append(alias)
        else:
            # Si no hay " AS ", usar el nombre completo limpiado
            alias = col_str.replace("Column<", "").replace(">", "").replace("'", "").strip()
            flat_cols_str.append(alias)
    
    # Buscamos duplicados
    seen = set()
    duplicates = set(x for x in flat_cols_str if x in seen or seen.add(x))

    for duplicate in duplicates:
        column_alias = duplicate.replace(")'>", "")
        path_list = {"alias": column_alias, "path_list": []}

        print(f"Columna duplicada: {column_alias}")
        # Quitamos los duplicados
        index = (
            0  # Se usa index en vez de remove al array por limitaciones de compilador
        )

        for col in flat_cols.copy():
            # print(str(col.name()))
            if column_alias in str(col.name()):
                if "NULL" in str(col.name()):
                    flat_cols.pop(index)
                else:
                    path_list["path_list"].append(get_path_list(str(col.name())))

            index += 1
        if len(path_list["path_list"]) > 1:
            print(f"Columna duplicada no nula: {column_alias}, {path_list}")
            cleaned_column = F.coalesce(
                *[F.col(path).cast(T.StringType()) for path in path_list["path_list"]],
            ).alias(path_list["alias"])

            flat_cols = [col for col in flat_cols if get_path_list(str(col.name())) not in path_list["path_list"]]

            flat_cols.append(cleaned_column)

    # Aplicar transformación dinámica a listas estructuradas
    for array_path, alias_name in array_cols:
        extracted_fields = extract_fields_from_array(df.schema, array_path)
        if extracted_fields:
            flat_cols.append(
                generate_transform_expr(array_path, extracted_fields).alias(alias_name)
            )

    return df.select(*flat_cols)
