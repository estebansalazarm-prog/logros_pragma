from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

def create_struct_alignment_lambda(new_schema: T.StructType, stored_schema: T.StructType):
    """
    Genera dinámicamente una función para alinear los campos de un struct dentro
    de un F.transform.

    Si un campo del stored_schema no existe en el new_schema, lo añade como NULL.

    Args:
        new_schema: Schema real del JSON (camelCase).
        stored_schema: Schema destino con alias (snake_case).

    Returns:
        Una función que recibe un struct `x` y retorna un nuevo F.struct(...)
        con los campos alineados al stored_schema.
    """
    
    # Pre-calculamos un mapa normalizado para búsquedas eficientes
    # Mapa: {nombre_normalizado: nombre_original}
    normalized_new_fields = {
        f.name.lower().replace("_", ""): f.name for f in new_schema.fields
    }

    def build_struct(x):
        exprs = []
        # La fuente de la verdad siempre es el schema destino (stored_schema)
        for stored_field in stored_schema.fields:
            alias = stored_field.name
            dtype = stored_field.dataType
            
            # Buscamos el campo correspondiente en el schema de entrada
            normalized_stored_name = alias.lower().replace("_", "")
            original_field_name = normalized_new_fields.get(normalized_stored_name)

            # Si el campo existe en el schema de entrada, lo procesamos
            if original_field_name:
                field_accessor = getattr(x, original_field_name)

                # La lógica de extracción .S, .N, .BOOL se mantiene
                if isinstance(dtype, T.StringType):
                    expr = field_accessor.S.alias(alias)
                elif isinstance(dtype, (T.LongType, T.IntegerType, T.ShortType, T.ByteType)):
                    expr = field_accessor.N.cast(dtype).alias(alias)
                elif isinstance(dtype, (T.DoubleType, T.FloatType)):
                    expr = field_accessor.N.cast(dtype).alias(alias)
                elif isinstance(dtype, T.TimestampType):
                    expr = field_accessor.N.cast(T.LongType()).cast(T.TimestampType()).alias(alias)
                elif isinstance(dtype, T.BooleanType):
                    expr = field_accessor.BOOL.alias(alias)
                else: # Fallback por si hay algún tipo no contemplado
                    expr = field_accessor.S.alias(alias)

            # Si el campo NO existe, lo creamos como nulo con el tipo correcto
            else:
                expr = F.lit(None).cast(dtype).alias(alias)

            exprs.append(expr)

        return F.struct(*exprs)

    return build_struct


def align_schema(df: DataFrame, stored_schema: T.StructType) -> DataFrame:
    def align_field(field_name, new_type, stored_type):
        """
        Recursivamente ajusta el tipo de datos de una columna, incluyendo estructuras anidadas y arreglos.
        """
        if isinstance(stored_type, T.StructType) and isinstance(new_type, T.StructType):
            # Manejar StructType anidados
            aligned_fields = []
            for stored_field in stored_type.fields:
                sub_field_name = f"{field_name}.{stored_field.name}"
                if stored_field.name in [f.name for f in new_type.fields]:
                    new_sub_type = next(f.dataType for f in new_type.fields if f.name == stored_field.name)
                    aligned_fields.append(align_field(sub_field_name, new_sub_type, stored_field.dataType))
                else:
                    aligned_fields.append(F.lit(None).cast(stored_field.dataType).alias(stored_field.name))
            return F.struct(*aligned_fields).alias(field_name)

        elif isinstance(stored_type, T.ArrayType) and isinstance(new_type, T.ArrayType):
            # Manejar ArrayType (si es un array de estructuras, llamamos a align_field recursivamente)
            if isinstance(stored_type.elementType, T.StructType) and isinstance(new_type.elementType, T.StructType):
                aligned_struct = create_struct_alignment_lambda(new_type.elementType, stored_type.elementType)
                return F.transform(F.col(field_name), aligned_struct).alias(field_name)
            
            elif isinstance(new_type.elementType, T.StringType) and isinstance(stored_type.elementType, T.StructType):
                
                target_struct_schema = stored_type.elementType
                print(f"Aplicando F.from_json a la columna array: {field_name}")
                
                # Usamos F.transform para aplicar F.from_json a cada elemento 'x' (string) del array
                return F.transform(
                    F.col(field_name),
                    lambda x: F.from_json(x, target_struct_schema)
                ).alias(field_name.split(".")[-1])
            
            else:
                return F.col(field_name).cast(stored_type).alias(field_name)
        else:
            return F.col(field_name).cast(stored_type).alias(field_name.split(".")[-1])

    # Obtener los nombres y tipos de columnas
    new_schema_fields = {field.name: field.dataType for field in df.schema.fields}
    stored_schema_fields = {field.name: field.dataType for field in stored_schema.fields}

    # Columnas comunes
    common_columns = list(set(new_schema_fields.keys()) & set(stored_schema_fields.keys()))

    # Columnas faltantes en el nuevo esquema (se agregarán como NULL)
    missing_columns = {col: dtype for col, dtype in stored_schema_fields.items() if col not in new_schema_fields}
    print("Columnas faltantes")
    print(missing_columns)

    # Alinear columnas existentes con tipos correctos
    aligned_cols = [align_field(col, new_schema_fields[col], stored_schema_fields[col]) for col in common_columns]

    # Agregar columnas faltantes con valores nulos
    for col, dtype in missing_columns.items():
        aligned_cols.append(F.lit(None).cast(dtype).alias(col))

    return df.select(*aligned_cols)

