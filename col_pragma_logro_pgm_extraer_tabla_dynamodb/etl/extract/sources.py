from pyspark.sql import DataFrame
from addons.pyspark_utils.sources import Sources


class RawSources(Sources):
    """Extension de la clase sources para el job de Ingesta tabla Dynamo
    Los DataFrames se crean dinámicamente desde el YAML usando el schema_name.
    """
    # Los atributos de DataFrame se crean dinámicamente en Sources.__init__()
    # basado en el schema_name del YAML, no es necesario declararlos aquí
