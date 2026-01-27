# Job Glue: col_pragma_logro_pgm_extraer_tabla_dynamodb

## 📋 Tabla de Contenidos
- [Descripción General](#descripción-general)
- [Arquitectura del Job](#arquitectura-del-job)
- [Configuración YAML](#configuración-yaml)
- [Estructura del Código](#estructura-del-código)
- [Flujo de Ejecución](#flujo-de-ejecución)
- [Agregar Nueva Tabla](#agregar-nueva-tabla)
- [Troubleshooting](#troubleshooting)

## 📖 Descripción General

### ¿Qué hace este job?
Job **dinámico y configurable** que ingesta tablas de DynamoDB exportadas a S3 sin necesidad de modificar código. Toda la configuración se maneja mediante archivos YAML.

### Responsabilidades principales:
1. ✅ Lee exportaciones de DynamoDB desde S3 (formato JSON)
2. ✅ Aplana estructura DynamoDB (Item/NewImage → columnas planas)
3. ✅ Aplica transformaciones configuradas en YAML
4. ✅ Guarda en Analytics (Parquet) y Curated (Hudi)

### Modos de ejecución:
- **FULL**: Exportación completa de la tabla
- **INC**: Exportación incremental (CDC - Change Data Capture)

### Control de Versiones
| Versión | Descripción | Autor | Fecha |
|---------|-------------|-------|-------|
| 1.0 | Creación del job dinámico | Esteban Salazar y Oscar Vergara| 2025-01-30 |

## 🏗️ Arquitectura del Job

### Componentes principales:
```
col_pragma_logro_pgm_extraer_tabla_dynamodb
├── config/
│   ├── reports/              # 📄 Archivos YAML de configuración
│   │   ├── nombre_tabla.yml
│   ├── report_config.py      # Carga y parsea YAML
│   └── sources_dictionary.py # Vacío (legacy, no se usa)
├── etl/
│   ├── extract/
│   │   ├── get_sources_options.py  # Lee path S3 desde YAML
│   │   └── sources.py              # Carga DataFrames dinámicamente
│   ├── transform/
│   │   ├── raw_transformations.py  # Lógica de transformación
│   │   └── transformations.py      # Orquestador
│   └── load/
│       ├── metadata.py             # Agrega columnas de metadata
│       └── save.py                 # Guarda en Analytics/Curated
└── col_pragma_logro_pgm_extraer_tabla_dynamodb.py  # 🚀 Main
```

### Flujo de datos:
```
S3 Raw (JSON) → Flatten DynamoDB → Transformaciones → Analytics (Parquet) → Curated (Hudi)
```

## ⚙️ Configuración YAML

### Ubicación:
```
col_pragma_logro_pgm_extraer_tabla_dynamodb/config/reports/<nombre_tabla>.yml
```

### Estructura completa:
```yaml
process_metadata:
  process_id: 'extract_<tabla>'
  description: 'Descripción del proceso'
  owner: 'pragma'
  version: '1.0.0'

source_config:
  source_name: 'Nombre descriptivo de la fuente'
  connection_type: 's3_path'
  s3_details:
    path_template_inc: "s3://{raw_bucket}/path/incremental/..."
    path_template_full: "s3://{raw_bucket}/path/full/..."
    data_format: "json"

processing_config:
  schema_name: "col_<product_type>_<tabla>"
  key_columns:
    - "key"
    - "sortkey"
  order_by_column: "tstamp"
  partition_date: "created_at"
  precombine_key: "tstamp"

output_config:
  data_product: "<nombre_tabla>"
  product_type: "<tipo_producto>"
```

### Campos clave del YAML:

| Sección | Campo | Descripción | Ejemplo |
|---------|-------|-------------|---------|
| **source_config** | `path_template_inc` | Path S3 para incremental | `s3://.../incremental/...` |
| | `path_template_full` | Path S3 para full | `s3://.../full/...` |
| **processing_config** | `schema_name` | Nombre del schema (.schema.yml) | `col_pragma_tabla_final` |
| | `key_columns` | Columnas para filtro NOT NULL | `["key", "sortkey"]` |
| | `order_by_column` | Columna para window function | `"tstamp"` |
| | `partition_date` | Columna para particionar | `"created_at"` |
| | `precombine_key` | Columna para Hudi precombine | `"tstamp"` |
| **output_config** | `data_product` | Nombre de la tabla final | `"tabla_final"` |
| | `product_type` | Tipo de producto | `"pragma"` |

### Tablas configuradas actualmente:

| Tabla | YAML | Product Type | Proveedor |
|-------|------|--------------|-----------|
| tabla_final | nombre_tabla.yml | pragma | Pragma |


## 🔧 Estructura del Código

### 1. Main (col_pragma_logro_pgm_extraer_tabla_dynamodb.py)
```python
# 1. Carga configuración YAML
elt_config = load_etl_config_from_yaml(table_name=CONFIG_TABLE, job_config=job_config)

# 2. Crea catálogo con función personalizada
catalog = Catalog(glue_context, job_config, get_table_func=get_sources_options_report)

# 3. Carga sources dinámicamente
sources = RawSources(catalog, pre_load_sources)

# 4. Ejecuta transformaciones
transformations = Transformations(spark, job_config, sources)
df = transformations.get_main_table()

# 5. Agrega metadata y guarda
df_final = add_metadata(df, job_config)
save_analytics(df_final, job_config)
save_action(df_analytics, job_config.hudi_options, job_config.curated_table_path)
```

### 2. Carga de configuración (config/report_config.py)
```python
def load_etl_config_from_yaml(table_name, job_config):
    # Lee YAML y extrae data_product, precombine_key, product_type
    # Actualiza job_config.constants con estos valores
    # Reemplaza placeholders: {raw_bucket}, {process_year}, etc.
    # Retorna ETLTableConfig con toda la configuración
```

### 3. Extracción (etl/extract/get_sources_options.py)
```python
def get_sources_options_report(table_name, job_config):
    # Lee source_config del YAML
    # Selecciona path_template_inc o path_template_full según PROCESS_TYPE
    # Retorna S3File(bucket, prefix, origin, format)
```

### 4. Transformaciones (etl/transform/raw_transformations.py)
```python
def get_table_transformations(df_dinamic, job_config):
    # 1. Aplana estructura DynamoDB (Item o NewImage)
    # 2. Filtra NULL en key_columns
    # 3. Window function para último estado
    # 4. Genera columna 'id' (concatenación de key_columns)
    # 5. Alinea schema con .schema.yml
    # 6. Convierte timestamps
```

### 5. Carga (etl/load/save.py)
```python
# Analytics: Parquet, mode=overwrite
save_analytics(df, job_config)

# Curated: Hudi, mode=upsert
save_action(df, hudi_options, path)
```

## 🚀 Flujo de Ejecución

### Parámetros del Job

| Parámetro | Descripción | Valores | Ejemplo |
|-----------|-------------|---------|---------|
| ACCOUNT | Cuenta AWS | String | "123456789" |
| ENV | Ambiente de ejecución | dev, qa, pdn | "dev" |
| PROCESS_DATE | Fecha de proceso | YYYY-MM-DD | "2025-01-30" |
| PROCESS_TYPE | Tipo de proceso | FULL, INC | "INC" |
| CONFIG_TABLE | Nombre del archivo YAML (sin extensión) | String | "nombre_tabla" |

### Paso a paso:

#### 1️⃣ Carga de Configuración
```python
elt_config = load_etl_config_from_yaml(CONFIG_TABLE, job_config)
# - Lee config/reports/{CONFIG_TABLE}.yml
# - Extrae: data_product, precombine_key, product_type
# - Actualiza job_config.constants
# - Reemplaza placeholders: {raw_bucket}, {process_year}, {process_month}, etc.
```

#### 2️⃣ Extracción de Datos
```python
catalog = Catalog(glue_context, job_config, get_sources_options_report)
sources = RawSources(catalog, pre_load_sources)
# - get_sources_options_report() lee path S3 desde YAML
# - Selecciona path_template_inc o path_template_full
# - Carga DataFrame desde S3 (formato JSON)
```

#### 3️⃣ Transformaciones
```python
df = transformations.get_main_table()
# - flatten_dynamodb_struct(): Aplana Item/NewImage
# - Filtra NULL en key_columns (del YAML)
# - Window function: partition_by(key_columns).orderBy(order_by_column)
# - Genera 'id': concat_ws("-", key_columns)
# - align_schema(): Alinea con schema_name.schema.yml
# - Convierte timestamps (partition_date)
```

#### 4️⃣ Metadata y Carga
```python
df_final = add_metadata(df, job_config)
# - Agrega: momento_ingestion, job_process_date, year, month, day

save_analytics(df_final, job_config)
# - Formato: Parquet
# - Modo: overwrite
# - Path: s3://.../analytics/.../co_{product_type}_{data_product}/

save_action(df_analytics, hudi_options, curated_table_path)
# - Formato: Hudi
# - Modo: upsert
# - Primary key: id
# - Precombine key: del YAML
# - Path: s3://.../curated/.../co_{product_type}_{data_product}/
```

### Diagrama visual:
```
┌──────────────────────────────────────────────────────────┐
│  1. Cargar YAML (CONFIG_TABLE)                           │
│     └─> Actualiza job_config.constants                   │
└────────────────────┬─────────────────────────────────────┘
                     ▼
┌──────────────────────────────────────────────────────────┐
│  2. Leer S3 (path_template_inc/full)                     │
│     └─> DataFrame JSON (DynamoDB export)                 │
└────────────────────┬─────────────────────────────────────┘
                     ▼
┌──────────────────────────────────────────────────────────┐
│  3. Transformaciones                                      │
│     ├─> Flatten (Item/NewImage)                          │
│     ├─> Filter (key_columns NOT NULL)                    │
│     ├─> Window (último estado)                           │
│     ├─> Generar 'id'                                     │
│     ├─> Align schema                                     │
│     └─> Convert timestamps                               │
└────────────────────┬─────────────────────────────────────┘
                     ▼
┌──────────────────────────────────────────────────────────┐
│  4. Metadata + Particiones                               │
│     └─> year, month, day, momento_ingestion              │
└────────────────────┬─────────────────────────────────────┘
                     ▼
┌──────────────────────────────────────────────────────────┐
│  5. Save Analytics (Parquet, overwrite)                  │
└────────────────────┬─────────────────────────────────────┘
                     ▼
┌──────────────────────────────────────────────────────────┐
│  6. Save Curated (Hudi, upsert)                          │
└──────────────────────────────────────────────────────────┘
```

### Rutas de datos:

| Zona | Path | Formato | Modo |
|------|------|---------|------|
| **Raw** | `s3://{BUCKET}/{PREFIX}/{process_type}/year={year}/month={month}/day={day}/[hour={hour}/]AWSDynamoDB/[*/]data` | JSON | Read |
| **Curated** | `s3://{BUCKET}/{PREFIX}/` | Hudi | Upsert |

## ➕ Agregar Nueva Tabla

### Checklist completo:

#### ✅ Paso 1: Crear YAML de configuración
```bash
cd col_pragma_logro_pgm_extraer_tabla_dynamodb/config/reports/
cp nombre_tabla.yml nueva_tabla.yml
```

**Editar nueva_tabla.yml:**
```yaml
process_metadata:
  process_id: 'extract_nueva_tabla'
  description: 'Descripción de la nueva tabla'
  owner: 'pragma'

source_config:
  source_name: 'Nombre descriptivo'
  s3_details:
    path_template_inc: "s3://{raw_bucket}/.../nueva_tabla/incremental/..."
    path_template_full: "s3://{raw_bucket}/.../nueva_tabla/full/..."

processing_config:
  schema_name: "co_{product_type}_nueva_tabla"  # ⚠️ Debe coincidir con .schema.yml
  key_columns: ["key", "sortkey"]  # Columnas PK de DynamoDB
  order_by_column: "tstamp"  # Para window function
  partition_date: "created_at"  # Para particiones year/month/day
  precombine_key: "tstamp"  # Para Hudi

output_config:
  data_product: "nueva_tabla"
  product_type: "pragma" 
```

#### ✅ Paso 2: Crear schema YAML
```bash
cd addons/config/schemas/
cp col_pragma_tabla_final.schema.yml col_{product_type}_nueva_tabla.schema.yml
```

**Editar schema:**
- Definir todas las columnas con sus tipos
- Incluir columnas de metadata: `id`, `momento_ingestion`, `job_process_date`
- Formato: Lista de diccionarios con `Name`, `Type`, `Nullable`

#### ✅ Paso 3: Actualizar MANIFEST.in
```bash
cd glue/
vim MANIFEST.in
```
Agregar:
```
include addons/config/schemas/co_{product_type}_nueva_tabla.schema.yml
include col_pragma_logro_pgm_extraer_tabla_dynamodb/config/reports/nueva_tabla.yml
```

#### ✅ Paso 4: Probar localmente
```bash
spark-submit col_pragma_logro_pgm_extraer_tabla_dynamodb.py \
  --ACCOUNT=123456789 \
  --ENV=dev \
  --PROCESS_DATE=2025-01-30 \
  --PROCESS_TYPE=FULL \
  --CONFIG_TABLE=nueva_tabla
```

#### ✅ Paso 5: Crear tests
```bash
cd tests/
cp test_srf_curado_extraer_tabla_dynamodb_full.py test_nueva_tabla_full.py
```

### ⚠️ Puntos críticos:

| Aspecto | Validación |
|---------|------------|
| **Nombre schema** | `processing_config.schema_name` debe coincidir con archivo `.schema.yml` |
| **Key columns** | Deben existir en el DataFrame después de flatten |
| **Order by column** | Debe ser numérico/timestamp para ordenar correctamente |
| **Partition date** | Debe ser timestamp para generar year/month/day |
| **Paths S3** | Verificar que existan exportaciones en esas rutas |

## 🐛 Troubleshooting

### Errores Comunes

| ERROR | DESCRIPCIÓN | POSIBLE SOLUCIÓN |
|-------|-------------|------------------|
| FileNotFoundError: Archivo de configuración no encontrado | El archivo YAML no existe en config/reports/ | Verificar que el archivo {CONFIG_TABLE}.yml exista en la ruta correcta |
| ValueError: No se pudo cargar la configuración ETL desde YAML | Error al parsear el YAML | Validar sintaxis del YAML, verificar que todos los campos requeridos estén presentes |
| AnalysisException: Column 'key' cannot be resolved | Las columnas en key_columns no existen en el DataFrame | Verificar que key_columns en el YAML coincidan con las columnas del schema después de aplanar |
| FileNotFoundException: Path does not exist | No se encuentra el archivo en S3 | Verificar que la exportación de DynamoDB se haya completado correctamente en la ruta configurada |
| Error de memoria insuficiente en Glue Job | Datos muy grandes para la configuración actual | Aumentar DPU del job o ajustar coalesce(8) en el código |
| Schema mismatch | El schema del archivo no coincide con el esperado | Verificar que el archivo .schema.yml esté actualizado con la estructura correcta |
| Hudi commit failed | Error al escribir en formato Hudi | Verificar permisos IAM, revisar logs de Hudi, validar que no haya conflictos de escritura concurrente |
| Timeout reading from S3 | Lectura de S3 excede el tiempo límite | Verificar conectividad de red, aumentar timeout, revisar tamaño de archivos |
| Invalid timestamp conversion | Error al convertir columnas de timestamp | Verificar que partition_date en YAML sea una columna válida y tenga formato correcto |
| Duplicate key error in Hudi | Registros duplicados con la misma primary key | Revisar lógica de window function, validar que order_by_column sea correcto |

### 📊 Monitoreo

**CloudWatch Logs:**
- Output: `/aws-glue/jobs/output`
- Error: `/aws-glue/jobs/error`

**Métricas clave:**
- Registros procesados: `df_final.count()` en logs
- Tiempo de ejecución: Consola de Glue
- Errores: CloudWatch

**Validaciones automáticas:**
- ✅ Datos vacíos → retorna sin error
- ✅ Filtros NOT NULL en key_columns
- ✅ Conversión de tipos con manejo de errores
- ✅ Alineación de schema (agrega columnas faltantes como NULL)

### 🔍 Debug tips

**Ver configuración cargada:**
```python
print(job_config.elt_config_table.processing_config.schema_name)
print(job_config.elt_config_table.source_config.s3_details.path_template_inc)
```

**Ver DataFrame después de flatten:**
```python
df_flattened.printSchema()
df_flattened.show(5, truncate=False)
```

**Ver columnas generadas:**
```python
print("Columnas después de align_schema:", df_aligned_schema.columns)
```
