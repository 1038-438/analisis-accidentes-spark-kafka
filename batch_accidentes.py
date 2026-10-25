from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    count,
    round as spark_round,
    to_date,
    trim,
    upper,
    when
)


# ---------------------------------------------------------
# 1. Rutas del proyecto
# ---------------------------------------------------------

BASE = Path(__file__).resolve().parent

RUTA_DATOS = str(
    BASE / "data" / "accidentes.csv"
)

RUTA_RESULTADOS = str(
    BASE / "resultados"
)


# ---------------------------------------------------------
# 2. Crear la sesión de Spark
# ---------------------------------------------------------

spark = (
    SparkSession.builder
    .appName("AnalisisAccidentesTransitoColombia")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ---------------------------------------------------------
# 3. Cargar el archivo CSV
# ---------------------------------------------------------

print("\n==============================================")
print("CARGANDO EL CONJUNTO DE DATOS")
print("==============================================\n")

df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .option("encoding", "UTF-8")
    .csv(RUTA_DATOS)
)


# ---------------------------------------------------------
# 4. Exploración inicial
# ---------------------------------------------------------

cantidad_original = df.count()

print("Cantidad de registros originales:", cantidad_original)

print("\nColumnas encontradas:")
print(df.columns)

print("\nEsquema original:")
df.printSchema()

print("\nPrimeros registros:")
df.show(10, truncate=False)


# ---------------------------------------------------------
# 5. Normalizar los nombres de las columnas
# ---------------------------------------------------------

for nombre_original in df.columns:

    nombre_nuevo = (
        nombre_original
        .strip()
        .lower()
        .replace(" ", "_")
        .replace("-", "_")
    )

    if nombre_nuevo != nombre_original:
        df = df.withColumnRenamed(
            nombre_original,
            nombre_nuevo
        )

print("\nColumnas normalizadas:")
print(df.columns)


# ---------------------------------------------------------
# 6. Limpiar campos de texto
# ---------------------------------------------------------

columnas_texto = [
    "marca_vehiculo",
    "tipo_vehiculo",
    "gravedad_accidente",
    "departamento_accidente",
    "municipio_accidente",
    "autoridad_de_transito"
]

for columna in columnas_texto:

    if columna in df.columns:

        df = df.withColumn(
            columna,
            upper(trim(col(columna)))
        )


# ---------------------------------------------------------
# 7. Convertir tipos de datos
# ---------------------------------------------------------

if "modelo_vehiculo" in df.columns:

    df = df.withColumn(
        "modelo_vehiculo",
        col("modelo_vehiculo").cast("int")
    )


if "edad_vehiculo" in df.columns:

    df = df.withColumn(
        "edad_vehiculo",
        col("edad_vehiculo").cast("int")
    )


if "fecha_accidente" in df.columns:

    df = df.withColumn(
        "fecha_accidente_convertida",
        to_date(
            col("fecha_accidente"),
            "MM/yyyy"
        )
    )


# ---------------------------------------------------------
# 8. Eliminar duplicados y valores inválidos
# ---------------------------------------------------------

df_limpio = df.dropDuplicates()

df_limpio = df_limpio.filter(
    col("departamento_accidente").isNotNull()
)

df_limpio = df_limpio.filter(
    col("tipo_vehiculo").isNotNull()
)

df_limpio = df_limpio.filter(
    col("gravedad_accidente").isNotNull()
)

df_limpio = df_limpio.filter(
    col("gravedad_accidente").isin(
        "CON HERIDOS",
        "CON MUERTOS"
    )
)

df_limpio = df_limpio.filter(
    col("edad_vehiculo").isNull()
    | (
        (col("edad_vehiculo") >= 0)
        & (col("edad_vehiculo") <= 100)
    )
)


# ---------------------------------------------------------
# 9. Crear indicador de accidente fatal
# ---------------------------------------------------------

df_limpio = df_limpio.withColumn(
    "es_fatal",
    when(
        col("gravedad_accidente") == "CON MUERTOS",
        1
    ).otherwise(0)
)

df_limpio.cache()

cantidad_limpia = df_limpio.count()

print("\n==============================================")
print("RESULTADO DE LA LIMPIEZA")
print("==============================================")

print("Registros originales:", cantidad_original)
print("Registros después de la limpieza:", cantidad_limpia)
print(
    "Registros eliminados:",
    cantidad_original - cantidad_limpia
)


# ---------------------------------------------------------
# 10. Revisar valores nulos
# ---------------------------------------------------------

print("\nValores nulos por columna:")

expresiones_nulos = []

for columna in df_limpio.columns:

    expresion = count(
        when(
            col(columna).isNull(),
            columna
        )
    ).alias(columna)

    expresiones_nulos.append(expresion)

df_limpio.select(
    expresiones_nulos
).show(truncate=False)


# ---------------------------------------------------------
# 11. Análisis por gravedad
# ---------------------------------------------------------

resultado_gravedad = (
    df_limpio
    .groupBy("gravedad_accidente")
    .count()
    .orderBy(col("count").desc())
)

print("\nAccidentes según su gravedad:")

resultado_gravedad.show(
    truncate=False
)


# ---------------------------------------------------------
# 12. Análisis por departamento
# ---------------------------------------------------------

resultado_departamentos = (
    df_limpio
    .groupBy("departamento_accidente")
    .count()
    .orderBy(col("count").desc())
    .limit(15)
)

print("\nDepartamentos con más registros:")

resultado_departamentos.show(
    15,
    truncate=False
)


# ---------------------------------------------------------
# 13. Análisis por municipio
# ---------------------------------------------------------

resultado_municipios = (
    df_limpio
    .groupBy(
        "departamento_accidente",
        "municipio_accidente"
    )
    .count()
    .orderBy(col("count").desc())
    .limit(15)
)

print("\nMunicipios con más registros:")

resultado_municipios.show(
    15,
    truncate=False
)


# ---------------------------------------------------------
# 14. Análisis por tipo de vehículo
# ---------------------------------------------------------

resultado_vehiculos = (
    df_limpio
    .groupBy("tipo_vehiculo")
    .agg(
        count("*").alias("cantidad"),
        spark_round(
            avg("edad_vehiculo"),
            2
        ).alias("edad_promedio")
    )
    .orderBy(col("cantidad").desc())
    .limit(15)
)

print("\nTipos de vehículos más involucrados:")

resultado_vehiculos.show(
    15,
    truncate=False
)


# ---------------------------------------------------------
# 15. Porcentaje fatal por departamento
# ---------------------------------------------------------

resultado_fatalidad = (
    df_limpio
    .groupBy("departamento_accidente")
    .agg(
        count("*").alias("total_accidentes"),
        spark_round(
            avg("es_fatal") * 100,
            2
        ).alias("porcentaje_fatal")
    )
    .filter(
        col("total_accidentes") >= 100
    )
    .orderBy(
        col("porcentaje_fatal").desc()
    )
    .limit(15)
)

print("\nPorcentaje de registros fatales por departamento:")

resultado_fatalidad.show(
    15,
    truncate=False
)


# ---------------------------------------------------------
# 16. Guardar resultados como CSV
# ---------------------------------------------------------

resultados = [
    (
        "gravedad",
        resultado_gravedad
    ),
    (
        "departamentos",
        resultado_departamentos
    ),
    (
        "municipios",
        resultado_municipios
    ),
    (
        "tipos_vehiculo",
        resultado_vehiculos
    ),
    (
        "fatalidad_departamento",
        resultado_fatalidad
    )
]

for nombre, resultado in resultados:

    ruta_salida = (
        RUTA_RESULTADOS
        + "/"
        + nombre
    )

    (
        resultado
        .coalesce(1)
        .write
        .mode("overwrite")
        .option("header", True)
        .csv(ruta_salida)
    )


# ---------------------------------------------------------
# 17. Guardar el conjunto limpio en formato Parquet
# ---------------------------------------------------------

ruta_parquet = (
    RUTA_RESULTADOS
    + "/datos_limpios_parquet"
)

(
    df_limpio
    .write
    .mode("overwrite")
    .parquet(ruta_parquet)
)


print("\n==============================================")
print("PROCESAMIENTO FINALIZADO")
print("==============================================")

print(
    "Los resultados quedaron guardados en:",
    RUTA_RESULTADOS
)

spark.stop()
