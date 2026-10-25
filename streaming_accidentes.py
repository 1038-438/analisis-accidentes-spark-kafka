from pyspark.sql import SparkSession

from pyspark.sql.functions import (
    avg,
    col,
    count,
    from_json,
    from_unixtime,
    round as spark_round,
    to_timestamp,
    trim,
    upper,
    when,
    window
)

from pyspark.sql.types import (
    LongType,
    StringType,
    StructField,
    StructType
)


# ---------------------------------------------------------
# 1. Crear sesión de Spark
# ---------------------------------------------------------

spark = (
    SparkSession.builder
    .appName("StreamingAccidentesTransito")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ---------------------------------------------------------
# 2. Definir la estructura del JSON
# ---------------------------------------------------------

schema = StructType([
    StructField(
        "marca_vehiculo",
        StringType(),
        True
    ),
    StructField(
        "modelo_vehiculo",
        StringType(),
        True
    ),
    StructField(
        "tipo_vehiculo",
        StringType(),
        True
    ),
    StructField(
        "edad_vehiculo",
        StringType(),
        True
    ),
    StructField(
        "fecha_accidente",
        StringType(),
        True
    ),
    StructField(
        "gravedad_accidente",
        StringType(),
        True
    ),
    StructField(
        "departamento_accidente",
        StringType(),
        True
    ),
    StructField(
        "municipio_accidente",
        StringType(),
        True
    ),
    StructField(
        "autoridad_de_transito",
        StringType(),
        True
    ),
    StructField(
        "timestamp_ingesta",
        LongType(),
        True
    )
])


# ---------------------------------------------------------
# 3. Leer los mensajes del topic
# ---------------------------------------------------------

flujo_kafka = (
    spark.readStream
    .format("kafka")
    .option(
        "kafka.bootstrap.servers",
        "localhost:9092"
    )
    .option(
        "subscribe",
        "accidentes_transito"
    )
    .option(
        "startingOffsets",
        "latest"
    )
    .load()
)


# ---------------------------------------------------------
# 4. Convertir los mensajes JSON en columnas
# ---------------------------------------------------------

datos = (
    flujo_kafka
    .select(
        from_json(
            col("value").cast("string"),
            schema
        ).alias("datos")
    )
    .select("datos.*")
)


# ---------------------------------------------------------
# 5. Limpiar y transformar los datos
# ---------------------------------------------------------

datos_limpios = (
    datos
    .withColumn(
        "tipo_vehiculo",
        upper(
            trim(
                col("tipo_vehiculo")
            )
        )
    )
    .withColumn(
        "gravedad_accidente",
        upper(
            trim(
                col("gravedad_accidente")
            )
        )
    )
    .withColumn(
        "departamento_accidente",
        upper(
            trim(
                col("departamento_accidente")
            )
        )
    )
    .withColumn(
        "edad_vehiculo",
        col("edad_vehiculo").cast("int")
    )
    .withColumn(
        "evento_ts",
        to_timestamp(
            from_unixtime(
                col("timestamp_ingesta")
            )
        )
    )
    .filter(
        col(
            "departamento_accidente"
        ).isNotNull()
    )
    .filter(
        col(
            "tipo_vehiculo"
        ).isNotNull()
    )
)


# ---------------------------------------------------------
# 6. Aplicar marca de agua
# ---------------------------------------------------------

datos_con_watermark = (
    datos_limpios
    .withWatermark(
        "evento_ts",
        "2 minutes"
    )
)


# ---------------------------------------------------------
# 7. Agrupar por ventanas de un minuto
# ---------------------------------------------------------

resumen = (
    datos_con_watermark
    .groupBy(
        window(
            col("evento_ts"),
            "1 minute"
        ),
        "departamento_accidente",
        "tipo_vehiculo"
    )
    .agg(
        count("*").alias(
            "cantidad"
        ),
        spark_round(
            avg("edad_vehiculo"),
            2
        ).alias(
            "edad_promedio"
        ),
        spark_round(
            avg(
                when(
                    col("gravedad_accidente")
                    == "CON MUERTOS",
                    1
                ).otherwise(0)
            ) * 100,
            2
        ).alias(
            "porcentaje_fatal"
        )
    )
)


# ---------------------------------------------------------
# 8. Mostrar resultados en la consola
# ---------------------------------------------------------

consulta = (
    resumen.writeStream
    .outputMode("update")
    .format("console")
    .option(
        "truncate",
        False
    )
    .option(
        "numRows",
        50
    )
    .option(
        "checkpointLocation",
        "checkpoint/accidentes"
    )
    .start()
)


print(
    "Spark Streaming está esperando eventos de Kafka."
)

print(
    "Topic: accidentes_transito"
)


consulta.awaitTermination()
