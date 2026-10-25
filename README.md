Análisis de accidentes de tránsito con Spark y Kafka
Descripción
Este proyecto fue desarrollado como actividad académica para aplicar herramientas de Big Data sobre un conjunto de datos públicos de Colombia.

El proyecto analiza los vehículos involucrados en accidentes de tránsito mediante procesamiento batch con Apache Spark y simula la llegada de nuevos registros mediante Apache Kafka y Spark Structured Streaming.

Problema seleccionado
El problema consiste en identificar los departamentos, municipios y tipos de vehículos con mayor participación en accidentes de tránsito.

También se calcula la edad promedio de los vehículos y el porcentaje de registros asociados con accidentes con muertos.

Fuente de los datos
Portal Nacional de Datos Abiertos de Colombia.

Identificador del conjunto:

6jmc-vaxk

Dirección:

https://www.datos.gov.co/resource/6jmc-vaxk.csv

Tecnologías utilizadas
Ubuntu Linux
Python 3
Apache Spark
PySpark
Apache Kafka
Spark Structured Streaming
Pandas
Matplotlib
Git
GitHub
Estructura
batch_accidentes.py: procesamiento batch.
visualizar_resultados.py: creación de gráficas.
generador_kafka.py: envío de registros a Kafka.
streaming_accidentes.py: consumo de eventos con Spark.
requirements.txt: dependencias de Python.
.gitignore: archivos excluidos del repositorio.
Procesamiento batch
El procesamiento batch realiza:

Lectura del conjunto de datos.
Normalización de los nombres de las columnas.
Limpieza de campos de texto.
Conversión de tipos de datos.
Eliminación de registros duplicados.
Eliminación de edades inválidas.
Análisis por gravedad.
Análisis por departamento.
Análisis por municipio.
Análisis por tipo de vehículo.
Cálculo del porcentaje de registros fatales.
Exportación de resultados en CSV y Parquet.
Ejecución del procesamiento batch
Descargar el conjunto:

mkdir -p data

wget -O data/accidentes.csv \
'https://www.datos.gov.co/api/views/6jmc-vaxk/rows.csv?accessType=DOWNLOAD'
Ejecutar:

spark-submit batch_accidentes.py
Generar las gráficas:

python3 visualizar_resultados.py
Procesamiento en tiempo real
El productor lee registros del archivo CSV y los envía al topic:

accidentes_transito
El consumidor de Spark analiza los eventos en ventanas de un minuto.

Ejecutar el productor
python3 generador_kafka.py \
--intervalo 0.5 \
--cantidad 100
Ejecutar Spark Streaming
Se debe reemplazar VERSION_SPARK por la versión instalada:

spark-submit \
--packages \
org.apache.spark:spark-sql-kafka-0-10_2.12:VERSION_SPARK \
streaming_accidentes.py
Autor
Actividad académica universitaria.

Estudiante: Jorge Luis Hernández Matías
