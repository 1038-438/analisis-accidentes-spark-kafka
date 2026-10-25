import argparse
import csv
import json
import time

from kafka import KafkaProducer


# ---------------------------------------------------------
# Argumentos del programa
# ---------------------------------------------------------

parser = argparse.ArgumentParser(
    description=(
        "Generador de accidentes para Kafka"
    )
)

parser.add_argument(
    "--archivo",
    default="data/accidentes.csv",
    help="Ruta del archivo CSV"
)

parser.add_argument(
    "--topic",
    default="accidentes_transito",
    help="Nombre del topic de Kafka"
)

parser.add_argument(
    "--intervalo",
    type=float,
    default=1.0,
    help="Segundos entre cada mensaje"
)

parser.add_argument(
    "--cantidad",
    type=int,
    default=100,
    help="Cantidad de registros por enviar"
)

args = parser.parse_args()


# ---------------------------------------------------------
# Conectar el productor con Kafka
# ---------------------------------------------------------

producer = KafkaProducer(
    bootstrap_servers=[
        "localhost:9092"
    ],
    value_serializer=lambda valor: json.dumps(
        valor,
        ensure_ascii=False
    ).encode("utf-8")
)


print(
    "Productor conectado con Kafka."
)

print(
    "Topic:",
    args.topic
)

print(
    "Archivo:",
    args.archivo
)

print(
    "Cantidad por enviar:",
    args.cantidad
)


# ---------------------------------------------------------
# Leer el CSV y enviar los eventos
# ---------------------------------------------------------

contador = 0

with open(
    args.archivo,
    encoding="utf-8-sig",
    newline=""
) as archivo_csv:

    lector = csv.DictReader(
        archivo_csv
    )

    for fila in lector:

        evento = {}

        for clave, valor in fila.items():

            nombre_normalizado = (
                clave
                .strip()
                .lower()
                .replace(" ", "_")
                .replace("-", "_")
            )

            if valor:

                evento[nombre_normalizado] = (
                    valor.strip()
                )

            else:

                evento[nombre_normalizado] = None

        evento["timestamp_ingesta"] = int(
            time.time()
        )

        producer.send(
            args.topic,
            value=evento
        )

        producer.flush()

        contador += 1

        print(
            "Evento",
            contador,
            "enviado:",
            evento
        )

        if contador >= args.cantidad:
            break

        time.sleep(
            args.intervalo
        )


producer.close()

print(
    "\nProceso finalizado."
)

print(
    "Total de eventos enviados:",
    contador
)
