from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd


BASE = Path(__file__).resolve().parent

RUTA_RESULTADOS = BASE / "resultados"


def leer_resultado(nombre_carpeta):

    carpeta = RUTA_RESULTADOS / nombre_carpeta

    archivos = list(
        carpeta.glob("part-*.csv")
    )

    if not archivos:

        raise FileNotFoundError(
            "No se encontró el CSV en: "
            + str(carpeta)
        )

    return pd.read_csv(archivos[0])


# ---------------------------------------------------------
# Gráfica de departamentos
# ---------------------------------------------------------

departamentos = leer_resultado(
    "departamentos"
)

departamentos = departamentos.sort_values(
    "count"
)

plt.figure(figsize=(11, 7))

plt.barh(
    departamentos["departamento_accidente"],
    departamentos["count"]
)

plt.title(
    "Departamentos con más vehículos involucrados"
)

plt.xlabel(
    "Cantidad de registros"
)

plt.ylabel(
    "Departamento"
)

plt.tight_layout()

plt.savefig(
    RUTA_RESULTADOS
    / "departamentos_top15.png",
    dpi=150
)

plt.close()


# ---------------------------------------------------------
# Gráfica de tipos de vehículos
# ---------------------------------------------------------

vehiculos = leer_resultado(
    "tipos_vehiculo"
)

vehiculos = vehiculos.sort_values(
    "cantidad"
)

plt.figure(figsize=(11, 7))

plt.barh(
    vehiculos["tipo_vehiculo"],
    vehiculos["cantidad"]
)

plt.title(
    "Tipos de vehículos más involucrados"
)

plt.xlabel(
    "Cantidad de registros"
)

plt.ylabel(
    "Tipo de vehículo"
)

plt.tight_layout()

plt.savefig(
    RUTA_RESULTADOS
    / "tipos_vehiculo_top15.png",
    dpi=150
)

plt.close()


# ---------------------------------------------------------
# Gráfica de gravedad
# ---------------------------------------------------------

gravedad = leer_resultado(
    "gravedad"
)

plt.figure(figsize=(8, 6))

plt.bar(
    gravedad["gravedad_accidente"],
    gravedad["count"]
)

plt.title(
    "Registros según la gravedad del accidente"
)

plt.xlabel(
    "Gravedad"
)

plt.ylabel(
    "Cantidad de registros"
)

plt.tight_layout()

plt.savefig(
    RUTA_RESULTADOS
    / "accidentes_por_gravedad.png",
    dpi=150
)

plt.close()


print(
    "Las gráficas fueron creadas correctamente."
)

print(
    "Ubicación:",
    RUTA_RESULTADOS
)
