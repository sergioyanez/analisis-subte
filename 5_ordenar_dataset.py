import dask.dataframe as dd
from dask.diagnostics import ProgressBar
import shutil
import dask
import dask.dataframe as dd
from dask.diagnostics import ProgressBar
import shutil
from pathlib import Path # <-- Asegúrate de importar Path

# --- INICIO DE LA CORRECCIÓN ---
# Configurar Dask para que use un directorio temporal en el disco externo.
# Esto evita que se llene el disco principal del sistema (/tmp).
temp_dir = Path("data/dask-worker-space")
temp_dir.mkdir(parents=True, exist_ok=True)

dask.config.set({"temporary_directory": str(temp_dir)})
print(f"🔧 Dask usará el directorio temporal: {temp_dir.resolve()}")
# --- FIN DE LA CORRECCIÓN ---


# El resto de tu código continúa igual que antes...
#################################################################################
# ...
#################################################################################
#################################################################################

# El dataset ya tiene un orden excelente gracias a la partición por año y línea, lo cual es fundamental para que las consultas sean rápidas.
# Sin embargo, para una analítica aún más eficiente, especialmente con series de tiempo,
# el siguiente paso recomendado es ordenar los datos por fecha y establecer esa columna como el índice del dataset.


# Ordenar por Fecha (Set Index) 🗓️
# Dentro de cada archivo Parquet, las filas no están necesariamente en orden cronológico.
# Ordenarlas por la columna fecha y establecerla como el "índice" del dataset le da a Dask un superpoder.
#
# Consultas por Rango de Fechas Ultrarrápidas: Si pides "todos los viajes de la primera semana de mayo",
# Dask sabrá exactamente en qué parte de los archivos empezar y terminar de leer.
#
# Análisis de Series de Tiempo Eficiente: Operaciones como calcular promedios móviles (rolling averages)
# o re-muestrear los datos por semana o mes se vuelven mucho más rápidas.


# historicos_parquet_ordenado
# será la versión "dorada" de éstos datos, perfectamente preparada para cualquier tipo de análisis que se quiera realizar.

#################################################################################
#################################################################################

# Rutas de entrada (el dataset simple) y de salida (el nuevo dataset ordenado)
PARQUET_IN = "data/dataset_subtes/historicos_parquet_simple"
PARQUET_OUT = "data/dataset_subtes/historicos_parquet_ordenado"

# Borra el destino anterior si existe
try:
    shutil.rmtree(PARQUET_OUT)
    print(f"🧹 Carpeta de destino anterior eliminada: {PARQUET_OUT}")
except FileNotFoundError:
    pass

# Carga el dataset simplificado
print(f"Cargando dataset desde: {PARQUET_IN}")
ddf = dd.read_parquet(PARQUET_IN)

# --- El Paso Clave: Ordenar por fecha y establecerlo como índice ---
print("\nOrdenando el dataset por fecha y estableciendo el índice...")
# set_index realiza una clasificación masiva de los datos. Es una operación pesada.
ddf_ordenado = ddf.set_index('fecha')

# Guarda el nuevo dataset ordenado e indexado
print(f"\nGuardando dataset ordenado en: {PARQUET_OUT}")
with ProgressBar():
    # Al guardar un DataFrame con índice, Dask gestiona la estructura de archivos
    # de forma optimizada para ese índice.
    ddf_ordenado.to_parquet(
        PARQUET_OUT,
        engine="pyarrow",
        compression="zstd",
        overwrite=True
    )

print("\n✅ Proceso de ordenamiento completado.")
print(f"Tu dataset final y optimizado para análisis está en: {PARQUET_OUT}")