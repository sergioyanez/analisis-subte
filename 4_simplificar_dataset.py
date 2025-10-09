import dask.dataframe as dd
from dask.diagnostics import ProgressBar
import shutil
#################################################################################
#################################################################################
# Se  eliminan las columnas pax_franq, pax_pagos, pax_pases_pagos,
# ya que la suma de ellas  está en la columna total todo queda en un parquet nuevo
# llamado historicos_parquet_simple
#################################################################################
#################################################################################
# Rutas de entrada (el dataset sin duplicados) y de salida (el nuevo dataset simple)
PARQUET_IN = "data/dataset_subtes/historicos_parquet_final"
PARQUET_OUT = "data/dataset_subtes/historicos_parquet_simple"

# Borra el destino anterior si existe para asegurar una escritura limpia
try:
    shutil.rmtree(PARQUET_OUT)
    print(f"🧹 Carpeta de destino anterior eliminada: {PARQUET_OUT}")
except FileNotFoundError:
    pass # Si no existe, no hacemos nada

# Carga el dataset que ya no tiene duplicados
print(f"Cargando dataset desde: {PARQUET_IN}")
ddf = dd.read_parquet(PARQUET_IN)

# Define las columnas que quieres eliminar
columnas_a_eliminar = ['pax_franq', 'pax_pagos', 'pax_pases_pagos']

print("\nColumnas originales:", ddf.columns.tolist())

# Elimina las columnas usando el método .drop()
print(f"\nEliminando las columnas: {columnas_a_eliminar}...")
ddf_simple = ddf.drop(columns=columnas_a_eliminar)

print("Columnas después de eliminar:", ddf_simple.columns.tolist())

# Guarda el nuevo dataset simplificado en la carpeta de salida
print(f"\nGuardando dataset simplificado en: {PARQUET_OUT}")
with ProgressBar():
    ddf_simple.to_parquet(
        PARQUET_OUT,
        engine="pyarrow",
        write_index=False,
        compression="zstd",
        partition_on=["year", "linea"],
        overwrite=True
    )

print("\n✅ Proceso de simplificación completado.")
