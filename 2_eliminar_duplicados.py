import dask.dataframe as dd
from dask.diagnostics import ProgressBar

# Rutas de entrada y salida
PARQUET_IN = "data/dataset_subtes/historicos_parquet_limpio"
PARQUET_OUT = "data/dataset_subtes/historicos_parquet_final"

print(f"Cargando dataset con duplicados desde: {PARQUET_IN}")
ddf = dd.read_parquet(PARQUET_IN)

# --- Paso 1: Eliminar los duplicados ---
print("Eliminando filas duplicadas...")
# drop_duplicates() crea un nuevo DataFrame sin las filas repetidas.
ddf_sin_duplicados = ddf.drop_duplicates()


# --- Paso 2: Guardar el nuevo dataset limpio ---
print(f"Guardando dataset final sin duplicados en: {PARQUET_OUT}")
with ProgressBar():
    ddf_sin_duplicados.to_parquet(
        PARQUET_OUT,
        engine="pyarrow",
        write_index=False,
        compression="zstd",
        partition_on=["year", "linea"],
        overwrite=True
    )

print("\n------ Verificación ------")
# --- Paso 3: Verificar los conteos (opcional pero recomendado) ---
with ProgressBar():
    conteo_antes = len(ddf)
    conteo_despues = len(ddf_sin_duplicados)

print(f"Registros antes de eliminar duplicados: {conteo_antes:,}")
print(f"Registros después de eliminar duplicados: {conteo_despues:,}")
print(f"Total de filas duplicadas eliminadas: {(conteo_antes - conteo_despues):,}")

print("\n✅ Proceso de limpieza final completado.")