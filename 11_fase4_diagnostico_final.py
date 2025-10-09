import dask.dataframe as dd
from dask.diagnostics import ProgressBar

# Apunta a tu dataset final
PARQUET_PATH = "data/dataset_subtes/final_con_features"

print(f"🔎 Cargando dataset desde: {PARQUET_PATH}")
ddf = dd.read_parquet(PARQUET_PATH)

print("\nBuscando todos los valores únicos en la columna 'estacion'...")
print("Esto puede tardar un minuto, ya que debe leer todo el dataset.")

with ProgressBar():
    # .unique() nos dará una lista de cada "nombre" de estación que existe en toda la columna
    valores_unicos = ddf['estacion'].unique().compute()

print("\n------ Valores Únicos Encontrados en 'estacion' ------")
print(valores_unicos)
print("----------------------------------------------------")

# Contamos cuántos son de tipo texto y cuántos son numéricos
nombres = [v for v in valores_unicos if isinstance(v, str)]
numeros = [v for v in valores_unicos if not isinstance(v, str)]

print(f"\nResumen del Diagnóstico:")
print(f"  - Se encontraron {len(nombres)} nombres de estación (ej: '{nombres[0]}')")
print(f"  - Se encontraron {len(numeros)} valores numéricos que deberían ser nombres.")