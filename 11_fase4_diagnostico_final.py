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
def es_numerico(valor):
    """Verifica si un valor es numérico, incluso si está almacenado como string"""
    try:
        # Intenta convertir a float
        float(valor)
        return True
    except (ValueError, TypeError):
        return False

nombres = []
numeros = []

for v in valores_unicos:
    if es_numerico(v):
        numeros.append(v)
    else:
        nombres.append(v)

print(f"\nResumen del Diagnóstico:")
print(f"  - Se encontraron {len(nombres)} nombres de estación de texto")
if len(nombres) > 0:
    print(f"    Ejemplos: {nombres[:5]}")
print(f"  - Se encontraron {len(numeros)} valores numéricos que deberían ser nombres")
if len(numeros) > 0:
    print(f"    Ejemplos: {numeros[:10]}")

# Análisis adicional de los valores numéricos
if len(numeros) > 0:
    print(f"\n📊 Análisis de valores numéricos:")
    numeros_float = [float(n) for n in numeros]
    print(f"  - Rango: {min(numeros_float):.1f} - {max(numeros_float):.1f}")
    print(f"  - Total de valores únicos numéricos: {len(set(numeros_float))}")

    # Mostrar algunos ejemplos de conversión
    print(f"\n🔍 Ejemplos de conversión:")
    for i, num in enumerate(numeros[:10]):
        print(f"  '{num}' → {float(num)}")

# Mostrar el porcentaje de cada tipo
total = len(valores_unicos)
pct_nombres = (len(nombres) / total) * 100
pct_numeros = (len(numeros) / total) * 100

print(f"\n📈 Distribución:")
print(f"  - Nombres de texto: {len(nombres)} ({pct_nombres:.1f}%)")
print(f"  - Valores numéricos: {len(numeros)} ({pct_numeros:.1f}%)")
