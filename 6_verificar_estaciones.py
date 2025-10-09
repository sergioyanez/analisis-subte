import dask.dataframe as dd
import pandas as pd
from dask.diagnostics import ProgressBar
import re


def limpiar_nombre_estacion(nombre):
    """
    Función mejorada para estandarizar un único nombre de estación.
    """
    nombre = str(nombre).upper().strip()

    # Corrección específica para SAENZ PEÑA, más segura que la anterior
    if 'SAENZ' in nombre:
        return 'SAENZ PEÑA'
    # Corrección específica para AGÜERO
    if 'AG' in nombre and 'ERO' in nombre:
        return 'AGÜERO'

    # Eliminar puntos y letras de línea al final (B, D, H, E)
    nombre = nombre.replace('.', '')
    if nombre.endswith(('B', 'D', 'H', 'E')):
        nombre = nombre[:-1]

    return nombre


def main():
    """
    Script para obtener una lista única y LIMPIA de todas las estaciones.
    """
    PARQUET_PATH = "data/dataset_subtes/historicos_parquet_ordenado"
    print(f"🔎 Cargando datos desde: {PARQUET_PATH}")
    ddf = dd.read_parquet(PARQUET_PATH)

    print("\n[INFO] Calculando nombres únicos de estaciones... (Esto puede tardar un par de minutos)")
    with ProgressBar():
        lista_cruda = ddf['estacion'].unique().compute()

    print(f"\n[INFO] Se encontraron {len(lista_cruda)} valores únicos en crudo. Limpiando...")

    nombres_limpios = set()  # Usamos un 'set' para guardar automáticamente solo valores únicos

    for item in lista_cruda:
        # --- FILTRO PRINCIPAL ---
        # Nos quedamos solo con los strings que contengan al menos una letra.
        # Esto elimina todos los números ('0', '1.0', '245.0', etc.)
        if isinstance(item, str) and re.search('[A-Z]', item.upper()):
            nombre_corregido = limpiar_nombre_estacion(item)
            nombres_limpios.add(nombre_corregido)

    # Convertimos el set a una lista y la ordenamos para que sea fácil de leer
    lista_final_estaciones = sorted(list(nombres_limpios))

    print("\n------ Lista de Estaciones Únicas y LIMPIAS ------")
    print(lista_final_estaciones)
    print(f"\n✅ Se encontraron {len(lista_final_estaciones)} estaciones únicas después de la limpieza.")


if __name__ == "__main__":
    main()