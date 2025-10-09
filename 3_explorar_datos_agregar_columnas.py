# -*- coding: utf-8 -*-
"""
Script de Exploración y Transformación de Datos de Subtes.

Fase 1 (COMPLETA): Creación de 'tipo_dia', 'franja_horaria' y 'ubicacion'.
"""
import dask
import dask.dataframe as dd
import pandas as pd
import holidays
from dask.diagnostics import ProgressBar

# Configuramos pandas para que siempre muestre todas las columnas
pd.set_option('display.max_columns', None)


def generar_resumen(ddf: dd.DataFrame):
    """
    Calcula y muestra un resumen del Dask DataFrame, incluyendo las nuevas columnas.
    """
    print("\n------ Resumen del Dataset Final ------")

    with ProgressBar():
        tasks = {
            "total_registros": ddf.shape[0],
            "nulos_por_columna": ddf.isnull().sum(),
            "estadisticas_total": ddf["total"].describe(),
            "tipo_dia_counts": ddf['tipo_dia'].value_counts(),
            "franja_horaria_counts": ddf['franja_horaria'].value_counts(),
            "ubicacion_counts": ddf['ubicacion'].value_counts()
        }
        results = dask.compute(tasks)[0]

    total_columnas = len(ddf.columns)

    print(f"\n1. Dimensiones:")
    print(f"   - Número total de registros: {results['total_registros']:,}")
    print(f"   - Número total de columnas: {total_columnas}")
    print(f"\n2. Columnas y Tipos de Datos:")
    print(ddf.dtypes)
    print(f"\n3. Primeras 5 filas de muestra:")
    print(ddf.head())
    print(f"\n4. Conteo de Valores Nulos:")
    if results['nulos_por_columna'].sum() > 0:
        print(results['nulos_por_columna'][results['nulos_por_columna'] > 0])
    else:
        print("   ✅ No se encontraron valores nulos.")
    print("\n5. Estadísticas de la columna 'total':")
    print(results['estadisticas_total'].round(2))
    print("\n6. Conteo de registros por 'tipo_dia':")
    print(results['tipo_dia_counts'])
    print("\n7. Conteo de registros por 'franja_horaria':")
    print(results['franja_horaria_counts'])
    print("\n8. Conteo de registros por 'ubicacion':")
    print(results['ubicacion_counts'])


def main():
    """
    Función principal para cargar datos, crear variables y ejecutar el resumen.
    """
    PARQUET_PATH = "data/dataset_subtes/historicos_parquet_ordenado"

    try:
        print(f"🔎 Cargando datos desde: {PARQUET_PATH}")
        ddf = dd.read_parquet(PARQUET_PATH)
        ddf = ddf.set_index(ddf.index.astype('datetime64[ns]'))
    except FileNotFoundError:
        print(f"❌ ERROR: No se encontró el directorio de datos en '{PARQUET_PATH}'.")
        return

    print("\n🚧 Iniciando Fase 1: Creación de variables...")

    # --- Punto 1: 'tipo_dia' ---
    print("   - Creando la columna 'tipo_dia'...")
    min_year, max_year = dask.compute(ddf.index.year.min(), ddf.index.year.max())
    YEARS = range(int(min_year), int(max_year) + 1)
    feriados_set = set(holidays.AR(years=YEARS).keys())
    ddf = ddf.reset_index()
    condicion_finde_feriado = ddf['fecha'].isin(list(feriados_set)) | ddf['fecha'].dt.dayofweek.isin([5, 6])
    ddf['tipo_dia'] = 'Día Hábil'
    ddf['tipo_dia'] = ddf['tipo_dia'].mask(condicion_finde_feriado, 'Fin de Semana/Feriado')
    ddf = ddf.set_index('fecha')
    print("   - Columna 'tipo_dia' lista.")

    # --- Punto 2: 'franja_horaria' ---
    print("   - Creando la columna 'franja_horaria'...")
    hora_serie = dd.to_datetime(ddf['desde'], format='%H:%M:%S').dt.hour
    condicion_pico = ((hora_serie >= 7) & (hora_serie <= 9)) | ((hora_serie >= 17) & (hora_serie <= 19))
    ddf['franja_horaria'] = 'Valle'
    ddf['franja_horaria'] = ddf['franja_horaria'].mask(condicion_pico, 'Pico')
    print("   - Columna 'franja_horaria' lista.")

    # --- Punto 3: 'ubicacion' ---
    print("   - Creando la columna 'ubicacion'...")
    # Lista curada basada en tu selección y la limpieza de datos
    estaciones_centricas = [
        '9 DE JULIO', 'ALBERTI', 'ANGEL GALLARDO', 'CALLAO', 'CONGRESO',
        'FACULTAD DE DERECHO', 'FLORIDA', 'INDEPENDENCIA', 'LIMA', 'MALABIA',
        'ONCE', 'PASCO', 'PERU', 'PIEDRAS', 'PLAZA DE MAYO', 'PLAZA ITALIA',
        'PLAZA MISERERE', 'RETIRO', 'TRIBUNALES'
    ]

    # La limpieza de la columna 'estacion' se hace al vuelo
    ddf['estacion_limpia'] = ddf['estacion'].str.upper().str.strip()
    condicion_centrica = ddf['estacion_limpia'].isin(estaciones_centricas)

    ddf['ubicacion'] = 'Periférica'
    ddf['ubicacion'] = ddf['ubicacion'].mask(condicion_centrica, 'Céntrica')

    # Opcional: eliminar la columna de limpieza si no la necesitas más
    ddf = ddf.drop(columns=['estacion_limpia'])
    print("   - Columna 'ubicacion' lista.")

    print("\n✅ Fase 1 completada. Todas las variables han sido creadas.")

    generar_resumen(ddf)

    print("\n\nAnálisis de exploración y transformación completado.")


if __name__ == "__main__":
    main()