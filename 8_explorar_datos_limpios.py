import dask
import dask.dataframe as dd
import pandas as pd
from dask.diagnostics import ProgressBar

# Configuramos pandas para que siempre muestre todas las columnas
pd.set_option('display.max_columns', None)


def generar_resumen(ddf: dd.DataFrame):
    """
    Calcula y muestra un resumen completo de la estructura y calidad
    de un Dask DataFrame.
    """
    print("\n------ Resumen del Dataset Final con Características Adicionales ------")

    with ProgressBar():
        # Definimos todas las tareas que queremos computar
        total_registros_task = ddf.shape[0]
        nulos_por_columna_task = ddf.isnull().sum()
        estadisticas_total_task = ddf["total"].describe()

        # Ejecutamos las tareas rápidas en paralelo
        (
            total_registros,
            nulos_por_columna,
            estadisticas_total,
        ) = dask.compute(
            total_registros_task,
            nulos_por_columna_task,
            estadisticas_total_task,
        )

    total_columnas = len(ddf.columns)

    # 1. Dimensiones
    print(f"\n1. Dimensiones:")
    print(f"   - Número total de registros: {total_registros:,}")
    print(f"   - Número total de columnas: {total_columnas}")

    # 2. Tipos de Datos
    print(f"\n2. Columnas y Tipos de Datos:")
    print(ddf.dtypes)

    # 3. Muestra de Datos
    print(f"\n3. Primeras 5 filas de muestra:")
    # mostramos también las nuevas columnas
    print(ddf.head()[['estacion', 'total', 'tipo_dia', 'franja_horaria', 'ubicacion']])

    # 4. Valores Nulos
    print(f"\n4. Conteo de Valores Nulos:")
    if nulos_por_columna.sum() > 0:
        print(nulos_por_columna[nulos_por_columna > 0])
    else:
        print("   ✅ No se encontraron valores nulos.")

    # 5. Estadísticas Descriptivas
    print("\n5. Estadísticas de la columna 'total':")
    print(estadisticas_total.round(2))


def main():
    """
    Función principal para cargar los datos y ejecutar el resumen.
    """
    # Apunta a la versión final con las nuevas características.
    PARQUET_PATH = "data/dataset_subtes/final_con_features"

    try:
        print(f"🔎 Cargando datos desde: {PARQUET_PATH}")
        ddf = dd.read_parquet(PARQUET_PATH)
        generar_resumen(ddf)
    except FileNotFoundError:
        print(f"❌ ERROR: No se encontró el directorio de datos en '{PARQUET_PATH}'.")
        print("   Asegúrate de haber ejecutado todos los scripts de procesamiento primero.")

    print("\n\nAnálisis de exploración completado.")


if __name__ == "__main__":
    main()