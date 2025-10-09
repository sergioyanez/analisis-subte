import dask
import dask.dataframe as dd
from dask.diagnostics import ProgressBar


def fase2_analisis_de_hipotesis(parquet_path: str):
    """
    Filtra el dataset para los grupos definidos por la Hipótesis Alternativa (HA)
    y la Hipótesis Nula (H0), y calcula el promedio de pasajeros para cada uno.
    """
    try:
        print(f"📊 Cargando dataset final desde: {parquet_path}")
        ddf = dd.read_parquet(parquet_path)
    except FileNotFoundError:
        print(f"❌ ERROR: No se encontró el dataset en '{parquet_path}'.")
        print("   Asegúrate de haber ejecutado todos los scripts de procesamiento primero.")
        return

    # --- Grupo HA: Hipótesis Alternativa ---
    # Este grupo representa el "efecto" que queremos probar: la máxima concentración.
    print("\n1. Filtrando Grupo HA (Hipótesis Alternativa)...")
    condicion_ha = (
            (ddf["tipo_dia"] == "Día Hábil") &
            (ddf["franja_horaria"] == "Pico") &
            (ddf["ubicacion"] == "Céntrica")
    )
    ddf_ha = ddf[condicion_ha]

    # Se define la tarea para calcular el promedio de HA
    media_ha_task = ddf_ha["total"].mean()

    # --- Grupo H0: Hipótesis Nula ---
    # Este grupo representa la "línea de base" o el estado sin el efecto.
    print("2. Filtrando Grupo H0 (Hipótesis Nula)...")
    condicion_h0 = (
            (ddf["tipo_dia"] == "Fin de Semana/Feriado") &
            (ddf["ubicacion"] == "Céntrica")
    )
    ddf_h0 = ddf[condicion_h0]

    # Se define la tarea para calcular el promedio de H0
    media_h0_task = ddf_h0["total"].mean()

    # --- Computación y Resultados ---
    print("\n3. Calculando los promedios (esto puede tardar un momento)...")
    with ProgressBar():
        # dask.compute ejecuta ambas tareas en paralelo
        media_ha, media_h0 = dask.compute(media_ha_task, media_h0_task)

    print("\n------ Resultados de la Fase 2 ------")
    print(f"📈 Promedio de pasajeros para HA (Día Hábil, Pico, Céntrica): {media_ha:.2f}")
    print(f"📉 Promedio de pasajeros para H0 (Finde/Feriado, Céntrica): {media_h0:.2f}")

    return media_ha, media_h0


if __name__ == "__main__":
    # Ruta al dataset final con todas las características
    RUTA_FINAL = "data/dataset_subtes/final_con_features"

    # Llama a la función de la Fase 2
    fase2_analisis_de_hipotesis(RUTA_FINAL)