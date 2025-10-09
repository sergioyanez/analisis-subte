import dask.dataframe as dd
import pandas as pd
import plotly.express as px
from dask.diagnostics import ProgressBar


def fase4_analisis_final(parquet_path: str):
    """
    Carga el dataset final y genera los análisis y visualizaciones
    para responder a las preguntas de investigación.
    """
    try:
        print(f"📊 Cargando dataset final desde: {parquet_path}")
        ddf = dd.read_parquet(parquet_path)
    except FileNotFoundError:
        print(f"❌ ERROR: No se encontró el dataset en '{parquet_path}'.")
        return

    # --- 1. Top 20 estaciones ---
    print("\n1. Calculando el total para TODAS las estaciones...")
    with ProgressBar():
        todas_las_estaciones = ddf.groupby('estacion')['total'].sum().compute()

    top_20_df = todas_las_estaciones.sort_values(ascending=False).head(20).reset_index()
    top_20_df['estacion'] = top_20_df['estacion'].astype(str)

    print("\n--- DATOS FINALES PARA EL GRÁFICO 'TOP 20' ---")
    print(top_20_df)
    print("---------------------------------------------\n")

    fig_top_estaciones = px.bar(
        top_20_df.sort_values(by='total', ascending=True),
        x='total',
        y='estacion',
        orientation='h',
        title='Top 20 Estaciones más Concurridas (Total Histórico)',
        labels={'total': 'Total de Pasajeros', 'estacion': 'Estación'}
    )
    fig_top_estaciones.show()

    # --- 2. Heatmap día × hora ---
    print("\n2. Generando mapa de calor por día y hora...")

    ddf['dia_semana'] = ddf.index.dt.day_name()
    ddf['hora_dia'] = dd.to_datetime(ddf['desde'], format="%H:%M:%S", errors='coerce').dt.hour

    with ProgressBar():
        heatmap_data = ddf.groupby([ddf['dia_semana'].astype('object'), 'hora_dia'])['total'].sum().compute()

    heatmap_pivot = heatmap_data.unstack().fillna(0)

    dias_ordenados = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    nombres_dias_es = {"Monday": "Lunes", "Tuesday": "Martes", "Wednesday": "Miércoles",
                       "Thursday": "Jueves", "Friday": "Viernes", "Saturday": "Sábado", "Sunday": "Domingo"}
    heatmap_pivot = heatmap_pivot.reindex(dias_ordenados).rename(index=nombres_dias_es)

    fig_heatmap = px.imshow(
        heatmap_pivot,
        title='Volumen Total de Pasajeros por Día de la Semana y Hora',
        labels={'x': 'Hora del Día', 'y': 'Día de la Semana', 'color': 'Total de Pasajeros'}
    )
    fig_heatmap.show()

    # --- INICIO DEL GRÁFICO FALTANTE ---
    # --- 3. Promedio general por tipo de día ---
    print("\n3. Calculando promedio de pasajeros por tipo de día (Todas las Estaciones)...")
    ddf['tipo_dia'] = ddf['tipo_dia'].astype('object')

    with ProgressBar():
        uso_por_tipo_dia = ddf.groupby('tipo_dia')['total'].mean().compute()

    print("\n------ Uso Promedio General por Tipo de Día ------")
    print(uso_por_tipo_dia.round(2))

    fig_tipo_dia = px.bar(
        uso_por_tipo_dia.reset_index(),
        x='tipo_dia',
        y='total',
        title='Promedio General de Pasajeros: Días Hábiles vs. Fines de Semana/Feriados',
        labels={'total': 'Promedio de Pasajeros', 'tipo_dia': 'Tipo de Día'},
        category_orders={'tipo_dia': ['Día Hábil', 'Fin de Semana/Feriado']} # Ordena las barras
    )
    fig_tipo_dia.show()
    # --- FIN DEL GRÁFICO FALTANTE ---

    # --- 4. Comparación detallada por ubicación y tipo de día ---
    print("\n4. Calculando promedio detallado por ubicación y tipo de día...")

    ddf['ubicacion'] = ddf['ubicacion'].astype('object')
    ddf['tipo_dia'] = ddf['tipo_dia'].astype('object')

    with ProgressBar():
        uso_detallado = ddf.groupby(['ubicacion', 'tipo_dia'])['total'].mean().compute().reset_index()

    print("\n------ Uso Promedio Detallado (Datos para el Gráfico) ------")
    print(uso_detallado.round(2))

    fig_detallado = px.bar(
        uso_detallado,
        x='ubicacion',
        y='total',
        color='tipo_dia',
        barmode='group',
        title='Promedio de Pasajeros por Ubicación y Tipo de Día',
        labels={'total': 'Promedio de Pasajeros', 'ubicacion': 'Ubicación', 'tipo_dia': 'Tipo de Día'}
    )
    fig_detallado.show()

    print("\n✅ Análisis final completado.")


if __name__ == "__main__":
    RUTA_FINAL = "data/dataset_subtes/final_con_features"
    fase4_analisis_final(RUTA_FINAL)