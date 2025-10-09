# -*- coding: utf-8 -*-
"""
Análisis de datos históricos de viajes en el Subte de Buenos Aires.

Este script realiza el siguiente proceso:
1.  Descarga y descomprime los datasets históricos de viajes (2014-2021).
2.  Procesa y normaliza los archivos CSV en un formato eficiente (Parquet),
    manejando diferentes esquemas y errores de formato.
3.  Analiza los datos procesados para calcular el total de usuarios por línea y período.
4.  Genera visualizaciones interactivas (líneas, heatmap, treemap) para explorar los resultados.
"""

# ==============================================================================
# 1. IMPORTACIONES Y CONFIGURACIÓN INICIAL
# ==============================================================================
import os
import shutil
import zipfile
import csv
import gc
import unicodedata
from pathlib import Path
from glob import glob

# Librerías de análisis y datos
import pandas as pd
import dask.dataframe as dd
import pyarrow as pa
import pyarrow.parquet as pq
from dask.diagnostics import ProgressBar

# Librerías de visualización
import plotly.express as px

# (Opcional) Descarga de datos
import gdown

# ==============================================================================
# 2. DEFINICIÓN DE CONSTANTES GLOBALES
# ==============================================================================
# --- Rutas del proyecto (relativas para portabilidad) ---
# Se asume que existe una carpeta 'data' en el mismo directorio que el script.
DATA_DIR = Path("data")
RAW_DATA_PATH = DATA_DIR / "dataset_subtes"
PARQUET_PATH = RAW_DATA_PATH / "historicos_parquet_limpio"

# --- Archivos a procesar ---
TARGET_CSVS = [
    "historico_2014.csv", "historico_2015.csv", "historico_2016.csv",
    "historico_2017.csv", "historico_2018.csv", "historico_2019.csv",
    "historico_2020.csv", "historico_2021.csv"
]

# --- Esquema final de columnas para los datos normalizados ---
FINAL_COLS = [
    'fecha', 'linea', 'estacion', 'desde', 'hasta', 'periodo',
    'pax_franq', 'pax_pagos', 'pax_pases_pagos', 'total'
]


# ==============================================================================
# 3. FUNCIONES DE PREPARACIÓN Y PROCESAMIENTO DE DATOS
# ==============================================================================

def descargar_y_extraer_zip(drive_id: str, output_dir: Path, zip_name: str):
    """
    Descarga y descomprime un archivo ZIP desde Google Drive si no existe.
    """
    if (output_dir / "historico_2014.csv").exists():
        print(f"✔️ Los datos ya existen en: {output_dir}")
        return True

    print(f"📥 Descargando datos en: {output_dir}...")
    try:
        output_dir.mkdir(parents=True, exist_ok=True)
        zip_path = output_dir / zip_name

        gdown.download(id=drive_id, output=str(zip_path), quiet=False)

        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(output_dir)

        os.remove(zip_path)
        print(f"✅ Descarga y extracción completadas.")
        return True
    except Exception as e:
        print(f"❌ Error durante la descarga: {e}")
        return False


def _normalizar_encabezados(cols: list) -> list:
    """Limpia y estandariza una lista de nombres de columnas."""
    s = (pd.Series(cols).astype(str)
         .str.replace('\ufeff', '', regex=False)  # BOM
         .str.strip().str.lower()
         .str.normalize('NFKD').str.encode('ascii', 'ignore').str.decode('ascii')
         .str.replace(r'[\s-]+', '_', regex=True))
    return list(s)


def _sanitizar_chunk(df: pd.DataFrame) -> pd.DataFrame:
    """Aplica un conjunto de reglas de limpieza a un chunk de datos."""
    df.columns = _normalizar_encabezados(df.columns)

    df = df.rename(columns={
        "pax_freq": "pax_franq", "pax_frec": "pax_franq",
        "pax_pago": "pax_pagos", "paxpagos": "pax_pagos",
        "pax_total": "total"
    })

    for col in FINAL_COLS:
        if col not in df.columns:
            df[col] = pd.NA

    # --- INICIO DE LA OPTIMIZACIÓN DE FECHAS ---
    s = df['fecha'].astype(str).str.strip()
    # Intenta convertir explícitamente los dos formatos más comunes.
    # Esto es mucho más rápido y eficiente en memoria que dejar que pandas adivine.
    f1 = pd.to_datetime(s, format='%Y-%m-%d', errors='coerce')
    f2 = pd.to_datetime(s, format='%d/%m/%Y', errors='coerce')
    df['fecha'] = f1.fillna(f2)
    # --- FIN DE LA OPTIMIZACIÓN DE FECHAS ---

    num_cols = ['pax_franq', 'pax_pagos', 'pax_pases_pagos', 'total']
    for c in num_cols:
        df[c] = pd.to_numeric(df[c], errors='coerce').astype('Int32')

    str_cols = ['desde', 'estacion', 'hasta', 'linea', 'periodo']
    for c in str_cols:
        df[c] = df[c].astype('string').str.replace(r'[\x00-\x1f\x7f]', '', regex=True).str.strip()

    if 'linea' in df.columns:
        df['linea'] = (df['linea'].str.upper()
                       .str.replace('LINEA', '', regex=False)
                       .str.replace(r'[^A-Z]', '', regex=True)
                       .str[-1:])
        valid_lines = set(list("ABCDEH"))
        df = df[df['linea'].isin(valid_lines)]

    if 'estacion' in df.columns:
        mask_totales = (
            df['estacion'].str.contains(r'\bTOTAL\b', case=False, na=False) |
            df['estacion'].str.contains(r'\bSUBTOTAL\b', case=False, na=False) |
            df['estacion'].str.fullmatch(r'\s*', na=False)
        )
        df = df[~mask_totales]

    return df[FINAL_COLS].dropna(how='all')


def procesar_csvs_a_parquet():
    """
    Versión optimizada que lee todos los CSVs con Dask para evitar picos de memoria,
    aplica la limpieza por particiones y guarda el resultado en Parquet.
    """
    if PARQUET_PATH.exists():
        print(f"✔️ El dataset Parquet ya existe en: {PARQUET_PATH}")
        return

    print(f"🛠️  Procesando CSVs a Parquet de forma eficiente con Dask...")

    csv_files = [str(RAW_DATA_PATH / filename) for filename in TARGET_CSVS]

    ddf = dd.read_csv(
        csv_files,
        dtype=str,
        encoding='latin-1',
        engine='python',
        on_bad_lines='skip',
        sep=None,
        # --- INICIO DE LA CORRECCIÓN ---
        # Forzamos a Dask a usar bloques más pequeños para reducir el uso de RAM.
        # Si esto falla, puedes probar un valor aún más bajo, como '16MB'.
        blocksize='32MB'
        # --- FIN DE LA CORRECCIÓN ---
    )

    meta_df = pd.DataFrame({
        'fecha': pd.Series([], dtype='datetime64[ns]'),
        'linea': pd.Series([], dtype='string'),
        'estacion': pd.Series([], dtype='string'),
        'desde': pd.Series([], dtype='string'),
        'hasta': pd.Series([], dtype='string'),
        'periodo': pd.Series([], dtype='string'),
        'pax_franq': pd.Series([], dtype='Int32'),
        'pax_pagos': pd.Series([], dtype='Int32'),
        'pax_pases_pagos': pd.Series([], dtype='Int32'),
        'total': pd.Series([], dtype='Int32')
    })
    meta_df = meta_df[FINAL_COLS]

    ddf_clean = ddf.map_partitions(_sanitizar_chunk, meta=meta_df)

    ddf_clean = ddf_clean.assign(
        total=ddf_clean['total'].fillna(0).astype('int64')
    ).dropna(subset=['fecha'])

    ddf_final = ddf_clean.assign(
        year=ddf_clean['fecha'].dt.year,
        periodo=ddf_clean['fecha'].dt.strftime('%Y-%m')
    )

    print(f"💾 Guardando dataset unificado en: {PARQUET_PATH}...")
    with ProgressBar():
        ddf_final.to_parquet(
            PARQUET_PATH,
            engine="pyarrow",
            write_index=False,
            compression="zstd",
            partition_on=["year", "linea"],
            overwrite=True
        )
    print("✅ Proceso completado.")


# ==============================================================================
# 4. FUNCIONES DE ANÁLISIS Y VISUALIZACIÓN
# ==============================================================================

def analizar_y_visualizar():
    """
    Carga los datos procesados de Parquet y genera las visualizaciones.
    """
    if not PARQUET_PATH.exists():
        print("❌ No se encontró el dataset Parquet. Ejecuta el procesamiento primero.")
        return

    print("📊 Cargando datos para análisis y visualización...")
    ddf = dd.read_parquet(PARQUET_PATH, engine="pyarrow")

    # --- Cálculo de usuarios por línea y período ---
    print("   -> Agregando usuarios por línea y período...")
    with ProgressBar():
        df_linea_periodo = (ddf.groupby(['linea', 'periodo'])['total']
                            .sum()
                            .compute()
                            .reset_index()
                            .rename(columns={'total': 'usuarios'}))

    df_linea_periodo = df_linea_periodo.sort_values(['linea', 'periodo'])

    # --- Creación de Gráficos ---
    print("📈 Generando visualizaciones...")

    # 1. Gráfico de Líneas
    fig_line = px.line(
        df_linea_periodo,
        x="periodo",
        y="usuarios",
        color="linea",
        title="Usuarios por Línea de Subte a lo Largo del Tiempo",
        markers=True,
        labels={"periodo": "Período (Año-Mes)", "usuarios": "Cantidad de Usuarios"}
    )
    fig_line.update_layout(xaxis_tickangle=-45)
    fig_line.show()

    # 2. Heatmap
    df_pivot = df_linea_periodo.pivot_table(index="linea", columns="periodo", values="usuarios", aggfunc='sum').fillna(0)
    fig_heatmap = px.imshow(
        df_pivot,
        aspect="auto",
        origin="lower",
        title="Heatmap de Usuarios por Línea y Período",
        labels={"x": "Período", "y": "Línea", "color": "Usuarios"}
    )
    fig_heatmap.update_xaxes(side="top")
    fig_heatmap.show()

    # 3. Treemap
    df_treemap = df_linea_periodo[df_linea_periodo["usuarios"] > 0]
    if not df_treemap.empty:
        fig_treemap = px.treemap(
            df_treemap,
            path=[px.Constant("Total General"), "linea", "periodo"],
            values="usuarios",
            color="usuarios",
            color_continuous_scale="Viridis",
            title="Distribución de Usuarios por Línea y Período (Treemap)"
        )
        fig_treemap.update_traces(root_color="lightgrey")
        fig_treemap.show()

    print("✅ Visualizaciones generadas.")


# ==============================================================================
# 5. PUNTO DE ENTRADA PRINCIPAL
# ==============================================================================

def main():
    """
    Orquesta la ejecución completa del pipeline: descarga, procesa y analiza.
    """
    # Paso 1: Asegurarse de que los datos crudos estén disponibles
    descargar_y_extraer_zip(
        drive_id="1tgJ1kyTGmxd_EtQ-3oqVracBXKGfMaxq",
        output_dir=RAW_DATA_PATH,
        zip_name="subtes_historicos.zip"
    )

    # Paso 2: Procesar los CSVs a un formato Parquet limpio y unificado
    procesar_csvs_a_parquet()

    # Paso 3: Cargar los datos limpios y generar los análisis y gráficos
    analizar_y_visualizar()


if __name__ == "__main__":
    main()