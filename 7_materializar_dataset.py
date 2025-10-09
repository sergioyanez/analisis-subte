# -*- coding: utf-8 -*-
"""
7_materializar_dataset.py

Limpia la columna 'estacion' y crea características de alto nivel,
escribiendo el dataset final a Parquet particionado.
"""
from __future__ import annotations
import argparse
import json
import os
import time
import shutil
import re

import dask
import dask.dataframe as dd
from dask.diagnostics import ProgressBar
import holidays
import pandas as pd

# ----------------------------- Configuración -----------------------------

DEFAULT_IN = "data/dataset_subtes/historicos_parquet_ordenado"
DEFAULT_OUT = "data/dataset_subtes/final_con_features"

# NOTA: Actualiza estos valores con los resultados de tu primera ejecución.
EXPECTED = {"rows": 0, "cols": 0, "tipo_dia": {}, "franja_horaria": {}, "ubicacion": {}, }

SCHEMA_BASE = {
    "estacion": "string", "desde": "string", "hasta": "string",
    "periodo": "string", "total": "int64", "year": "int64", "linea": "string",
}


# ------------------------------- Funciones Principales --------------------------------

def limpiar_y_filtrar_estaciones(ddf: dd.DataFrame) -> dd.DataFrame:
    """
    Aplica una limpieza robusta a la columna 'estacion' para eliminar
    valores numéricos y estandarizar nombres.
    """
    print("🧼 Limpiando y estandarizando nombres de estaciones...")

    def limpiar_nombre(nombre: str) -> str:
        """Función para limpiar un único nombre de estación."""
        nombre = str(nombre).upper().strip()
        nombre = nombre.replace("PEÃ‘A", "PEÑA")
        if 'SAENZ' in nombre: return 'SAENZ PEÑA'
        if 'AG' in nombre and 'ERO' in nombre: return 'AGÜERO'
        nombre = nombre.replace('.', '')
        nombre = re.sub(r' [BDEH]$', '', nombre).strip()
        return nombre

    # --- INICIO DE LA CORRECCIÓN ---
    # Usamos ~.isnull() que es compatible con Dask, en lugar de .notna()
    is_numeric = ~dd.to_numeric(ddf['estacion'], errors='coerce').isnull()
    # --- FIN DE LA CORRECCIÓN ---

    ddf = ddf[~is_numeric]
    ddf['estacion'] = ddf['estacion'].map(limpiar_nombre, meta=('estacion', 'string'))

    return ddf


def build_ddf(parquet_in: str) -> dd.DataFrame:
    """Lee el dataset, limpia estaciones, crea características y asegura tipos."""
    print(f"🔎 Leyendo datos de: {parquet_in}")
    ddf = dd.read_parquet(parquet_in)

    ddf = limpiar_y_filtrar_estaciones(ddf)
    ddf = ddf.astype(SCHEMA_BASE)
    ddf = ddf.reset_index()

    print("🧱 Creando característica 'tipo_dia'...")
    feriados_set = set(holidays.AR(years=range(2014, 2023)).keys())
    condicion_finde = ddf["fecha"].isin(list(feriados_set)) | ddf["fecha"].dt.dayofweek.isin([5, 6])
    ddf["tipo_dia"] = "Día Hábil"
    ddf["tipo_dia"] = ddf["tipo_dia"].mask(condicion_finde, "Fin de Semana/Feriado")

    print("🧱 Creando característica 'franja_horaria'...")
    hora_serie = dd.to_datetime(ddf["desde"], format="%H:%M:%S", errors="coerce").dt.hour
    condicion_pico = ((hora_serie >= 7) & (hora_serie < 10)) | ((hora_serie >= 17) & (hora_serie < 20))
    ddf["franja_horaria"] = "Valle"
    ddf["franja_horaria"] = ddf["franja_horaria"].mask(condicion_pico, "Pico")

    print("🧱 Creando característica 'ubicacion'...")
    estaciones_centricas = [
        "9 DE JULIO", "C. PELLEGRINI", "CALLAO", "CATEDRAL", "CONGRESO", "CORREO CENTRAL",
        "DIAGONAL NORTE", "FLORIDA", "GENERAL SAN MARTÍN", "INDEPENDENCIA", "L. N. ALEM",
        "LAVALLE", "LIMA", "OBELISCO", "PASCO", "PERU", "PIEDRAS", "PLAZA DE MAYO",
        "RETIRO", "SAENZ PEÑA", "TRIBUNALES", "URUGUAY"
    ]
    ddf["ubicacion"] = "Periférica"
    ddf["ubicacion"] = ddf["ubicacion"].mask(ddf["estacion"].isin(estaciones_centricas), "Céntrica")

    return ddf.set_index("fecha").astype({
        "tipo_dia": "category", "franja_horaria": "category", "ubicacion": "category"
    })


def write_parquet(ddf: dd.DataFrame, parquet_out: str) -> None:
    """Escribe el dataset a Parquet de forma segura para la memoria."""
    print(f"💾 Escribiendo dataset definitivo en: {parquet_out}")
    shutil.rmtree(parquet_out, ignore_errors=True)
    with ProgressBar():
        ddf.to_parquet(
            parquet_out, engine="pyarrow", write_index=True,
            compression="snappy", partition_on=["year", "linea"], overwrite=True
        )


# (Aquí puedes mantener tus funciones de validación y metadatos si lo deseas)

def main():
    """Flujo principal de ejecución del script."""
    ddf = build_ddf(DEFAULT_IN)
    write_parquet(ddf, DEFAULT_OUT)
    print("\n🎯 Dataset definitivo listo para analytics.")
    print(f"   Cargar con: dd.read_parquet('{DEFAULT_OUT}')")


if __name__ == "__main__":
    main()