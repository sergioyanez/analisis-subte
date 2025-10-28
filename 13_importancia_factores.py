# -*- coding: utf-8 -*-
"""
13_importancia_factores.py

Calcula la "importancia" de los factores para explicar la variación de
`total` usando eta² univariante (escalable con Dask) y muestra:
- Porcentaje eta² por factor (no necesariamente suma 100%).
- Porcentaje normalizado que sí suma 100% (recomendado para el gráfico de torta).

Factores considerados: estacion, tipo_dia, franja_horaria, ubicacion.
Fuente: data/dataset_subtes/final_con_features
"""
from __future__ import annotations
import sys
from typing import Dict

import dask
import dask.dataframe as dd
from dask.diagnostics import ProgressBar

PARQUET_PATH = "data/dataset_subtes/final_con_features"
FACTORES = ["estacion", "tipo_dia", "franja_horaria", "ubicacion"]
TARGET = "total"


# -------------------- Utilidades --------------------

def _eta2_univariante(ddf: dd.DataFrame, factor: str, y: str) -> float:
    """Calcula eta^2 (porcentaje de varianza explicada) para un factor.

    eta^2 = SS_between / SS_total, con:
    - SS_between = sum(n_g * (ybar_g - ybar)^2)
    - SS_total = var_total * n_total

    Devuelve el porcentaje (0-100).
    """
    g = ddf.groupby(factor)[y]
    mean_g_task = g.mean()
    count_g_task = g.count()
    mean_global_task = ddf[y].mean()
    var_total_task = ddf[y].var(ddof=0)
    n_total_task = ddf.shape[0]

    with ProgressBar():
        mean_g, count_g, mean_global, var_total, n_total = dask.compute(
            mean_g_task, count_g_task, mean_global_task, var_total_task, n_total_task
        )

    ss_between = float(((count_g * (mean_g - mean_global) ** 2).sum()))
    ss_total = float(var_total) * int(n_total)

    if ss_total <= 0:
        return 0.0
    return 100.0 * (ss_between / ss_total)


# -------------------- Flujo principal --------------------

def calcular_eta2_por_factor() -> Dict[str, float]:
    print("🔎 Cargando datos desde:", PARQUET_PATH)
    ddf = dd.read_parquet(PARQUET_PATH)

    print("\n📐 Calculando eta² univariante (total ~ factor) con Dask...")
    eta2 = {}
    for factor in FACTORES:
        try:
            pct = _eta2_univariante(ddf, factor, TARGET)
        except Exception as e:
            print("   ⚠️ Error en eta² para", factor, ":", e)
            pct = 0.0
        eta2[factor] = round(float(pct), 2)
        print("   -", factor, ":", eta2[factor], "%")
    return eta2


def main():
    eta2 = calcular_eta2_por_factor()

    # Normalizar para que suma 100% (útil para gráfico de torta)
    suma = sum(eta2.values())
    normalizado = {k: (round(v * 100.0 / suma, 2) if suma > 0 else 0.0) for k, v in eta2.items()}

    print("\n------ Porcentajes para el gráfico de torta ------")
    print("(a) Eta² bruto por factor (no suma 100%):")
    for k, v in eta2.items():
        print("  -", k, ":", v, "%")

    print("\n(b) Porcentaje normalizado (suma 100%):")
    total_norm = 0.0
    for k, v in normalizado.items():
        print("  -", k, ":", v, "%")
        total_norm += v
    print("  Total:", round(total_norm, 2), "%")


if __name__ == "__main__":
    main()
