# -*- coding: utf-8 -*-
"""
Script de Diagnóstico: Conteo de Datos en Cada Etapa del Pipeline

Este script te permite ver cuántos registros tienes en cada carpeta de datos
para identificar dónde se están perdiendo registros.
"""
import dask.dataframe as dd
from dask.diagnostics import ProgressBar
from pathlib import Path
import pandas as pd

# Definir todas las rutas de tus datasets
RUTAS = {
    "1. Datos iniciales (parquet_limpio)": "data/dataset_subtes/historicos_parquet_limpio",
    "2. Sin duplicados (parquet_final)": "data/dataset_subtes/historicos_parquet_final",
    "3. Simplificado (sin columnas extra)": "data/dataset_subtes/historicos_parquet_simple",
    "4. Ordenado": "data/dataset_subtes/historicos_parquet_ordenado",
    "5. Final con features": "data/dataset_subtes/final_con_features",
}


def contar_registros(ruta: str) -> dict:
    """
    Cuenta registros totales y analiza la columna 'estacion' si existe.
    """
    try:
        ddf = dd.read_parquet(ruta)

        with ProgressBar():
            total = len(ddf)
            columnas = list(ddf.columns)

            # Si existe la columna 'estacion', analizarla
            estacion_info = None
            if 'estacion' in columnas:
                estacion_series = ddf['estacion'].astype(str)

                # Contar cuántos son numéricos (pueden convertirse a número)
                numericos = (~dd.to_numeric(estacion_series, errors='coerce').isna()).sum()
                no_numericos = total - numericos

                estacion_info = {
                    'numericos': int(numericos.compute()),
                    'no_numericos': int(no_numericos.compute())
                }

        return {
            'existe': True,
            'total': total,
            'columnas': len(columnas),
            'estacion_info': estacion_info
        }
    except Exception as e:
        return {
            'existe': False,
            'error': str(e)
        }


def main():
    print("=" * 80)
    print("🔍 DIAGNÓSTICO DE CONTEO DE DATOS POR ETAPA")
    print("=" * 80)

    resultados = {}

    for nombre, ruta in RUTAS.items():
        print(f"\n📊 Analizando: {nombre}")
        print(f"   Ruta: {ruta}")

        if not Path(ruta).exists():
            print(f"   ⚠️  La carpeta NO existe")
            resultados[nombre] = {'existe': False}
            continue

        info = contar_registros(ruta)
        resultados[nombre] = info

        if info['existe']:
            print(f"   ✅ Total de registros: {info['total']:,}")
            print(f"   📋 Número de columnas: {info['columnas']}")

            if info['estacion_info']:
                est = info['estacion_info']
                print(f"   🏢 Columna 'estacion':")
                print(f"      - Valores NUMÉRICOS: {est['numericos']:,}")
                print(f"      - Valores NO numéricos (texto): {est['no_numericos']:,}")
                porcentaje_numericos = (est['numericos'] / info['total']) * 100
                print(f"      - % de registros numéricos: {porcentaje_numericos:.2f}%")
        else:
            print(f"   ❌ Error: {info.get('error', 'Desconocido')}")

    # Resumen final
    print("\n" + "=" * 80)
    print("📈 RESUMEN DE PÉRDIDA DE DATOS")
    print("=" * 80)

    totales = [(nombre, r['total']) for nombre, r in resultados.items() if r.get('existe', False)]

    if len(totales) > 1:
        for i in range(len(totales) - 1):
            nombre_actual, total_actual = totales[i]
            nombre_siguiente, total_siguiente = totales[i + 1]

            diferencia = total_actual - total_siguiente
            porcentaje = (diferencia / total_actual) * 100 if total_actual > 0 else 0

            print(f"\n{nombre_actual} → {nombre_siguiente}")
            print(f"   Registros perdidos: {diferencia:,} ({porcentaje:.2f}%)")

    print("\n" + "=" * 80)
    print("✅ Diagnóstico completado")


if __name__ == "__main__":
    main()
