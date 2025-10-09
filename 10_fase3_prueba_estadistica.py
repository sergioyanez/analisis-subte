import dask
import dask.dataframe as dd
from dask.diagnostics import ProgressBar
from scipy import stats


def fase3_prueba_estadistica(parquet_path: str, sample_size: int = 10000):
    """
    Realiza la Fase 3 completa:
    1. Calcula los promedios de los grupos HA y H0.
    2. Verifica la condición del "doble".
    3. Toma muestras y realiza un test t para determinar la significancia estadística.
    """
    try:
        print(f"📊 Cargando dataset final desde: {parquet_path}")
        ddf = dd.read_parquet(parquet_path)
    except FileNotFoundError:
        print(f"❌ ERROR: No se encontró el dataset en '{parquet_path}'.")
        return

    # --- Filtrado de Grupos ---
    condicion_ha = (
            (ddf["tipo_dia"] == "Día Hábil") &
            (ddf["franja_horaria"] == "Pico") &
            (ddf["ubicacion"] == "Céntrica")
    )
    ddf_ha = ddf[condicion_ha]

    condicion_h0 = (
            (ddf["tipo_dia"] == "Fin de Semana/Feriado") &
            (ddf["ubicacion"] == "Céntrica")
    )
    ddf_h0 = ddf[condicion_h0]

    # --- Cálculo de Promedios ---
    print("\nCalculando promedios de los grupos HA y H0...")
    with ProgressBar():
        media_ha, media_h0 = dask.compute(
            ddf_ha["total"].mean(),
            ddf_h0["total"].mean()
        )
    print(f"   - Promedio para HA: {media_ha:.2f}")
    print(f"   - Promedio para H0: {media_h0:.2f}")

    # --- Verificación del Factor "Doble" ---
    print("\n1. Verificación del Factor 'Doble' (Promedio HA >= 2 * Promedio H0)...")
    if media_ha >= 2 * media_h0:
        print(
            f"   ✅ Se cumple la condición: {media_ha:.2f} es mayor o igual que el doble de {media_h0:.2f} (que es {2 * media_h0:.2f}).")
        print("   Procediendo con el test de significancia estadística.")
    else:
        print(
            f"   ❌ No se cumple la condición: {media_ha:.2f} es menor que el doble de {media_h0:.2f} (que es {2 * media_h0:.2f}).")
        return

    # --- INICIO DE LA CORRECCIÓN ---
    # --- Toma de Muestras usando Fracción ---
    print("\n2. Contando filas para calcular la fracción de muestreo...")
    with ProgressBar():
        # Contamos cuántas filas hay en cada grupo filtrado
        len_ha, len_h0 = dask.compute(len(ddf_ha), len(ddf_h0))
    print(f"   - Se encontraron {len_ha:,} registros para HA y {len_h0:,} para H0.")

    # Calculamos la fracción necesaria para obtener 'sample_size' elementos de cada grupo
    frac_ha = sample_size / len_ha if len_ha > 0 else 0
    frac_h0 = sample_size / len_h0 if len_h0 > 0 else 0

    print("\n3. Tomando muestras para la Prueba de Medias (t-test)...")
    # Tomamos la muestra usando la fracción calculada
    sample_ha = ddf_ha["total"].sample(frac=frac_ha, random_state=42).compute()
    sample_h0 = ddf_h0["total"].sample(frac=frac_h0, random_state=42).compute()
    print(f"   - Muestras tomadas: {len(sample_ha)} para HA y {len(sample_h0)} para H0.")
    # --- FIN DE LA CORRECCIÓN ---

    # --- Realización del Test t ---
    t_statistic, p_value = stats.ttest_ind(sample_ha, sample_h0, equal_var=False)

    print("\n------ Resultados de la Prueba Estadística ------")
    print(f"   - Estadístico t: {t_statistic:.4f}")
    print(f"   - P-value: {p_value}")  # Imprimimos el p-value completo para ver su magnitud

    alfa = 0.05
    print(f"\n4. Conclusión (para un nivel de significancia alfa = {alfa}):")
    if p_value < alfa:
        print("   ✅ El p-value es menor que alfa.")
        print("   Se rechaza la Hipótesis Nula (H0).")
        print("   La diferencia entre los promedios es estadísticamente significativa.")
    else:
        print("   ❌ El p-value es mayor o igual que alfa.")
        print("   No se puede rechazar la Hipótesis Nula (H0).")
        print("   La diferencia observada podría deberse al azar.")


if __name__ == "__main__":
    RUTA_FINAL = "data/dataset_subtes/final_con_features"
    fase3_prueba_estadistica(RUTA_FINAL)