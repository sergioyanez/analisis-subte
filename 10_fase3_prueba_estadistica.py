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

    # --- Realización de Tests t ---
    # Test t de Welch (recomendado para varianzas desiguales)
    t_welch, p_welch = stats.ttest_ind(sample_ha, sample_h0, equal_var=False)

    # Test t estándar (para comparación)
    t_standard, p_standard = stats.ttest_ind(sample_ha, sample_h0, equal_var=True)

    # Test de Mann-Whitney U (alternativa no paramétrica)
    u_statistic, p_mannwhitney = stats.mannwhitneyu(sample_ha, sample_h0, alternative='two-sided')

    # Calculamos estadísticas descriptivas adicionales
    mean_ha_sample = sample_ha.mean()
    mean_h0_sample = sample_h0.mean()
    std_ha_sample = sample_ha.std()
    std_h0_sample = sample_h0.std()

    print("\n------ Estadísticas Descriptivas de las Muestras ------")
    print(f"   - Muestra HA: Media = {mean_ha_sample:.2f}, Desv. Std = {std_ha_sample:.2f}, n = {len(sample_ha)}")
    print(f"   - Muestra H0: Media = {mean_h0_sample:.2f}, Desv. Std = {std_h0_sample:.2f}, n = {len(sample_h0)}")
    print(f"   - Diferencia de medias: {mean_ha_sample - mean_h0_sample:.2f}")

    # --- VALIDACIONES ADICIONALES ---
    print("\n------ Diagnósticos y Validaciones ------")

    # Verificar si hay valores extremos o atípicos
    print(f"   - Rango HA: min={sample_ha.min():.2f}, max={sample_ha.max():.2f}")
    print(f"   - Rango H0: min={sample_h0.min():.2f}, max={sample_h0.max():.2f}")

    # Calcular coeficientes de variación
    cv_ha = (std_ha_sample / mean_ha_sample) * 100 if mean_ha_sample != 0 else float('inf')
    cv_h0 = (std_h0_sample / mean_h0_sample) * 100 if mean_h0_sample != 0 else float('inf')
    print(f"   - Coef. de Variación HA: {cv_ha:.1f}%")
    print(f"   - Coef. de Variación H0: {cv_h0:.1f}%")

    # Calcular el tamaño del efecto (Cohen's d)
    pooled_std = ((std_ha_sample**2 + std_h0_sample**2) / 2)**0.5
    cohens_d = (mean_ha_sample - mean_h0_sample) / pooled_std
    print(f"   - Tamaño del efecto (Cohen's d): {cohens_d:.2f}")

    # Interpretación del tamaño del efecto
    if abs(cohens_d) < 0.2:
        efecto_interpretacion = "pequeño"
    elif abs(cohens_d) < 0.5:
        efecto_interpretacion = "pequeño a mediano"
    elif abs(cohens_d) < 0.8:
        efecto_interpretacion = "mediano a grande"
    else:
        efecto_interpretacion = "muy grande (posiblemente problemático)"
    print(f"   - Interpretación del efecto: {efecto_interpretacion}")

    # Verificar normalidad aproximada (regla empírica)
    ratio_std = std_ha_sample/std_h0_sample
    print(f"   - Razón de desv. estándares: {ratio_std:.2f}")
    if ratio_std > 2 or 1/ratio_std > 2:
        print("   ⚠️  ADVERTENCIA: Las desviaciones estándar son muy diferentes")
        print("   📊 RECOMENDACIÓN: Usar Welch's t-test (equal_var=False)")

    # Test de Levene para homogeneidad de varianzas
    levene_stat, levene_p = stats.levene(sample_ha, sample_h0)
    print(f"   - Test de Levene (igualdad de varianzas): p = {levene_p:.2e}")
    if levene_p < 0.05:
        print("   ⚠️  Las varianzas son significativamente diferentes (p < 0.05)")
        print("   ✅ Welch's t-test es la opción correcta")
    else:
        print("   ✅ Las varianzas no son significativamente diferentes")

    print("\n------ Comparación de Tests Estadísticos ------")

    # Welch's t-test (recomendado)
    print(f"🔬 WELCH'S T-TEST (varianzas desiguales):")
    print(f"   - Estadístico t: {t_welch:.4f}")
    if p_welch < 1e-10:
        print(f"   - P-value: {p_welch:.2e} (< 1e-10)")
    else:
        print(f"   - P-value: {p_welch:.6f}")

    # T-test estándar (para comparación)
    print(f"\n📊 T-TEST ESTÁNDAR (varianzas iguales - para comparación):")
    print(f"   - Estadístico t: {t_standard:.4f}")
    if p_standard < 1e-10:
        print(f"   - P-value: {p_standard:.2e} (< 1e-10)")
    else:
        print(f"   - P-value: {p_standard:.6f}")

    # Mann-Whitney U (no paramétrico)
    print(f"\n🔢 MANN-WHITNEY U TEST (no paramétrico):")
    print(f"   - Estadístico U: {u_statistic:.0f}")
    if p_mannwhitney < 1e-10:
        print(f"   - P-value: {p_mannwhitney:.2e} (< 1e-10)")
    else:
        print(f"   - P-value: {p_mannwhitney:.6f}")

    # Calcular grados de libertad para Welch's test
    n1, n2 = len(sample_ha), len(sample_h0)
    s1_sq, s2_sq = std_ha_sample**2, std_h0_sample**2
    welch_df = (s1_sq/n1 + s2_sq/n2)**2 / ((s1_sq/n1)**2/(n1-1) + (s2_sq/n2)**2/(n2-1))

    print(f"\n📈 INFORMACIÓN ADICIONAL:")
    print(f"   - Grados de libertad (t-test estándar): {n1 + n2 - 2}")
    print(f"   - Grados de libertad (Welch's test): {welch_df:.1f}")
    print(f"   - Diferencia en gl: {(n1 + n2 - 2) - welch_df:.1f}")

    print("\n------ Resultados Finales (Welch's t-test) ------")
    print(f"   ✅ Test recomendado: Welch's t-test (equal_var=False)")
    print(f"   - Estadístico t: {t_welch:.4f}")
    if p_welch < 1e-10:
        print(f"   - P-value: {p_welch:.2e} (prácticamente cero)")
    else:
        print(f"   - P-value: {p_welch:.6f}")
    print(f"   - Grados de libertad: {welch_df:.1f}")
    print(f"   - Magnitud del efecto (diferencia estandarizada): {abs(t_welch):.2f}")

    alfa = 0.05
    print(f"\n4. Conclusión (para un nivel de significancia alfa = {alfa}):")
    if p_welch < alfa:
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