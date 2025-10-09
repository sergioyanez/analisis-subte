# Proyecto de Análisis de Datos del Subte

Este proyecto analiza datos históricos del subte... (aquí una breve descripción).

## 🚀 Puesta en marcha

Sigue estos pasos para configurar el entorno y ejecutar el proyecto.

### 1. Clonar el repositorio
```bash
git clone [https://github.com/sergioyanez/analisis-subte.git](https://github.com/sergioyanez/analisis-subte.git)
cd analisis-subte
```

### 2. Configurar los datos
Descarga el archivo `datos.zip` desde https://drive.google.com/file/d/1YTu2RULI5r-XyrlqnFXq6EIWnmb-EqDI/view?usp=sharing y descomprímelo en la raíz de este proyecto. La estructura de carpetas debería quedar así:

```
analisis-subte/
├── datos/
│   ├── historicos_parquet_final/
│   └── ... (otras carpetas de datos)
├── 1_analisis_de_subte.py
├── ... (otros scripts)
└── README.md
```

### 3. Crear el entorno virtual e instalar dependencias
```bash
# Crear el entorno virtual
python -m venv venv

# Activarlo (en Windows)
.\venv\Scripts\activate

# Activarlo (en Mac/Linux)
source venv/bin/activate

# Instalar las librerías
pip install -r requirements.txt
```

### 4. Órden de ejecución
Debes recorrer los scripts en el siguiente órden:
```bash

python 1_analitica_de_subtes.py
python 2_eliminar_duplicados.py
python 3_explorar_datos_agregar_columnas.py
python 4_simplificar_dataset.py
python 5_ordenar_dataset.py
python 6_verificar_estaciones.py
python 7_materializar_dataset.py
python 8_explorar_datos_limpios.py
python 9_fase2_analisis_hipotesis.py
python 10_fase3_prueba_estadistica.py
python 11_fase4_diagnostico_final.py
python 12_fase4_visualizacion_final.py
```
# Metodología de Análisis de Datos de Tráfico

El siguiente documento detalla el procedimiento técnico en cuatro fases para analizar el volumen de pasajeros y contrastar la hipótesis planteada. El proceso abarca desde la creación de variables y la manipulación de datos hasta la ejecución de pruebas estadísticas y la visualización de resultados.

---

## ⚙️ Fase 1: Ingeniería de Características (Feature Engineering)

**Abarca la ejecución de los primeros 8 scripts**

**Función:** Enriquecer el conjunto de datos original mediante la creación de nuevas variables (características) a partir de los datos existentes. Estas nuevas características son indispensables para segmentar los datos y realizar el análisis de hipótesis posterior.

### 🕒 1.1. Indexación Temporal del Dataset
- **Acción:** Se establece la columna *fecha* como el índice principal del DataFrame.  
- **Propósito:** Facilitar operaciones y filtrados basados en series de tiempo, optimizando el rendimiento y la lógica de las consultas temporales.

### 📅 1.2. Creación de la Variable `tipo_dia`
- **Acción:** Se clasifica cada registro en “Día Hábil” o “Fin de Semana/Feriado”.
- **Implementación:**
  - Se extrae el día de la semana (Lunes=0, Domingo=6) de la columna *fecha*.
  - Se asigna “Día Hábil” a los días de 0 a 4.
  - Se asigna “Fin de Semana/Feriado” a los días 5 y 6.
  - Se consulta una lista externa de feriados nacionales para reclasificar dichos días como “Fin de Semana/Feriado”.

### ⏰ 1.3. Creación de la Variable `franja_horaria`
- **Acción:** Se categoriza cada registro en “Pico” o “Valle” según la hora de inicio.
- **Implementación:**
  - Se extrae la hora de la columna *desde*.
  - Se define el horario “Pico” como los intervalos de 7:00 a 9:59 y de 17:00 a 19:59.
  - Cualquier registro fuera de estos intervalos se clasifica como “Valle”.

### 📍 1.4. Creación de la Variable `ubicacion`
- **Acción:** Se clasifica cada estación como “Céntrica” o “Periférica”.
- **Implementación:**
  - Se define un conjunto predeterminado de estaciones “céntricas” (ejemplo: `['9 DE JULIO', 'CATEDRAL', 'RETIRO', ...]`).
  - Si la estación pertenece a este conjunto, se asigna “Céntrica”; de lo contrario, “Periférica”.

---

## 📊 Fase 2: Filtrado y Agregación de Datos

**Ejecución del script 9**

**Función:** Segmentar el DataFrame enriquecido para aislar los dos grupos de muestra requeridos por la hipótesis nula (H0) y la alternativa (HA), y calcular sus métricas agregadas.

### 🚦 2.1. Definición del Grupo 1 ( Hipótesis Alternativa HA): 
- **Acción:** Filtrar el dataset para obtener los registros que cumplen simultáneamente con las condiciones de máxima demanda teórica.
- **Condiciones de Filtrado:**
  - `tipo_dia == "Día Hábil"`
  - `franja_horaria == "Pico"`
  - `ubicacion == "Céntrica"`
- **Agregación:** Calcular el volumen promedio de pasajeros (`total.mean()`) para este subconjunto de datos.

### 🧘‍♂️ 2.2. Definición del Grupo 2 (Hipótesis nula H0): 
- **Acción:** Filtrar el dataset para obtener un grupo de control o base que representa una demanda menor.
- **Condiciones de Filtrado:**
  - `tipo_dia == "Fin de Semana/Feriado"`
  - `ubicacion == "Céntrica"`
- **Agregación:** Calcular el volumen promedio de pasajeros (`total.mean()`) para este segundo subconjunto.

---

## 📐 Fase 3: Contraste de Hipótesis

**Ejecución del script 10**

**Función:** Ejecutar una prueba estadística formal para determinar si la diferencia observada entre los promedios de los grupos MMC y MB es estadísticamente significativa.

### ✅ 3.1. Verificación de Condición Mínima
- **Acción:** Realizar una comprobación aritmética para validar la premisa principal de la hipótesis alternativa.
- **Criterio:** Evaluar si `Promedio(HA) >= 2 * Promedio(H0)`. Si esta condición no se cumple, la hipótesis alternativa se descarta sin necesidad de una prueba estadística.

### 🧮 3.2. Prueba de Medias (Test t de Muestras Independientes)
- **Acción:** Aplicar un test estadístico para comparar las medias de los dos grupos de muestra independientes.
- **Implementación:** Se utiliza la columna `total` del Grupo 1 (HA) y la columna `total` del Grupo 2 (H0) como entradas para la prueba t.
- **Interpretación del Resultado:** Un `p-value < 0.05` indica una diferencia estadísticamente significativa.

### 🧾 3.3. Conclusión del Contraste
- **Acción:** Sintetizar los resultados de los pasos anteriores para tomar una decisión final.
- **Criterio de Decisión:** Si se cumplen las dos condiciones (`Promedio(HA) >= 2 * Promedio(H0)` y `p-value < 0.05`), se rechaza la Hipótesis Nula (H0), apoyando la Hipótesis Alternativa (HA).

---

## 📈 Fase 4: Análisis Exploratorio y Visualización de Datos

**Ejecución del script 11 y 12** 

**Función:** Utilizar las características creadas para responder preguntas de investigación secundarias y comunicar los hallazgos de manera visual y efectiva.

### 🔍 4.1. Análisis de Distribución de Tráfico
- **Objetivo:** Identificar patrones generales de concurrencia.
- **Acciones:**
  - Gráfico de Barras: Visualizar el Top 20 de estaciones por volumen total.
  - Mapa de Calor (Heatmap): Días (X), horas (Y) e intensidad de color según volumen promedio.

### ⚖️ 4.2. Análisis Comparativo por Tipo de Día
- **Objetivo:** Cuantificar la diferencia en el uso del servicio entre días hábiles y fines de semana/feriados.
- **Acción:** Agrupar por `tipo_dia` y calcular el promedio de `total`.

---
