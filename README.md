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
Descarga el archivo `datos.zip` desde [este enlace de Google Drive / Dropbox] y descomprímelo en la raíz de este proyecto. La estructura de carpetas debería quedar así:

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

### 4. Ejecutar el análisis
Para correr el análisis principal, ejecuta el siguiente script:
```bash
python 11_fase4_visualizacion_final.py
```
