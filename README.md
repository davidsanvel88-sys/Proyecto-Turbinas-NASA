# 🛩️ Proyecto Turbinas — Telemetría y Diagnóstico Predictivo de Motores Turbofan

<p align="center">
  <img src="assets/dashboard_preview.png" alt="Dashboard de Diagnóstico de Turbinas" width="900"/>
</p>

---

## 📋 Descripción

Proyecto de **Data Engineering + Analítica Avanzada** que implementa un pipeline ETL de ingesta masiva de datos de telemetría de motores turbofan de la NASA, almacenamiento en PostgreSQL (Neon) y un dashboard interactivo de diagnóstico predictivo en Power BI.

El sistema permite monitorear en tiempo real la degradación de 100 motores turbofan, calcular la **Vida Útil Restante (RUL)** y generar **diagnósticos automáticos** con recomendaciones de mantenimiento basadas en el análisis de 21 sensores operacionales.

---

## 🏗️ Arquitectura del Proyecto

```
📁 Proyecto Turbinas
├── 📄 etl_turbofan.py              # Pipeline ETL principal (Extract-Transform-Load)
├── 📄 add_comments.py              # Script de metadata para PostgreSQL
├── 📊 Dashboard Turbina.pbix       # Dashboard interactivo Power BI
├── 📁 assets/
│   └── 🖼️ dashboard_preview.png   # Captura del dashboard
├── 📁 6. Turbofan Engine.../       # Dataset NASA (CMAPSSData)
│   ├── train_FD001.txt             # Datos de entrenamiento (20,631 registros)
│   ├── test_FD001.txt              # Datos de prueba
│   ├── RUL_FD001.txt               # Vida útil restante real
│   └── ...                         # Otros conjuntos FD002-FD004
└── 📄 README.md
```

---

## ⚙️ Pipeline ETL

El script `etl_turbofan.py` implementa un pipeline de **ingesta masiva** en 3 etapas:

### 1. Extracción (Extract)
- Descarga del dataset ZIP (~12 MB) desde **AWS S3** directamente en memoria (`io.BytesIO`)
- Manejo automático de ZIPs anidados (el archivo contiene `CMAPSSData.zip` internamente)
- Sin escritura a disco — procesamiento 100% en RAM

### 2. Transformación (Transform)
- Parsing del archivo `train_FD001.txt` (delimitado por espacios, sin encabezados)
- Asignación de **26 columnas** con nombres descriptivos de sensores
- Validación del DataFrame: 20,631 registros × 26 columnas × 100 motores

### 3. Carga (Load)
- Conexión segura a **PostgreSQL en Neon** (`sslmode=require`)
- Creación idempotente de tabla con tipos optimizados (`INTEGER`, `DOUBLE PRECISION`)
- **Bulk Insert** con `psycopg2.extras.execute_values` (lotes de 5,000 registros)
- Tiempo total de ejecución: **~7 segundos**

---

## 📊 Dashboard de Diagnóstico

El dashboard interactivo en **Power BI** se conecta directamente a la base de datos PostgreSQL y ofrece:

| Funcionalidad | Descripción |
|---|---|
| **Selector de Motor** | Dropdown para elegir cualquiera de los 100 motores |
| **Ciclos Restantes (RUL)** | Indicador numérico de vida útil restante estimada |
| **Tendencia de Temperatura** | Gráfico de línea del sensor s4 (HPC) por ciclo operacional |
| **Diagnóstico Automático** | Análisis textual de degradación con causa probable |
| **Impacto Físico** | Desglose del comportamiento de sensores críticos |
| **Recomendación AOG** | Acción inmediata de mantenimiento según severidad |

---

## 🗄️ Esquema de Base de Datos

```sql
CREATE TABLE turbofan_telemetry (
    id              SERIAL PRIMARY KEY,
    engine_id       INT NOT NULL,           -- Identificador del motor
    cycle           INT NOT NULL,           -- Ciclo operacional
    setting1-3      FLOAT,                  -- Configuraciones operativas
    s1_temp_inlet   FLOAT,                  -- Temp entrada ventilador
    s2_pres_inlet   FLOAT,                  -- Presión entrada compresor
    s3_temp_lpc     FLOAT,                  -- Temp compresor baja presión
    s4_temp_hpc     FLOAT,                  -- Temp compresor alta presión
    s5_pres_lpc     FLOAT,                  -- Presión salida compresor baja
    s6_pres_hpc     FLOAT,                  -- Presión salida compresor alta
    s7_fan_speed    FLOAT,                  -- Velocidad ventilador
    s8_core_speed   FLOAT,                  -- Velocidad núcleo
    s9-s21          FLOAT                   -- 13 sensores adicionales
);
-- 20,631 registros | 100 motores | 26 columnas
```

---

## 🔧 Tecnologías Utilizadas

| Tecnología | Uso |
|---|---|
| **Python 3.14** | Lenguaje principal del ETL |
| **Pandas** | Transformación y análisis de datos |
| **psycopg2** | Conexión y bulk insert a PostgreSQL |
| **PostgreSQL (Neon)** | Base de datos en la nube (serverless) |
| **Power BI** | Dashboard de visualización y diagnóstico |
| **NASA CMAPSSData** | Dataset de simulación de degradación de turbofan |

---

## 🚀 Ejecución

### Prerrequisitos
```bash
pip install pandas requests psycopg2-binary python-dotenv
```

### Configurar credenciales
```bash
# Copiar el template y agregar tus credenciales reales
cp .env.example .env

# Editar .env con tu string de conexión a PostgreSQL
# DATABASE_URL=postgresql://usuario:contraseña@host/base_de_datos?sslmode=require
```

> ⚠️ **Seguridad:** Las credenciales se cargan desde el archivo `.env` (excluido de Git). Nunca subas el `.env` al repositorio.

### Ejecutar el Pipeline ETL
```bash
python etl_turbofan.py
```

### Salida esperada
```
=================================================================
  ETL Pipeline: NASA Turbofan Engine Degradation Simulation
=================================================================
📥 Descargando ZIP desde S3...
   ✅ Descarga completada: 11.9 MB en 1.7s
🔧 Extrayendo 'train_FD001.txt' del ZIP...
📊 Transformación completada:
   Filas: 20,631 | Columnas: 26 | Motores: 100
🚀 Insertando 20,631 registros (Bulk Insert)...
   ✅ Inserción completada en 4.89s
=================================================================
  ✅ ¡PIPELINE COMPLETADO EXITOSAMENTE!
  📦 Registros insertados: 20,631
  ⏱️  Tiempo total: 6.81 segundos
=================================================================
```

---

## 📈 Dataset

**NASA Turbofan Engine Degradation Simulation (CMAPSSData)**

- **Fuente:** [NASA Prognostics Data Repository](https://phm-datasets.s3.amazonaws.com/NASA/6.+Turbofan+Engine+Degradation+Simulation+Data+Set.zip)
- **Descripción:** Datos de simulación run-to-failure de motores turbofan con 21 sensores operacionales
- **Referencia:** A. Saxena, K. Goebel, "Turbofan Engine Degradation Simulation Data Set", NASA Ames Prognostics Data Repository

---

## 👤 Autor

**David Sánchez Velázquez**
- GitHub: [@davidsanvel88-sys](https://github.com/davidsanvel88-sys)

---

<p align="center">
  <i>Proyecto de portafolio — Data Engineering & Predictive Analytics</i>
</p>
