
# MailAmericas – Ventas Data Pipeline (AWS Lakehouse)

**Autor:** Italo Contreras Pérez  
**Rol:** Senior Data Engineer  
**Stack:** AWS Glue, S3, Athena, Python, PyArrow, Pandas

---

## 🏗️ Arquitectura General

El proyecto implementa un pipeline de datos de ventas multi-capa (**Raw → Bronze → Silver → Gold**) en AWS, diseñado para transformar y analizar la información de ventas proveniente de archivos Excel (.xlsx) por sucursal.  

Cada capa representa un nivel de calidad, granularidad y preparación de los datos para análisis y visualización en **Athena** o **QuickSight**.

---

## 🔁 Flujo General

| Capa | Descripción | Ubicación | Script Glue |
|------|--------------|------------|--------------|
| **Raw** | Almacena los archivos originales .xlsx por sucursal. | `s3://mailamericas-datalake/raw/ventas/` | — |
| **Bronze** | Limpieza y estandarización de columnas. Se genera Parquet particionado. | `s3://mailamericas-datalake/bronze/ventas/` | `ventas_ingest_raw_to_bronze.py` |
| **Silver** | Enriquecimiento con tipo de cambio y métricas financieras (ARS → USD). | `s3://mailamericas-datalake/silver/ventas/` | `ventas_transform_bronze_to_silver.py` |
| **Gold** | Agregación analítica (margen, estacionalidad, cumplimiento). | `s3://mailamericas-datalake/gold/ventas_analiticas/` | `ventas_aggregate_silver_to_gold.py` |

---

## ⚙️ Scripts principales

### 1️⃣ ventas_ingest_raw_to_bronze.py
- Lee archivos Excel (.xlsx) desde S3 Raw.  
- Limpia encabezados y normaliza nombres de columnas.  
- Convierte los datos a **Parquet** comprimido (Snappy).  
- Particiona por `sucursal/year/month`.  
- Incluye manejo de errores y logging detallado.

### 2️⃣ ventas_transform_bronze_to_silver.py
- Lee Parquets desde Bronze.  
- Calcula métricas financieras (ARS → USD).  
- Agrega tipo de cambio (`exchange_rate_ars_usd_2024.csv`).  
- Añade columnas `DIA_MES` y `DIA_SEMANA`.  
- Escribe nuevamente en formato Parquet particionado.

### 3️⃣ ventas_aggregate_silver_to_gold.py
- Agrega métricas a nivel mensual y por producto:  
  - Margen total y porcentaje.  
  - Producto con mayor margen.  
  - Día y día de la semana con mayores ventas.  
  - Cumplimiento del objetivo de margen (`>20%` = “superó”).  
- Escribe salida en capa GOLD.

---

## 🧠 Scripts SQL (Athena)


| Archivo | Descripción |
|----------|--------------|
| `create_bronze_table.sql` | Crea tabla externa en Athena sobre S3 Bronze. |
| `create_silver_table.sql` | Crea tabla externa sobre S3 Silver. |
| `create_gold_table.sql` | Crea tabla externa sobre S3 Gold. |
| `create_reference_table.sql` | Crea tabla de tipo de cambio. |

---

## 🧱 Buenas prácticas
- Particionado jerárquico (`sucursal/year/month`).
- Compresión eficiente: Parquet + Snappy.
- Validación de columnas y tipos de datos.
- Logging y manejo de excepciones robusto.
- Modularización y reutilización de funciones.

---

## Capturas

-mailamericas_bronze.ventas

![bronze](docs/screenshots/mailamericas_bronze.ventas.png)


-mailamericas_silver.ventas

![silver](docs/screenshots/mailamericas_silver.ventas.png)

-mailamericas_gold.ventas

![gold](docs/screenshots/mailamericas_gold.ventas.png)