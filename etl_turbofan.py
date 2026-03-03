"""
=============================================================================
ETL Pipeline: NASA Turbofan Engine Degradation Simulation Data
=============================================================================
Autor: Senior Data Engineer
Fecha: 2026-03-03
Descripción:
    Script de ingesta masiva que extrae datos crudos de telemetría de turbinas
    de la NASA desde un archivo ZIP remoto, los transforma en memoria usando
    Pandas y los carga en PostgreSQL (Neon) mediante Bulk Insert.
=============================================================================
"""

import io
import os
import time
import zipfile

import pandas as pd
import psycopg2
import psycopg2.extras
import requests
from dotenv import load_dotenv

# Cargar variables de entorno desde .env
load_dotenv()

# ===========================================================================
# CONFIGURACIÓN
# ===========================================================================

# URL del dataset ZIP remoto
ZIP_URL = (
    "https://phm-datasets.s3.amazonaws.com/NASA/"
    "6.+Turbofan+Engine+Degradation+Simulation+Data+Set.zip"
)

# Archivo objetivo dentro del ZIP
TARGET_FILE = "train_FD001.txt"

# String de conexión a PostgreSQL (Neon) — cargado desde variable de entorno
DB_CONNECTION_STRING = os.environ["DATABASE_URL"]

# Tabla destino
TABLE_NAME = "turbofan_telemetry"

# Mapeo de columnas (26 columnas, en el orden exacto del TXT)
COLUMNS = [
    "engine_id",        # Identificador del motor
    "cycle",            # Ciclo operacional
    "setting1",         # Configuración operativa 1
    "setting2",         # Configuración operativa 2
    "setting3",         # Configuración operativa 3
    "s1_temp_inlet",    # Sensor 1: Temperatura de entrada
    "s2_pres_inlet",    # Sensor 2: Presión de entrada
    "s3_temp_lpc",      # Sensor 3: Temperatura LPC (compresor baja presión)
    "s4_temp_hpc",      # Sensor 4: Temperatura HPC (compresor alta presión)
    "s5_pres_lpc",      # Sensor 5: Presión LPC
    "s6_pres_hpc",      # Sensor 6: Presión HPC
    "s7_fan_speed",     # Sensor 7: Velocidad del ventilador
    "s8_core_speed",    # Sensor 8: Velocidad del núcleo
    "s9_bypass_ratio",  # Sensor 9: Ratio de bypass
    "s10_burner_pres",  # Sensor 10: Presión del quemador
    "s11_bleed_enthalpy",  # Sensor 11: Entalpía de purga
    "s12_hpt_speed",    # Sensor 12: Velocidad HPT (turbina alta presión)
    "s13_lpt_speed",    # Sensor 13: Velocidad LPT (turbina baja presión)
    "s14_static_pres",  # Sensor 14: Presión estática
    "s15_fuel_flow",    # Sensor 15: Flujo de combustible
    "s16_temp_exhaust", # Sensor 16: Temperatura de escape
    "s17_pres_exhaust", # Sensor 17: Presión de escape
    "s18_vibration",    # Sensor 18: Vibración
    "s19_shaft_speed",  # Sensor 19: Velocidad del eje
    "s20_temp_ambient", # Sensor 20: Temperatura ambiente
    "s21_coolant_bleed",# Sensor 21: Purga de refrigerante
]


# ===========================================================================
# PASO 1: DESCARGA DEL ZIP EN MEMORIA
# ===========================================================================
def download_zip_to_memory(url: str) -> io.BytesIO:
    """Descarga el archivo ZIP desde la URL y lo almacena en memoria."""
    print("📥 Descargando ZIP desde S3...")
    print(f"   URL: {url[:80]}...")
    start = time.time()

    response = requests.get(url, timeout=120)
    response.raise_for_status()

    elapsed = time.time() - start
    size_mb = len(response.content) / (1024 * 1024)
    print(f"   ✅ Descarga completada: {size_mb:.1f} MB en {elapsed:.1f}s")

    return io.BytesIO(response.content)


# ===========================================================================
# PASO 2: EXTRACCIÓN Y TRANSFORMACIÓN
# ===========================================================================
def extract_and_transform(zip_buffer: io.BytesIO, target_file: str) -> pd.DataFrame:
    """Extrae el archivo TXT del ZIP (soporta ZIPs anidados) y lo transforma en un DataFrame."""
    print(f"\n🔧 Extrayendo '{target_file}' del ZIP...")

    with zipfile.ZipFile(zip_buffer, "r") as outer_zf:
        all_files = outer_zf.namelist()
        print(f"   Archivos en el ZIP externo: {len(all_files)}")
        print(f"   Contenido: {all_files}")

        # Buscar directamente en el ZIP externo
        matched = [f for f in all_files if f.endswith(target_file)]

        if matched:
            # Archivo encontrado directamente
            actual_path = matched[0]
            print(f"   Ruta interna: {actual_path}")
            with outer_zf.open(actual_path) as txt_file:
                df = pd.read_csv(
                    txt_file,
                    sep=r"\s+",
                    header=None,
                    names=COLUMNS,
                    engine="python",
                )
        else:
            # Buscar ZIPs anidados
            inner_zips = [f for f in all_files if f.endswith(".zip")]
            if not inner_zips:
                raise FileNotFoundError(
                    f"'{target_file}' no encontrado en el ZIP. "
                    f"Archivos disponibles: {all_files}"
                )

            print(f"   ZIP anidado detectado: {inner_zips[0]}")
            # Extraer el ZIP interno en memoria
            inner_zip_data = io.BytesIO(outer_zf.read(inner_zips[0]))

            with zipfile.ZipFile(inner_zip_data, "r") as inner_zf:
                inner_files = inner_zf.namelist()
                print(f"   Archivos en ZIP interno: {len(inner_files)}")

                matched_inner = [f for f in inner_files if f.endswith(target_file)]
                if not matched_inner:
                    raise FileNotFoundError(
                        f"'{target_file}' no encontrado en ZIP anidado. "
                        f"Archivos disponibles: {inner_files}"
                    )

                actual_path = matched_inner[0]
                print(f"   Ruta interna: {actual_path}")

                with inner_zf.open(actual_path) as txt_file:
                    df = pd.read_csv(
                        txt_file,
                        sep=r"\s+",
                        header=None,
                        names=COLUMNS,
                        engine="python",
                    )

    print(f"\n📊 Transformación completada:")
    print(f"   Filas:    {len(df):,}")
    print(f"   Columnas: {len(df.columns)}")
    print(f"   Motores únicos: {df['engine_id'].nunique()}")
    print(f"   Memoria: {df.memory_usage(deep=True).sum() / (1024*1024):.2f} MB")
    print(f"\n   Primeras 3 filas:")
    print(df.head(3).to_string(index=False))

    return df


# ===========================================================================
# PASO 3: CARGA A POSTGRESQL (BULK INSERT)
# ===========================================================================
def load_to_postgres(df: pd.DataFrame, conn_string: str, table: str) -> int:
    """
    Crea la tabla (si no existe) e inserta todos los registros
    usando execute_values para máxima eficiencia.
    """
    conn = None
    cursor = None
    rows_inserted = 0

    try:
        # --- Conexión ---
        print(f"\n🔌 Conectando a PostgreSQL (Neon)...")
        conn = psycopg2.connect(conn_string)
        cursor = conn.cursor()
        print("   ✅ Conexión establecida")

        # --- Crear tabla (DROP + CREATE para idempotencia) ---
        print(f"\n🏗️  Creando tabla '{table}'...")
        cursor.execute(f"DROP TABLE IF EXISTS {table};")

        # Definir tipos: engine_id y cycle como INTEGER, el resto FLOAT
        col_defs = []
        for col in COLUMNS:
            if col in ("engine_id", "cycle"):
                col_defs.append(f"    {col} INTEGER NOT NULL")
            else:
                col_defs.append(f"    {col} DOUBLE PRECISION")

        create_sql = f"""
        CREATE TABLE {table} (
            id SERIAL PRIMARY KEY,
{chr(10).join(f'        {cd},' for cd in col_defs[:-1])}
            {col_defs[-1]}
        );
        """
        cursor.execute(create_sql)
        conn.commit()
        print(f"   ✅ Tabla '{table}' creada exitosamente")

        # --- Bulk Insert con execute_values ---
        print(f"\n🚀 Insertando {len(df):,} registros (Bulk Insert)...")
        start = time.time()

        # Preparar la query de inserción
        cols_str = ", ".join(COLUMNS)
        insert_sql = f"INSERT INTO {table} ({cols_str}) VALUES %s"

        # Convertir DataFrame a lista de tuplas para execute_values
        records = [tuple(row) for row in df.itertuples(index=False, name=None)]

        # execute_values: mucho más rápido que executemany
        psycopg2.extras.execute_values(
            cursor,
            insert_sql,
            records,
            page_size=5000,  # Lotes de 5000 para optimizar red
        )

        conn.commit()
        rows_inserted = len(records)

        elapsed = time.time() - start
        rate = rows_inserted / elapsed if elapsed > 0 else 0
        print(f"   ✅ Inserción completada en {elapsed:.2f}s")
        print(f"   📈 Velocidad: {rate:,.0f} registros/segundo")

        # --- Verificación ---
        cursor.execute(f"SELECT COUNT(*) FROM {table};")
        count = cursor.fetchone()[0]
        print(f"\n🔍 Verificación: {count:,} registros en la tabla '{table}'")

    except psycopg2.Error as e:
        print(f"\n❌ Error de PostgreSQL: {e}")
        if conn:
            conn.rollback()
        raise

    except Exception as e:
        print(f"\n❌ Error inesperado: {e}")
        if conn:
            conn.rollback()
        raise

    finally:
        # Siempre cerrar cursor y conexión
        if cursor:
            cursor.close()
            print("\n🔒 Cursor cerrado")
        if conn:
            conn.close()
            print("🔒 Conexión a PostgreSQL cerrada")

    return rows_inserted


# ===========================================================================
# MAIN: ORQUESTACIÓN DEL PIPELINE
# ===========================================================================
def main():
    """Ejecuta el pipeline ETL completo."""
    print("=" * 65)
    print("  ETL Pipeline: NASA Turbofan Engine Degradation Simulation")
    print("=" * 65)
    pipeline_start = time.time()

    # PASO 1: Descarga
    zip_buffer = download_zip_to_memory(ZIP_URL)

    # PASO 2: Extracción y Transformación
    df = extract_and_transform(zip_buffer, TARGET_FILE)

    # PASO 3: Carga a PostgreSQL
    total_inserted = load_to_postgres(df, DB_CONNECTION_STRING, TABLE_NAME)

    # RESUMEN FINAL
    total_time = time.time() - pipeline_start
    print("\n" + "=" * 65)
    print(f"  ✅ ¡PIPELINE COMPLETADO EXITOSAMENTE!")
    print(f"  📦 Registros insertados: {total_inserted:,}")
    print(f"  ⏱️  Tiempo total: {total_time:.2f} segundos")
    print(f"  🗄️  Tabla destino: {TABLE_NAME}")
    print("=" * 65)


if __name__ == "__main__":
    main()
