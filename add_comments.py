"""
Agrega comentarios descriptivos a las columnas de turbofan_telemetry en PostgreSQL.
"""
import os

import psycopg2
from dotenv import load_dotenv

# Cargar variables de entorno desde .env
load_dotenv()

DB_CONNECTION_STRING = os.environ["DATABASE_URL"]

COLUMN_COMMENTS = {
    "s1_temp_inlet":      "Temp entrada ventilador",
    "s2_pres_inlet":      "Presion entrada compresor",
    "s3_temp_lpc":        "Temp compresor baja presion",
    "s4_temp_hpc":        "Temp compresor alta presion",
    "s5_pres_lpc":        "Presion salida compresor baja",
    "s6_pres_hpc":        "Presion salida compresor alta",
    "s7_fan_speed":       "Velocidad ventilador",
    "s8_core_speed":      "Velocidad nucleo",
    "s9_bypass_ratio":    "Relacion de derivacion",
    "s10_burner_pres":    "Presion quemador",
    "s11_bleed_enthalpy": "Entalpia purga",
    "s12_hpt_speed":      "Velocidad turbina alta",
    "s13_lpt_speed":      "Velocidad turbina baja",
    "s14_static_pres":    "Presion estatica",
    "s15_fuel_flow":      "Flujo combustible",
    "s16_temp_exhaust":   "Temp escape",
    "s17_pres_exhaust":   "Presion escape",
    "s18_vibration":      "Vibracion de eje",
    "s19_shaft_speed":    "Velocidad eje secundario",
    "s20_temp_ambient":   "Temp ambiente",
    "s21_coolant_bleed":  "Purga refrigerante",
}

conn = None
try:
    print("Conectando a PostgreSQL (Neon)...")
    conn = psycopg2.connect(DB_CONNECTION_STRING)
    cur = conn.cursor()
    print("Conexion establecida\n")

    # Agregar comentarios a cada columna de sensor
    for col, desc in COLUMN_COMMENTS.items():
        sql = f"COMMENT ON COLUMN turbofan_telemetry.{col} IS '{desc}';"
        cur.execute(sql)
        print(f"   OK: {col:25s} -> {desc}")

    conn.commit()
    print("\nComentarios aplicados. Verificando...\n")

    # Verificar que los comentarios se guardaron
    cur.execute("""
        SELECT column_name, col_description(
            (SELECT oid FROM pg_class WHERE relname = 'turbofan_telemetry'),
            ordinal_position
        ) as comment
        FROM information_schema.columns
        WHERE table_name = 'turbofan_telemetry'
        ORDER BY ordinal_position;
    """)

    print("--- Estructura final de la tabla ---")
    for row in cur.fetchall():
        comment = row[1] if row[1] else "(sin comentario)"
        print(f"   {row[0]:25s} | {comment}")

    # Verificar que los datos siguen intactos
    cur.execute("SELECT COUNT(*) FROM turbofan_telemetry;")
    count = cur.fetchone()[0]
    print(f"\n   Registros en tabla: {count:,}")

    cur.close()
    print("\n   Comentarios agregados exitosamente!")

except Exception as e:
    print(f"Error: {e}")
    if conn:
        conn.rollback()
finally:
    if conn:
        conn.close()
        print("   Conexion cerrada.")
