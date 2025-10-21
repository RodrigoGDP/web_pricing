import sqlite3
import csv
from collections import defaultdict

DB_NAME = "database.db"
CSV_NAME = "unidades.csv"
PROFORMA_CSV_NAME = "proforma_unidad.csv"
PROYECTOS_VALIDOS = ["STILL", "COS", "PS", "ANG", "NUN"]

conn = sqlite3.connect(DB_NAME)
cursor = conn.cursor()
print("Conectado a la base de datos SQLite.")

cursor.execute("DROP TABLE IF EXISTS unidades")
print("Tabla 'unidades' antigua eliminada.")

cursor.execute("""
    CREATE TABLE unidades (
        codigo TEXT PRIMARY KEY, nombre TEXT, estado_comercial TEXT,
        precio_venta REAL, precio_lista REAL, precio_m2 REAL, area_techada REAL,
        piso TEXT, nombre_tipologia TEXT, proformas_count INTEGER,
        nombre_proyecto TEXT
    )
""")
print("Tabla 'unidades' creada con todas las columnas.")

# Leer proforma_unidad.csv y contar proformas por codigo_unidad
print(f"Leyendo proformas desde '{PROFORMA_CSV_NAME}'...")
proformas_por_unidad = defaultdict(int)
try:
    with open(PROFORMA_CSV_NAME, 'r', encoding='utf-8') as proforma_file:
        proforma_reader = csv.DictReader(proforma_file)
        for proforma_row in proforma_reader:
            codigo_unidad = proforma_row.get('codigo_unidad', '').strip()
            if codigo_unidad:
                proformas_por_unidad[codigo_unidad] += 1
    print(f"Se encontraron proformas para {len(proformas_por_unidad)} unidades.")
except FileNotFoundError:
    print(f"ADVERTENCIA: No se encontró el archivo '{PROFORMA_CSV_NAME}'. Se usará 0 proformas para todas las unidades.")
    proformas_por_unidad = defaultdict(int)

try:
    with open(CSV_NAME, 'r', encoding='utf-8') as csvfile:
        csv_reader = csv.DictReader(csvfile)
        unidades_a_insertar = []
        
        for row in csv_reader:
            project_code = row.get('codigo_proyecto', '').upper()
            if project_code not in PROYECTOS_VALIDOS: continue
            if "estacionamiento" in row.get('tipo_unidad', '').lower(): continue
            try:
                if int(row.get('piso', '-1')) < 0: continue
            except (ValueError, TypeError):
                continue

            # NUEVO: Obtener el conteo de proformas desde proforma_unidad.csv
            codigo_unidad = row['codigo']
            proformas_count = proformas_por_unidad.get(codigo_unidad, 0)

            try:
                precio_venta_float = float(row.get('precio_venta') or '0')
                precio_lista_float = float(row.get('precio_lista') or '0')
                precio_m2_float = float(row.get('precio_m2') or '0')
                area_techada_float = float(row.get('area_techada') or '0')
            except (ValueError, TypeError):
                precio_venta_float, precio_lista_float, precio_m2_float, area_techada_float = 0.0, 0.0, 0.0, 0.0

            unidad = (
                codigo_unidad, row['nombre'], row['estado_comercial'],
                precio_venta_float, precio_lista_float, precio_m2_float, area_techada_float,
                row['piso'], row['nombre_tipologia'],
                proformas_count, row['nombre_proyecto']
            )
            unidades_a_insertar.append(unidad)

        # La sentencia INSERT ya es correcta, no necesita cambios
        cursor.executemany("""
            INSERT INTO unidades (codigo, nombre, estado_comercial, precio_venta, precio_lista, precio_m2, area_techada, piso, nombre_tipologia, proformas_count, nombre_proyecto)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, unidades_a_insertar)
        conn.commit()
        print(f"\nReporte de Carga: {len(unidades_a_insertar)} registros válidos insertados.")

except FileNotFoundError:
    print(f"ERROR: No se encontró el archivo '{CSV_NAME}'.")
except KeyError as e:
    print(f"ERROR: Falta una columna necesaria en tu CSV: {e}.")
# Si el error de UNIQUE constraint vuelve a aparecer, es porque hay códigos duplicados en tu CSV.
# En ese caso, la solución 'INSERT OR IGNORE' que vimos antes sería la correcta.
except sqlite3.IntegrityError as e:
    print(f"ERROR DE BASE DE DATOS: {e}. Esto probablemente significa que tienes 'códigos' duplicados en tu archivo unidades.csv.")
finally:
    conn.close()
    print("Conexión a la base de datos cerrada.")