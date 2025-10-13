import sqlite3
import csv

DB_NAME = "database.db"
CSV_NAME = "unidades.csv"
PROYECTOS_VALIDOS = ["STILL", "COS", "PS"]

conn = sqlite3.connect(DB_NAME)
cursor = conn.cursor()
print("Conectado a la base de datos SQLite.")

cursor.execute("DROP TABLE IF EXISTS unidades")
print("Tabla 'unidades' antigua eliminada.")

# CAMBIO: Añadimos la columna 'precio_m2'
cursor.execute("""
    CREATE TABLE unidades (
        codigo TEXT PRIMARY KEY, nombre TEXT, estado_comercial TEXT,
        precio_venta REAL, precio_lista REAL, precio_m2 REAL,
        piso TEXT, nombre_tipologia TEXT, proformas_count INTEGER,
        nombre_proyecto TEXT
    )
""")
print("Tabla 'unidades' creada con 'precio_m2' y las demás columnas.")

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

            proforma_codes = row.get('codigo_proforma', '')
            proformas_count = len(proforma_codes.split(',')) if proforma_codes and not proforma_codes.isspace() else 0

            try:
                precio_venta_float = float(row.get('precio_venta') or '0')
                precio_lista_float = float(row.get('precio_lista') or '0')
                precio_m2_float = float(row.get('precio_m2') or '0') # <-- NUEVO
            except (ValueError, TypeError):
                precio_venta_float, precio_lista_float, precio_m2_float = 0.0, 0.0, 0.0

            unidad = (
                row['codigo'], row['nombre'], row['estado_comercial'], 
                precio_venta_float, precio_lista_float, precio_m2_float, # <-- NUEVO
                row['piso'], row['nombre_tipologia'],
                proformas_count, row['nombre_proyecto']
            )
            unidades_a_insertar.append(unidad)

        cursor.executemany("""
            INSERT INTO unidades (codigo, nombre, estado_comercial, precio_venta, precio_lista, precio_m2, piso, nombre_tipologia, proformas_count, nombre_proyecto)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, unidades_a_insertar)
        conn.commit()
        print(f"\nReporte de Carga: {len(unidades_a_insertar)} registros válidos insertados.")

except FileNotFoundError:
    print(f"ERROR: No se encontró el archivo '{CSV_NAME}'.")
except KeyError as e:
    print(f"ERROR: Falta una columna necesaria en tu CSV: {e}.")
finally:
    conn.close()
    print("Conexión a la base de datos cerrada.")