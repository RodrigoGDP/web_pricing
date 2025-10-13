import sqlite3
import csv

DB_NAME = "database.db"
CSV_NAME = "unidades.csv" 

# CAMBIO: Definimos una lista de los proyectos que queremos cargar
PROYECTOS_VALIDOS = ["STILL", "COS", "PS"]

conn = sqlite3.connect(DB_NAME)
cursor = conn.cursor()
print("Conectado a la base de datos SQLite.")

cursor.execute("DROP TABLE IF EXISTS unidades")
print("Tabla 'unidades' antigua eliminada.")

# CAMBIO: Añadimos la columna 'nombre_proyecto'
cursor.execute("""
    CREATE TABLE unidades (
        codigo TEXT PRIMARY KEY,
        nombre TEXT,
        estado_comercial TEXT,
        precio_venta REAL,
        precio_lista REAL, 
        piso TEXT,
        nombre_tipologia TEXT,
        proformas_count INTEGER,
        nombre_proyecto TEXT
    )
""")
print("Tabla 'unidades' creada con 'nombre_proyecto' y las demás columnas.")

try:
    with open(CSV_NAME, 'r', encoding='utf-8') as csvfile:
        csv_reader = csv.DictReader(csvfile)
        unidades_a_insertar = []
        
        for row in csv_reader:
            project_code = row.get('codigo_proyecto', '').upper()
            
            # CAMBIO: El filtro ahora comprueba si el código del proyecto está en nuestra lista
            if project_code not in PROYECTOS_VALIDOS:
                continue

            # El resto de los filtros se mantienen
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
            except (ValueError, TypeError):
                precio_venta_float, precio_lista_float = 0.0, 0.0

            unidad = (
                row['codigo'], row['nombre'], row['estado_comercial'], 
                precio_venta_float, precio_lista_float,
                row['piso'], row['nombre_tipologia'],
                proformas_count,
                row['nombre_proyecto'] # <-- NUEVO DATO
            )
            unidades_a_insertar.append(unidad)

        # CAMBIO: Actualizamos la sentencia INSERT
        cursor.executemany("""
            INSERT INTO unidades (codigo, nombre, estado_comercial, precio_venta, precio_lista, piso, nombre_tipologia, proformas_count, nombre_proyecto)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, unidades_a_insertar)
        conn.commit()
        print(f"\nReporte de Carga: {len(unidades_a_insertar)} registros válidos insertados de los proyectos {PROYECTOS_VALIDOS}.")

except FileNotFoundError:
    print(f"ERROR: No se encontró el archivo '{CSV_NAME}'.")
except KeyError as e:
    print(f"ERROR: Falta una columna necesaria en tu CSV: {e}.")
finally:
    conn.close()
    print("Conexión a la base de datos cerrada.")