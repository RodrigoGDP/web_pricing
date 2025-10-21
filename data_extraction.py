import psycopg2
import os
import csv
from dotenv import load_dotenv
from loguru import logger
from pathlib import Path

# Cargar variables de entorno desde .env
load_dotenv()

# --- Configuración de Loguru ---
logger.add("logs/data_extraction.log", rotation="10 MB", level="INFO")

# --- Configuración de la base de datos Redshift ---
REDSHIFT_HOST: str = os.getenv("REDSHIFT_HOST")
REDSHIFT_PORT: str = os.getenv("REDSHIFT_PORT")
REDSHIFT_DB: str = os.getenv("REDSHIFT_DB")
REDSHIFT_USER: str = os.getenv("REDSHIFT_USER")
REDSHIFT_PASSWORD: str = os.getenv("REDSHIFT_PASSWORD")

if not all([REDSHIFT_HOST, REDSHIFT_PORT, REDSHIFT_DB, REDSHIFT_USER, REDSHIFT_PASSWORD]):
    logger.error("Faltan una o más credenciales de Redshift en el archivo .env.")
    raise ValueError("Credenciales de Redshift incompletas.")

# --- Configuración de Rutas y Esquema ---
PROJECT_ROOT: Path = Path(__file__).resolve().parent
TARGET_SCHEMA: str = "llosaedificaciones"

# Tablas específicas a extraer
TABLES_TO_EXTRACT = ["unidades", "proforma_unidad"]

def download_table_as_csv(conn, schema: str, table: str):
    """
    Descarga una tabla específica desde Redshift y la guarda como un archivo CSV.
    """
    output_path = PROJECT_ROOT / f"{table}.csv"
    full_table_name = f'"{schema}"."{table}"'
    logger.info(f"Descargando tabla '{full_table_name}' a '{output_path}'...")

    sql_query = f"SELECT * FROM {full_table_name};"

    try:
        with conn.cursor() as cursor:
            cursor.execute(sql_query)
            
            # Obtener encabezados de las columnas
            headers = [desc[0] for desc in cursor.description]
            
            with open(output_path, "w", encoding="utf-8", newline='') as f:
                writer = csv.writer(f)
                
                # Escribir la fila de encabezados
                writer.writerow(headers)
                
                # Escribir las filas de datos
                writer.writerows(cursor.fetchall())

        logger.success(f"Tabla '{full_table_name}' guardada exitosamente en '{output_path}'.")
    except psycopg2.Error as e:
        logger.error(f"Error al descargar la tabla '{full_table_name}': {e}")
        # Si hay un error, borramos el archivo parcial que se pudo haber creado
        if output_path.exists():
            output_path.unlink()
        raise

def main():
    """
    Función principal para extraer las tablas específicas.
    """
    conn = None
    try:
        logger.info("Conectando a la base de datos de Redshift...")
        conn = psycopg2.connect(
            host=REDSHIFT_HOST,
            port=REDSHIFT_PORT,
            dbname=REDSHIFT_DB,
            user=REDSHIFT_USER,
            password=REDSHIFT_PASSWORD
        )
        logger.info("Conexión a Redshift establecida.")

        for table_name in TABLES_TO_EXTRACT:
            try:
                download_table_as_csv(conn, TARGET_SCHEMA, table_name)
            except Exception as e:
                logger.error(f"Error al procesar la tabla '{table_name}': {e}")
                continue

        logger.info("Proceso de extracción completado.")

    except psycopg2.OperationalError as e:
        logger.error(f"Error de conexión a Redshift: {e}")
        logger.error("Verifica las credenciales, la red y los permisos de IP en los Security Groups de Redshift.")
    except Exception as e:
        logger.error(f"Ocurrió un error inesperado en el proceso principal: {e}")
    finally:
        if conn:
            conn.close()
            logger.info("Conexión a Redshift cerrada.")

if __name__ == "__main__":
    main()