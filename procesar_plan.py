import pandas as pd

def procesar_plan_estudios(ruta_archivo):
    """
    Lee un archivo CSV con la estructura Unidad, Tema, Subtema y lo convierte
    en una estructura de datos anidada para luego imprimirlo.
    """
    try:
        # 1. Leer el archivo CSV en un DataFrame de pandas
        df = pd.read_csv(ruta_archivo)
    except FileNotFoundError:
        print(f"Error: No se encontró el archivo en la ruta '{ruta_archivo}'")
        return

    # 2. Estructura para almacenar el plan completo
    plan_estudios = []
    unidad_actual = None
    tema_actual = None

    # 3. Iterar sobre cada fila del DataFrame
    for _, fila in df.iterrows():
        # --- Nivel 1: Unidad ---
        # Si la columna 'Unidad' no está vacía, es una nueva unidad.
        if pd.notna(fila['Unidad']):
            unidad_actual = {
                "nombre": fila['Unidad'],
                "temas": []
            }
            plan_estudios.append(unidad_actual)
            # Reiniciamos el tema actual ya que estamos en una nueva unidad
            tema_actual = None

        # --- Nivel 2: Tema ---
        # Si la columna 'Tema' no está vacía y tenemos una unidad activa.
        if pd.notna(fila['Tema']) and unidad_actual is not None:
            tema_actual = {
                "nombre": fila['Tema'],
                "subtemas": []
            }
            unidad_actual["temas"].append(tema_actual)

        # --- Nivel 3: Subtema ---
        # Si la columna 'Subtema' no está vacía y tenemos un tema activo.
        if pd.notna(fila['Subtema']) and tema_actual is not None:
            tema_actual["subtemas"].append(fila['Subtema'])

    return plan_estudios

def imprimir_plan(plan):
    """
    Imprime el plan de estudios de forma formateada.
    """
    if not plan:
        print("El plan de estudios está vacío o no se pudo procesar.")
        return

    print("="*50)
    print("        PLAN DE ESTUDIOS DETALLADO")
    print("="*50)

    for unidad in plan:
        print(f"\n■ {unidad['nombre']}")
        for tema in unidad['temas']:
            print(f"  ● {tema['nombre']}")
            for subtema in tema['subtemas']:
                print(f"    - {subtema}")
    print("\n" + "="*50)


# --- Ejecución Principal ---
if __name__ == "__main__":
    # Nombre del archivo que subiste
    nombre_archivo_csv = 'unidades.csv'
    
    # Procesar el plan
    mi_plan = procesar_plan_estudios(nombre_archivo_csv)
    
    # Imprimir el resultado
    imprimir_plan(mi_plan)
