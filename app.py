import sqlite3
import random
from flask import Flask, render_template, request, redirect, url_for

app = Flask(__name__)
DB_NAME = "database.db"

# ... (El filtro de moneda y la función get_db_connection se mantienen igual) ...
@app.template_filter('currency')
def format_currency(value):
    if value is None: return "$0"
    return f"${value:,.0f}"

def get_db_connection():
    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = sqlite3.Row
    return conn

# --- NUEVO: Redirección para la ruta principal ---
@app.route('/')
def index():
    # Redirige automáticamente al primer proyecto disponible (o a STILL si existe)
    conn = get_db_connection()
    projects = conn.execute("SELECT DISTINCT nombre_proyecto FROM unidades ORDER BY nombre_proyecto").fetchall()
    conn.close()
    if projects:
        first_project = 'STILL' if 'STILL' in [p['nombre_proyecto'] for p in projects] else projects[0]['nombre_proyecto']
        return redirect(url_for('pricing', project_name=first_project))
    return "No hay proyectos cargados en la base de datos."


# --- NUEVA RUTA DE PRICING GENÉRICA ---
@app.route('/pricing/<project_name>')
def pricing(project_name):
    # --- Lógica de la parrilla (antes en still_grid) ---
    tipologia_filtro = request.args.get('tipologia')
    vista_actual = request.args.get('vista', 'precio')
    
    conn = get_db_connection()
    # CAMBIO: Obtenemos todos los proyectos para el selector
    all_projects = conn.execute("SELECT DISTINCT nombre_proyecto FROM unidades ORDER BY nombre_proyecto").fetchall()
    
    # CAMBIO CRÍTICO: Filtramos las unidades por el proyecto seleccionado en la URL
    units_from_db = conn.execute("SELECT * FROM unidades WHERE nombre_proyecto = ?", (project_name,)).fetchall()
    conn.close()

    # El resto de la lógica ahora opera solo con los datos del proyecto filtrado
    if not units_from_db:
        return render_template('pricing_grid.html', grid={}, all_tipologias=[], all_projects=all_projects, current_project=project_name)

    all_tipologias = sorted(list(set(u['nombre_tipologia'] for u in units_from_db)))
    
    # ... (La lógica de alertas y construcción del grid se mantiene igual) ...
    tipologias_data = {}
    for unit in units_from_db:
        tipologia = unit['nombre_tipologia']
        if tipologia not in tipologias_data: tipologias_data[tipologia] = []
        tipologias_data[tipologia].append(unit)
    unidades_con_alerta = set()
    for tipologia, units_in_tipo in tipologias_data.items():
        total_units = len(units_in_tipo)
        sold_units = sum(1 for u in units_in_tipo if u['estado_comercial'].lower() == 'vendido')
        sold_percentage = (sold_units / total_units * 100) if total_units > 0 else 0
        if sold_percentage >= 20.0:
            for u in units_in_tipo:
                if u['estado_comercial'].lower() != 'vendido': unidades_con_alerta.add(u['codigo'])
    grid_data = {}
    for unit in units_from_db:
        piso = unit['piso']
        if piso not in grid_data: grid_data[piso] = []
        alerta_status = 'normal'
        if unit['codigo'] in unidades_con_alerta: alerta_status = 'subir_precio'
        css_class = ''
        if tipologia_filtro and unit['nombre_tipologia'] != tipologia_filtro: css_class = 'difuminado'
        processed_unit = {
            'codigo': unit['codigo'], 'estado_comercial': unit['estado_comercial'].strip().capitalize(),
            'precio_venta': unit['precio_venta'], 'precio_lista': unit['precio_lista'],
            'nombre_tipologia': unit['nombre_tipologia'], 'alerta_status': alerta_status,
            'proformas_count': unit['proformas_count'], 'css_class': css_class
        }
        grid_data[piso].append(processed_unit)
    try:
        sorted_floors = sorted(grid_data.keys(), key=lambda p: int(''.join(filter(str.isdigit, p or '0'))), reverse=True)
    except (ValueError, TypeError):
        sorted_floors = sorted(grid_data.keys(), reverse=True)
    sorted_grid_data = {floor: grid_data[floor] for floor in sorted_floors}
    max_columns = max(len(units) for units in sorted_grid_data.values()) if sorted_grid_data else 0

    # Lógica para decidir qué plantilla renderizar (sin cambios)
    if request.headers.get('HX-Request') == 'true':
        return render_template('_grid_container.html', grid=sorted_grid_data, max_columns=max_columns, vista_actual=vista_actual)
    else:
        return render_template('pricing_grid.html', 
                               grid=sorted_grid_data, 
                               all_tipologias=all_tipologias,
                               all_projects=all_projects,
                               current_project=project_name,
                               tipologia_filtro=tipologia_filtro,
                               vista_actual=vista_actual,
                               max_columns=max_columns)

if __name__ == '__main__':
    app.run(debug=True)