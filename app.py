import sqlite3
import random
from flask import Flask, render_template, request, redirect, url_for

# --- 1. INICIALIZACIÓN DE LA APLICACIÓN ---
app = Flask(__name__)
DB_NAME = "database.db"

# --- 2. FILTRO DE MONEDA PERSONALIZADO ---
@app.template_filter('currency')
def format_currency(value):
    if value is None: return "$0"
    return f"${value:,.0f}"

# --- 3. DATOS DE PRUEBA PARA EL DASHBOARD ORIGINAL ---
layout_overview_data = {
    "Tipo 1": {"sold_percentage": 42.8, "average_price": 358},
    "Tipo 5": {"sold_percentage": 52.5, "average_price": 324},
    "Tipo 8": {"sold_percentage": 61.2, "average_price": 317},
    "Tipo 11": {"sold_percentage": 50.4, "average_price": 302},
}

# --- 4. FUNCIÓN AUXILIAR PARA LA BASE DE DATOS ---
def get_db_connection():
    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = sqlite3.Row
    return conn

# --- 5. RUTAS DEL DASHBOARD ORIGINAL ---
@app.route('/')
def dashboard():
    return render_template('index.html', data={}, layout_data=layout_overview_data)

@app.route('/update-price/<layout_type>', methods=['POST'])
def update_price(layout_type):
    return "OK"

# --- 6. RUTA DE REDIRECCIÓN PARA LA PARRILLA ---
@app.route('/pricing')
def pricing_redirect():
    conn = get_db_connection()
    projects = conn.execute("SELECT DISTINCT nombre_proyecto FROM unidades ORDER BY nombre_proyecto").fetchall()
    conn.close()
    if projects:
        first_project = 'STILL' if 'STILL' in [p['nombre_proyecto'] for p in projects] else projects[0]['nombre_proyecto']
        return redirect(url_for('pricing', project_name=first_project))
    return "No hay proyectos cargados en la base de datos."

# --- 7. RUTA PRINCIPAL PARA LA PARRILLA DE PRECIOS ---
@app.route('/pricing/<project_name>')
def pricing(project_name):
    tipologia_filtro = request.args.get('tipologia')
    vista_actual = request.args.get('vista', 'precio')
    
    conn = get_db_connection()
    all_projects = conn.execute("SELECT DISTINCT nombre_proyecto FROM unidades ORDER BY nombre_proyecto").fetchall()
    units_from_db = conn.execute("SELECT * FROM unidades WHERE nombre_proyecto = ?", (project_name,)).fetchall()
    conn.close()

    if not units_from_db:
        return render_template('pricing_grid.html', grid={}, all_tipologias=[], all_projects=all_projects, current_project=project_name, approval_table_data=[], legend_data={'red': 0, 'green': 0, 'gray': 0})

    all_tipologias = sorted(list(set(u['nombre_tipologia'] for u in units_from_db)))
    
    tipologias_data_grouped = {t: [u for u in units_from_db if u['nombre_tipologia'] == t] for t in all_tipologias}
    unidades_con_alerta = set()
    for tipologia, units_in_tipo in tipologias_data_grouped.items():
        total_units = len(units_in_tipo)
        sold_units = sum(1 for u in units_in_tipo if u['estado_comercial'].lower() == 'vendido')
        sold_percentage = (sold_units / total_units * 100) if total_units > 0 else 0
        if sold_percentage >= 20.0:
            for u in units_in_tipo:
                if u['estado_comercial'].lower() != 'vendido':
                    unidades_con_alerta.add(u['codigo'])

    approval_table_data = []
    legend_data = {'red': 0, 'green': 0, 'gray': 0}
    
    for tipologia_name, units_in_tipo in tipologias_data_grouped.items():
        total_proformas = sum(u['proformas_count'] for u in units_in_tipo)
        precios_m2 = [u['precio_m2'] for u in units_in_tipo if u['precio_m2'] > 0]
        avg_precio_m2 = sum(precios_m2) / len(precios_m2) if precios_m2 else 0
        total_count = len(units_in_tipo)
        available_count = sum(1 for u in units_in_tipo if u['estado_comercial'].lower() != 'vendido')
        has_alert = any(u['codigo'] in unidades_con_alerta for u in units_in_tipo)
        approval_table_data.append({
            'tipologia': tipologia_name,
            'unidades_disponibles_str': f"{available_count}/{total_count}",
            'total_proformas': total_proformas,
            'avg_precio_m2': avg_precio_m2,
            'has_alert': has_alert,
        })

    grid_data = {}
    for unit in units_from_db:
        if unit['estado_comercial'].lower() == 'vendido': legend_data['gray'] += 1
        elif unit['codigo'] in unidades_con_alerta: legend_data['red'] += 1
        else: legend_data['green'] += 1
        piso = unit['piso']
        if piso not in grid_data: grid_data[piso] = []
        alerta_status = 'subir_precio' if unit['codigo'] in unidades_con_alerta else 'normal'
        css_class = 'difuminado' if tipologia_filtro and unit['nombre_tipologia'] != tipologia_filtro else ''
        processed_unit = {
            'codigo': unit['codigo'], 'estado_comercial': unit['estado_comercial'],
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

    if request.headers.get('HX-Request') == 'true':
        # CAMBIO: Ahora renderiza el nuevo parcial que incluye los filtros y la parrilla
        return render_template('_filters_and_grid.html',
                               grid=sorted_grid_data,
                               all_tipologias=all_tipologias,
                               current_project=project_name,
                               tipologia_filtro=tipologia_filtro,
                               vista_actual=vista_actual,
                               max_columns=max_columns)
    else:
        return render_template('pricing_grid.html', 
                               grid=sorted_grid_data, all_tipologias=all_tipologias,
                               all_projects=all_projects, current_project=project_name,
                               tipologia_filtro=tipologia_filtro, vista_actual=vista_actual,
                               max_columns=max_columns,
                               approval_table_data=approval_table_data,
                               legend_data=legend_data)

# --- 8. INICIO DE LA APLICACIÓN ---
if __name__ == '__main__':
    app.run(debug=True)