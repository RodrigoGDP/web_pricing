import sqlite3
import random
from flask import Flask, render_template, request, redirect, url_for

# --- 1. INICIALIZACIÓN DE LA APLICACIÓN ---
app = Flask(__name__)
DB_NAME = "database.db"

# --- 2. FILTRO DE MONEDA PERSONALIZADO ---
@app.template_filter('currency')
def format_currency(value):
    """Formatea un número como una moneda con comas y el símbolo de dólar."""
    if value is None:
        return "$0"
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
    """Renderiza el dashboard principal."""
    # (La lógica completa del dashboard se mantiene)
    return render_template('index.html', data={}, layout_data=layout_overview_data)

@app.route('/update-price/<layout_type>', methods=['POST'])
def update_price(layout_type):
    # (Esta función se mantiene para el dashboard original)
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
    # --- A. CAPTURA DE FILTROS Y CARGA DE DATOS ---
    tipologia_filtro = request.args.get('tipologia')
    vista_actual = request.args.get('vista', 'precio')
    
    conn = get_db_connection()
    all_projects = conn.execute("SELECT DISTINCT nombre_proyecto FROM unidades ORDER BY nombre_proyecto").fetchall()
    units_from_db = conn.execute("SELECT * FROM unidades WHERE nombre_proyecto = ?", (project_name,)).fetchall()
    conn.close()

    if not units_from_db:
        return render_template('pricing_grid.html', grid={}, all_tipologias=[], all_projects=all_projects, current_project=project_name)

    all_tipologias = sorted(list(set(u['nombre_tipologia'] for u in units_from_db)))
    
    # --- B. LÓGICA DE ALERTAS DEL 20% ---
    tipologias_data_grouped = {}
    for unit in units_from_db:
        tipologia = unit['nombre_tipologia']
        if tipologia not in tipologias_data_grouped:
            tipologias_data_grouped[tipologia] = []
        tipologias_data_grouped[tipologia].append(unit)

    unidades_con_alerta = set()
    for tipologia, units_in_tipo in tipologias_data_grouped.items():
        total_units = len(units_in_tipo)
        sold_units = sum(1 for u in units_in_tipo if u['estado_comercial'].lower() == 'vendido')
        sold_percentage = (sold_units / total_units * 100) if total_units > 0 else 0
        if sold_percentage >= 20.0:
            for u in units_in_tipo:
                if u['estado_comercial'].lower() != 'vendido':
                    unidades_con_alerta.add(u['codigo'])

    # --- C. CÁLCULOS PARA LA TABLA DE APROBACIÓN Y LEYENDA ---
    approval_table_data = []
    legend_data = {'red': 0, 'green': 0, 'gray': 0}
    
    for tipologia_name in all_tipologias:
        units_in_tipo = tipologias_data_grouped.get(tipologia_name, [])
        total_proformas = sum(u['proformas_count'] for u in units_in_tipo)
        precios_m2 = [u['precio_m2'] for u in units_in_tipo if u['precio_m2'] > 0]
        avg_precio_m2 = sum(precios_m2) / len(precios_m2) if precios_m2 else 0
        approval_table_data.append({
            'tipologia': tipologia_name,
            'total_proformas': total_proformas,
            'avg_precio_m2': avg_precio_m2,
        })

    # --- D. CONSTRUCCIÓN DE LA PARRILLA Y CÁLCULO DE LEYENDA ---
    grid_data = {}
    for unit in units_from_db:
        # Lógica de Leyenda
        if unit['estado_comercial'].lower() == 'vendido':
            legend_data['gray'] += 1
        elif unit['codigo'] in unidades_con_alerta:
            legend_data['red'] += 1
        else: # Disponible y sin alerta
            legend_data['green'] += 1

        # Lógica de Parrilla
        piso = unit['piso']
        if piso not in grid_data:
            grid_data[piso] = []
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

    # --- E. RENDERIZADO FINAL (HTMX o carga completa) ---
    if request.headers.get('HX-Request') == 'true':
        return render_template('_grid_and_filters_partial.html',
                               grid=sorted_grid_data, all_tipologias=all_tipologias,
                               current_project=project_name, tipologia_filtro=tipologia_filtro,
                               vista_actual=vista_actual, max_columns=max_columns)
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