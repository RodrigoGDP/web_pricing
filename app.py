import sqlite3
import random
from flask import Flask, render_template, request, redirect, url_for

# --- 1. INICIALIZACIÓN DE LA APLICACIÓN ---
app = Flask(__name__)

# Cache para max_columns por proyecto
project_max_columns = {}
DB_NAME = "database.db"

def safe_get(row, key, default=None):
    """Función auxiliar para obtener valores de sqlite3.Row de manera segura"""
    try:
        value = row[key]
        return value if value is not None else default
    except (KeyError, IndexError):
        return default

def get_max_columns_for_project(project_name, units_from_db):
    """Obtiene el max_columns para un proyecto, usando cache si está disponible"""
    if project_name not in project_max_columns:
        # Calcular max_columns basado en TODAS las unidades del proyecto
        grid_data_for_max_calc = {}
        for unit in units_from_db:
            piso = safe_get(unit, 'piso', '')
            if piso not in grid_data_for_max_calc:
                grid_data_for_max_calc[piso] = []
            grid_data_for_max_calc[piso].append(unit)
        project_max_columns[project_name] = max(len(units) for units in grid_data_for_max_calc.values()) if grid_data_for_max_calc else 0
    return project_max_columns[project_name]

# --- INICIO DE LA SOLUCIÓN ---
@app.route('/')
def index():
    # Redirige a la página de precios de un proyecto por defecto (ej. "STILL")
    # Puedes cambiar "STILL" por cualquier otro de tus proyectos válidos.
    return redirect(url_for('pricing', project_name='STILL'))
# --- FIN DE LA SOLUCIÓN ---

# --- 2. FILTRO DE MONEDA PERSONALIZADO ---
@app.template_filter('currency')
def format_currency(value):
    if value is None: return "$0"
    return f"${value:,.0f}"

# --- SE ELIMINA EL DICCIONARIO layout_overview_data ---

# --- 4. FUNCIÓN AUXILIAR PARA LA BASE DE DATOS ---
def get_db_connection():
    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = sqlite3.Row
    return conn

# --- FUNCIÓN AUXILIAR PARA ACCESO SEGURO A ROWS ---

# --- SE ELIMINAN LAS RUTAS ANTIGUAS DEL DASHBOARD ---

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
    tipologia_filtro = request.args.getlist('tipologia')
    # Limpiar la lista de tipologías (remover valores vacíos)
    tipologia_filtro = [t for t in tipologia_filtro if t.strip()]
    vista_actual = request.args.get('vista', 'precio')
    
    conn = get_db_connection()
    all_projects = conn.execute("SELECT DISTINCT nombre_proyecto FROM unidades ORDER BY nombre_proyecto").fetchall()
    units_from_db = conn.execute("SELECT * FROM unidades WHERE nombre_proyecto = ?", (project_name,)).fetchall()
    conn.close()

    if not units_from_db:
        return render_template('pricing_grid.html', grid={}, all_tipologias=[], all_projects=all_projects, current_project=project_name, approval_table_data=[], legend_data={'red': 0, 'green': 0, 'gray': 0})

    all_tipologias = sorted(list(set(safe_get(u, 'nombre_tipologia', '') for u in units_from_db if safe_get(u, 'nombre_tipologia'))))
    
    tipologias_data_grouped = {t: [u for u in units_from_db if safe_get(u, 'nombre_tipologia', '') == t] for t in all_tipologias}
    unidades_con_alerta = set()
    for tipologia, units_in_tipo in tipologias_data_grouped.items():
        total_units = len(units_in_tipo)
        sold_units = sum(1 for u in units_in_tipo if (safe_get(u, 'estado_comercial', '') or '').lower() == 'vendido')
        sold_percentage = (sold_units / total_units * 100) if total_units > 0 else 0
        if sold_percentage >= 20.0:
            for u in units_in_tipo:
                # --- LÓGICA DE ALERTA CORREGIDA ---
                # La alerta solo debe aplicar a unidades DISPONIBLES, no a las separadas.
                estado_lower = (safe_get(u, 'estado_comercial', '') or '').lower()
                if estado_lower not in ['vendido', 'separado', 'proceso de separacion']:
                    unidades_con_alerta.add(safe_get(u, 'codigo', ''))

    approval_table_data = []
    
    # Calcular estadísticas dinámicas basadas en filtros de tipologías
    filtered_units = units_from_db
    if tipologia_filtro:
        # Si hay filtros específicos, filtrar las unidades
        filtered_units = [u for u in units_from_db if safe_get(u, 'nombre_tipologia', '') in tipologia_filtro]
    
    # Calcular estadísticas para el sidebar basadas en unidades filtradas
    sidebar_stats = {
        'total_unidades': len(filtered_units),
        'suma_precio': sum(
            (safe_get(u, 'precio_venta', 0) or 0) if (safe_get(u, 'estado_comercial', '') or '').lower() == 'vendido' 
            else (safe_get(u, 'precio_lista', 0) or 0) 
            for u in filtered_units
        ),
        'suma_area_total': sum(safe_get(u, 'area_techada', 0) or 0 for u in filtered_units),
        'suma_proformas': sum(safe_get(u, 'proformas_count', 0) or 0 for u in filtered_units)
    }
    
    # Calcular legend_data basado en unidades filtradas
    legend_data = {'red': 0, 'green': 0, 'gray': 0, 'yellow': 0}
    legend_stats = {
        'red': {'unidades': 0, 'precio': 0, 'area': 0, 'proformas': 0},
        'green': {'unidades': 0, 'precio': 0, 'area': 0, 'proformas': 0},
        'yellow': {'unidades': 0, 'precio': 0, 'area': 0, 'proformas': 0},
        'gray': {'unidades': 0, 'precio': 0, 'area': 0, 'proformas': 0}
    }
    
    # Procesar cada unidad filtrada para calcular estadísticas por estado
    for unit in filtered_units:
        estado_lower = (safe_get(unit, 'estado_comercial', '') or '').lower()
        precio = (safe_get(unit, 'precio_venta', 0) or 0) if estado_lower == 'vendido' else (safe_get(unit, 'precio_lista', 0) or 0)
        area = safe_get(unit, 'area_techada', 0) or 0
        proformas = safe_get(unit, 'proformas_count', 0) or 0
        
        if estado_lower == 'vendido':
            legend_data['gray'] += 1
            legend_stats['gray']['unidades'] += 1
            legend_stats['gray']['precio'] += precio
            legend_stats['gray']['area'] += area
            legend_stats['gray']['proformas'] += proformas
        elif estado_lower in ['separado', 'proceso de separacion']:
            legend_data['yellow'] += 1
            legend_stats['yellow']['unidades'] += 1
            legend_stats['yellow']['precio'] += precio
            legend_stats['yellow']['area'] += area
            legend_stats['yellow']['proformas'] += proformas
        elif safe_get(unit, 'codigo', '') in unidades_con_alerta:
            legend_data['red'] += 1
            legend_stats['red']['unidades'] += 1
            legend_stats['red']['precio'] += precio
            legend_stats['red']['area'] += area
            legend_stats['red']['proformas'] += proformas
        else:
            legend_data['green'] += 1
            legend_stats['green']['unidades'] += 1
            legend_stats['green']['precio'] += precio
            legend_stats['green']['area'] += area
            legend_stats['green']['proformas'] += proformas
    
    for tipologia_name, units_in_tipo in tipologias_data_grouped.items():
        total_proformas = sum(safe_get(u, 'proformas_count', 0) or 0 for u in units_in_tipo)
        precios_m2 = [safe_get(u, 'precio_m2', 0) or 0 for u in units_in_tipo if safe_get(u, 'precio_m2', 0) and safe_get(u, 'precio_m2', 0) > 0]
        avg_precio_m2 = sum(precios_m2) / len(precios_m2) if precios_m2 else 0
        total_count = len(units_in_tipo)
        available_count = sum(1 for u in units_in_tipo if safe_get(u, 'estado_comercial', '').lower() != 'vendido')
        has_alert = any(safe_get(u, 'codigo', '') in unidades_con_alerta for u in units_in_tipo)
        approval_table_data.append({
            'tipologia': tipologia_name,
            'unidades_disponibles_str': f"{available_count}/{total_count}",
            'total_proformas': total_proformas,
            'avg_precio_m2': avg_precio_m2,
            'has_alert': has_alert,
        })

    # Obtener max_columns: usar parámetro si está disponible, sino usar cache
    max_columns_param = request.args.get('max_columns')
    if max_columns_param:
        max_columns = int(max_columns_param)
    else:
        max_columns = get_max_columns_for_project(project_name, units_from_db)
    
    grid_data = {}
    for unit in units_from_db:
        # --- LÓGICA DE ESTADO CORREGIDA Y REORDENADA ---
        estado_lower = (safe_get(unit, 'estado_comercial', '') or '').lower()
        display_status = ''
        
        # 1. Primero, los estados comerciales fijos tienen la máxima prioridad.
        if estado_lower == 'vendido':
            display_status = 'vendido'
        elif estado_lower in ['separado', 'proceso de separacion']:
            display_status = 'separado'
        
        # 2. Solo si no es vendido ni separado, comprobamos si tiene alerta.
        elif safe_get(unit, 'codigo', '') in unidades_con_alerta:
            display_status = 'alerta-subir'
            
        # 3. Si no cumple ninguna de las anteriores, está disponible.
        else:
            display_status = 'disponible'
        # --- FIN DE LÓGICA CORREGIDA ---

        piso = safe_get(unit, 'piso', '')
        if piso not in grid_data: grid_data[piso] = []
        
        # Lógica de filtrado mejorada
        if not tipologia_filtro:
            # Si no hay filtro, no difuminar
            css_class = ''
        else:
            # Si hay filtros específicos, difuminar las que no están seleccionadas
            css_class = 'difuminado' if safe_get(unit, 'nombre_tipologia', '') not in tipologia_filtro else ''
        processed_unit = {
            'codigo': safe_get(unit, 'codigo', ''), 
            'estado_comercial': safe_get(unit, 'estado_comercial', ''),
            'precio_venta': safe_get(unit, 'precio_venta', 0) or 0, 
            'precio_lista': safe_get(unit, 'precio_lista', 0) or 0,
            'precio_m2': safe_get(unit, 'precio_m2', 0) or 0, 
            'nombre_tipologia': safe_get(unit, 'nombre_tipologia', ''), 
            'display_status': display_status, # CAMBIADO: de 'alerta_status' a 'display_status'
            'proformas_count': safe_get(unit, 'proformas_count', 0) or 0, 
            'css_class': css_class,
            'area_techada': safe_get(unit, 'area_techada', 0) or 0
        }
        grid_data[piso].append(processed_unit)

    try:
        sorted_floors = sorted(grid_data.keys(), key=lambda p: int(''.join(filter(str.isdigit, p or '0'))), reverse=True)
    except (ValueError, TypeError):
        sorted_floors = sorted(grid_data.keys(), reverse=True)
    
    # Asegurar que todas las filas tengan el mismo número de columnas
    sorted_grid_data = {}
    for floor in sorted_floors:
        units_in_floor = grid_data[floor].copy()
        # Rellenar con unidades vacías si es necesario
        while len(units_in_floor) < max_columns:
            units_in_floor.append({
                'codigo': '', 
                'estado_comercial': '',
                'precio_venta': 0, 
                'precio_lista': 0,
                'precio_m2': 0, 
                'nombre_tipologia': '', 
                'display_status': 'empty',
                'proformas_count': 0, 
                'css_class': 'empty-unit',
                'area_techada': 0
            })
        sorted_grid_data[floor] = units_in_floor

    # --- INICIO DE LA CORRECCIÓN ---
    if request.headers.get('HX-Request') == 'true':
        hx_target = request.headers.get('HX-Target', '')
        
        # Si el target es solo el grid, devolver el grid con actualizaciones OOB del sidebar y botón
        if hx_target == 'grid-container':
            return render_template('_grid_with_oob_updates.html',
                                   grid=sorted_grid_data, vista_actual=vista_actual, 
                                   max_columns=max_columns, tipologia_filtro=tipologia_filtro,
                                   legend_data=legend_data, sidebar_stats=sidebar_stats, legend_stats=legend_stats)
        # Si el target es el sidebar, devolver solo el sidebar
        elif hx_target == 'sidebar-stats':
            return render_template('_sidebar_stats.html',
                                   legend_data=legend_data, sidebar_stats=sidebar_stats, legend_stats=legend_stats)
        # Si el target es el texto del botón de tipologías
        elif hx_target == 'tipologia-button-text':
            return render_template('_tipologia_button_text.html',
                                   tipologia_filtro=tipologia_filtro)
        # Si el target incluye pricing-content-wrapper y sidebar-stats (filtro de tipologías)
        elif 'pricing-content-wrapper' in hx_target and 'sidebar-stats' in hx_target:
            return render_template('_grid_and_sidebar.html',
                                   grid=sorted_grid_data, all_tipologias=all_tipologias,
                                   current_project=project_name, tipologia_filtro=tipologia_filtro,
                                   vista_actual=vista_actual, max_columns=max_columns,
                                   approval_table_data=approval_table_data,
                                   legend_data=legend_data, sidebar_stats=sidebar_stats, legend_stats=legend_stats)
        # Si el target incluye múltiples elementos (grid-container, sidebar-stats)
        elif 'grid-container' in hx_target and 'sidebar-stats' in hx_target:
            return render_template('_grid_and_sidebar.html',
                                   grid=sorted_grid_data, all_tipologias=all_tipologias,
                                   current_project=project_name, tipologia_filtro=tipologia_filtro,
                                   vista_actual=vista_actual, max_columns=max_columns,
                                   approval_table_data=approval_table_data,
                                   legend_data=legend_data, sidebar_stats=sidebar_stats, legend_stats=legend_stats)
        else:
            # Si no, devolver todo el contenido de filtros y grid
            return render_template('_grid_and_sidebar.html',
                                   grid=sorted_grid_data, all_tipologias=all_tipologias,
                                   current_project=project_name, tipologia_filtro=tipologia_filtro,
                                   vista_actual=vista_actual, max_columns=max_columns,
                                   approval_table_data=approval_table_data,
                                   legend_data=legend_data, sidebar_stats=sidebar_stats, legend_stats=legend_stats)
    else:
        return render_template('pricing_grid.html', 
                               grid=sorted_grid_data, all_tipologias=all_tipologias,
                               all_projects=all_projects, current_project=project_name,
                               tipologia_filtro=tipologia_filtro, vista_actual=vista_actual,
                               max_columns=max_columns,
                               approval_table_data=approval_table_data,
                               legend_data=legend_data, sidebar_stats=sidebar_stats, legend_stats=legend_stats)
    # --- FIN DE LA CORRECCIÓN ---

# --- 8. INICIO DE LA APLICACIÓN ---
if __name__ == '__main__':
    app.run(debug=True)