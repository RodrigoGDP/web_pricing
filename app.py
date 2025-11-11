import csv
import sqlite3
import random
from collections import defaultdict, Counter
from datetime import datetime, date
from functools import lru_cache
from pathlib import Path

import pandas as pd
from flask import Flask, render_template, request, redirect, url_for

# --- 1. INICIALIZACIÓN DE LA APLICACIÓN ---
app = Flask(__name__)

# Cache para max_columns por proyecto
project_max_columns = {}
DB_NAME = "database.db"
BASE_DIR = Path(__file__).resolve().parent
DEFAULT_EXCHANGE_RATE_PEN = 3.8

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

def calculate_velocity(units_in_tipologia, project_name, conn):
    """Calcula la velocidad de venta para una tipología específica"""
    # Obtener fecha de inicio de venta del proyecto
    fecha_inicio_row = conn.execute(
        "SELECT fecha_inicio_venta FROM proyecto_fechas_inicio WHERE nombre_proyecto = ?", 
        (project_name,)
    ).fetchone()
    
    if not fecha_inicio_row:
        return 0.0
    
    fecha_inicio = datetime.strptime(fecha_inicio_row[0], '%Y-%m-%d').date()
    
    # Contar unidades vendidas de esta tipología
    # Los units_in_tipologia son tuplas, no diccionarios
    unidades_vendidas = []
    for u in units_in_tipologia:
        # u[2] es estado_comercial según el orden de la tabla
        estado = u[2] if u[2] else ''
        if estado.lower() == 'vendido':
            unidades_vendidas.append(u)
    
    total_vendidas = len(unidades_vendidas)
    
    if total_vendidas == 0:
        return 0.0
    
    # Si todas las unidades están vendidas, usar la fecha de la última venta
    if total_vendidas == len(units_in_tipologia):
        # Buscar la fecha de venta más reciente
        fechas_venta = []
        for u in unidades_vendidas:
            # u[12] es fecha_venta según el orden de la tabla
            fecha_venta_str = u[12] if u[12] else ''
            if fecha_venta_str:
                try:
                    fecha_venta = datetime.strptime(fecha_venta_str, '%Y-%m-%d').date()
                    fechas_venta.append(fecha_venta)
                except ValueError:
                    continue
        
        if fechas_venta:
            fecha_fin = max(fechas_venta)
        else:
            # Si no hay fechas de venta válidas, usar fecha actual
            fecha_fin = date.today()
    else:
        # Si no todas están vendidas, usar fecha actual
        fecha_fin = date.today()
    
    # Calcular meses transcurridos
    meses_transcurridos = (fecha_fin.year - fecha_inicio.year) * 12 + (fecha_fin.month - fecha_inicio.month)
    if fecha_fin.day < fecha_inicio.day:
        meses_transcurridos -= 1
    
    # Evitar división por cero
    if meses_transcurridos <= 0:
        meses_transcurridos = 1
    
    # Calcular velocidad (unidades vendidas por mes)
    velocidad = total_vendidas / meses_transcurridos
    return round(velocidad, 2)


def parse_int(value, default=0):
    try:
        if value is None:
            return default
        return int(float(value))
    except (ValueError, TypeError):
        return default


def get_total_habitaciones_from_unit(unit, project_name=""):
    """
    Obtiene el número de dormitorios de una unidad.
    Primero intenta usar el campo total_habitaciones. Si no existe o es 0,
    recurre a un mapeo precargado desde el CSV original.
    """
    total_habitaciones = safe_get(unit, 'total_habitaciones')
    if total_habitaciones not in (None, '', 'null'):
        value = parse_int(total_habitaciones, default=0)
        if 0 < value <= 6:
            return value

    # Intentar obtenerlo del CSV original usando el código de la unidad
    codigo = (safe_get(unit, 'codigo', '') or '').strip()
    project_key = (project_name or safe_get(unit, 'nombre_proyecto', '') or '').strip()
    if codigo and project_key:
        csv_map = load_total_habitaciones_map()
        project_map = csv_map.get(project_key, {})
        value = project_map.get(codigo)
        if value and 0 < value <= 6:
            return value

    # Heurística basada en área techada
    area_techada = safe_get(unit, 'area_techada', 0) or 0
    if area_techada and area_techada > 0:
        if area_techada <= 55:
            return 1
        if area_techada <= 95:
            return 2
        if area_techada <= 135:
            return 3
        if area_techada <= 170:
            return 4
        if area_techada <= 220:
            return 5
        return 6

    return 0


def generate_month_sequence(start_date, end_date):
    """Genera una lista de meses (formato YYYY-MM) desde start_date hasta end_date inclusive."""
    if not start_date or not end_date:
        return []

    sequence = []
    current_year = start_date.year
    current_month = start_date.month

    while (current_year, current_month) <= (end_date.year, end_date.month):
        sequence.append(f"{current_year:04d}-{current_month:02d}")
        if current_month == 12:
            current_month = 1
            current_year += 1
        else:
            current_month += 1
    return sequence


@lru_cache(maxsize=1)
def load_total_habitaciones_map():
    """
    Carga un mapeo por proyecto {nombre_proyecto: {codigo_unidad: total_habitaciones}} desde unidades.csv.
    Se cachea para evitar lecturas repetidas en disco.
    """
    mapping = defaultdict(dict)
    csv_path = BASE_DIR / 'unidades.csv'
    if not csv_path.exists():
        return mapping

    try:
        with csv_path.open('r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                codigo = (row.get('codigo') or '').strip()
                hab = row.get('total_habitaciones')
                proyecto = (row.get('nombre_proyecto') or '').strip()
                if not codigo or not hab or not proyecto:
                    continue
                try:
                    value = int(float(hab))
                except ValueError:
                    continue
                if 0 < value <= 6:
                    mapping[proyecto][codigo] = value
    except FileNotFoundError:
        return defaultdict(dict)

    return {project: dict(values) for project, values in mapping.items()}


def _clean_numeric_series(series):
    """Convierte una serie de strings en números flotantes, limpiando símbolos y separadores."""
    if series is None:
        return pd.Series(dtype=float)
    cleaned = (
        series.astype(str)
        .str.replace(r'[^\d\.,-]', '', regex=True)
        .str.replace(',', '', regex=False)
    )
    return pd.to_numeric(cleaned, errors='coerce')


@lru_cache(maxsize=1)
def load_competencia_metrics():
    """
    Calcula métricas de competencia por cantidad de dormitorios basadas en Tb_utf8.csv.
    Retorna un diccionario {dormitorios: {"precio_promedio": float, "velocidad_promedio": float, "muestras": int}}.
    """
    csv_path = BASE_DIR / 'Tb_utf8.csv'
    if not csv_path.exists():
        return {}

    try:
        df = pd.read_csv(
            csv_path,
            low_memory=False,
            parse_dates=['Fecha de Venta', 'Fecha de Inicio de Venta']
        )
    except Exception:
        return {}

    df.columns = [col.strip() for col in df.columns]
    required_cols = {
        'Precio por m2 - Venta Solarizado',
        'Estado de Inmueble',
        'Sector',
        'Fecha de Venta',
        'Fecha de Inicio de Venta',
        'Cantidad de Dormitorios'
    }
    if not required_cols.issubset(df.columns):
        return {}

    df = df[
        (df['Estado de Inmueble'].astype(str).str.strip().str.lower() == 'vendido') &
        (df['Sector'].astype(str).str.strip().str.lower() == 'lima top') &
        (df['Fecha de Venta'].dt.year.isin([2024, 2025]))
    ].copy()

    if df.empty:
        return {}

    df['Precio por m2 - Venta Solarizado'] = _clean_numeric_series(df['Precio por m2 - Venta Solarizado'])
    df['Cantidad de Dormitorios'] = pd.to_numeric(df['Cantidad de Dormitorios'], errors='coerce')
    df = df.dropna(subset=['Precio por m2 - Venta Solarizado', 'Cantidad de Dormitorios', 'Fecha de Venta', 'Fecha de Inicio de Venta'])

    if df.empty:
        return {}

    # Normalizar dormitorios al entero más cercano positivo
    df['Cantidad de Dormitorios'] = df['Cantidad de Dormitorios'].round().astype(int)
    df = df[df['Cantidad de Dormitorios'] > 0]

    if df.empty:
        return {}

    # Calcular velocidad unidad
    meses = (df['Fecha de Venta'].dt.year - df['Fecha de Inicio de Venta'].dt.year) * 12 + (
        df['Fecha de Venta'].dt.month - df['Fecha de Inicio de Venta'].dt.month
    )
    ajuste_dias = df['Fecha de Venta'].dt.day < df['Fecha de Inicio de Venta'].dt.day
    meses = meses - ajuste_dias.astype(int)
    meses = meses.clip(lower=1)
    df = df[meses > 0].copy()
    df['velocidad_unidad'] = 1 / meses

    if df.empty:
        return {}

    grouped = df.groupby('Cantidad de Dormitorios').agg(
        precio_promedio=('Precio por m2 - Venta Solarizado', 'mean'),
        velocidad_promedio=('velocidad_unidad', 'mean'),
        muestras=('Precio por m2 - Venta Solarizado', 'count')
    )

    return {
        int(dorm): {
            'precio_promedio': float(row['precio_promedio']),
            'velocidad_promedio': float(row['velocidad_promedio']),
            'muestras': int(row['muestras'])
        }
        for dorm, row in grouped.iterrows()
    }


def build_tipologia_dorm_map(tipologias_data_grouped, project_name):
    """Determina el número de dormitorios predominante por tipología."""
    dorm_map = {}
    for tipologia, tip_units in tipologias_data_grouped.items():
        dorm_counts = [
            get_total_habitaciones_from_unit(u, project_name)
            for u in tip_units
        ]
        dorm_counts = [d for d in dorm_counts if d and d > 0]
        if dorm_counts:
            dorm_map[tipologia] = Counter(dorm_counts).most_common(1)[0][0]
            continue

        # Fallback heurístico basado en área cuando no hay datos explícitos
        areas = [
            (safe_get(u, 'area_techada', 0) or 0)
            for u in tip_units
        ]
        areas = [a for a in areas if a and a > 0]
        if not areas:
            dorm_map[tipologia] = None
            continue

        avg_area = sum(areas) / len(areas)
        if avg_area <= 55:
            dorm_est = 1
        elif avg_area <= 95:
            dorm_est = 2
        elif avg_area <= 135:
            dorm_est = 3
        elif avg_area <= 170:
            dorm_est = 4
        else:
            dorm_est = 5

        dorm_map[tipologia] = dorm_est
    return dorm_map

# --- INICIO DE LA SOLUCIÓN ---
@app.route('/')
def index():
    # Redirige al dashboard de un proyecto por defecto (ej. "STILL")
    # Puedes cambiar "STILL" por cualquier otro de tus proyectos válidos.
    return redirect(url_for('dashboard', project_name='STILL'))
# --- FIN DE LA SOLUCIÓN ---

# --- 2. FILTRO DE MONEDA PERSONALIZADO ---
@app.template_filter('currency')
def format_currency(value):
    if value is None: return "$0"
    return f"${value:,.0f}"


@app.template_filter('currency_pen')
def format_currency_pen(value):
    if value is None:
        return ""
    try:
        if isinstance(value, float) and pd.isna(value):
            return ""
        return f"S/ {float(value):,.0f}"
    except (ValueError, TypeError):
        return ""


@app.template_filter('velocity_fmt')
def format_velocity(value):
    if value is None:
        return ""
    try:
        if isinstance(value, float) and pd.isna(value):
            return ""
        return f"{float(value):.2f} u/mes"
    except (ValueError, TypeError):
        return ""

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


@app.route('/dashboard')
def dashboard_redirect():
    conn = get_db_connection()
    projects = conn.execute("SELECT DISTINCT nombre_proyecto FROM unidades ORDER BY nombre_proyecto").fetchall()
    conn.close()
    if projects:
        first_project = 'STILL' if 'STILL' in [p['nombre_proyecto'] for p in projects] else projects[0]['nombre_proyecto']
        return redirect(url_for('dashboard', project_name=first_project))
    return "No hay proyectos cargados en la base de datos."


@app.route('/dashboard/<project_name>')
def dashboard(project_name):
    conn = get_db_connection()
    all_projects = conn.execute("SELECT DISTINCT nombre_proyecto FROM unidades ORDER BY nombre_proyecto").fetchall()
    units = conn.execute("SELECT * FROM unidades WHERE nombre_proyecto = ?", (project_name,)).fetchall()

    # Datos base
    fecha_inicio_row = conn.execute(
        "SELECT fecha_inicio_venta FROM proyecto_fechas_inicio WHERE nombre_proyecto = ?", 
        (project_name,)
    ).fetchone()
    conn.close()

    if not units:
        summary_cards = {
            'unidades_vendidas': 0,
            'progreso_temporal': 0,
            'precio_m2': 0,
            'ventas': 0,
            'area_vendida': 0
        }
        return render_template(
            'dashboard.html',
            all_projects=all_projects,
            current_project=project_name,
            summary_cards=summary_cards,
            evolutivo={'labels': [], 'units': [], 'ticket': [], 'price_m2': []},
            dorm_bars=[],
            layout_overview=[],
            gauge={'vendido_pct': 0, 'por_vender_pct': 0, 'incremento_pct': 0},
            meta_provisional=0,
            color_palette={
                'text_primary': '#454769',
                'text_secondary': '#626481',
                'bar_primary': '#727ab5',
                'bar_secondary': '#a9b9da',
                'bar_gray': '#cbcbd0',
                'line_blue': '#4f81bd',
                'line_red': '#c0504d',
                'line_green': '#9bbb59'
            }
        )

    today = date.today()
    start_date = None
    if fecha_inicio_row and fecha_inicio_row[0]:
        try:
            start_date = datetime.strptime(fecha_inicio_row[0], '%Y-%m-%d').date()
        except ValueError:
            start_date = None

    # Calcular progreso temporal
    meses_transcurridos = 0
    if start_date:
        meses_transcurridos = (today.year - start_date.year) * 12 + (today.month - start_date.month)
        if today.day < start_date.day:
            meses_transcurridos -= 1
        if meses_transcurridos < 0:
            meses_transcurridos = 0
    progreso_temporal = min(round((meses_transcurridos / 24) * 100, 1), 100) if meses_transcurridos else 0

    # Determinar unidades en alerta (rojas) replicando la lógica existente
    tipologias_data = defaultdict(list)
    for unit in units:
        tipologias_data[safe_get(unit, 'nombre_tipologia', '')].append(unit)

    tipologia_dorm_map = build_tipologia_dorm_map(tipologias_data, project_name)

    competencia_metrics = load_competencia_metrics()

    layout_overview = []
    meses_en_venta = max(meses_transcurridos, 1)
    for tipologia, tip_units in tipologias_data.items():
        total_tip = len(tip_units)
        sold_tip_units = [u for u in tip_units if (safe_get(u, 'estado_comercial', '') or '').lower() == 'vendido']
        sold_count = len(sold_tip_units)
        sold_pct = (sold_count / total_tip * 100) if total_tip > 0 else 0

        precio_m2_values = []
        for u in sold_tip_units:
            precio_m2_val = safe_get(u, 'precio_m2', 0)
            if precio_m2_val and precio_m2_val > 0:
                precio_m2_values.append(precio_m2_val)
            else:
                area = safe_get(u, 'area_techada', 0) or 0
                precio_venta = safe_get(u, 'precio_venta', 0) or 0
                if area > 0 and precio_venta > 0:
                    precio_m2_values.append(precio_venta / area)

        avg_precio_m2 = round(sum(precio_m2_values) / len(precio_m2_values), 2) if precio_m2_values else 0
        velocidad_venta = round(sold_count / meses_en_venta, 2) if meses_en_venta > 0 else 0
        absorcion = round(sold_count / total_tip, 2) if total_tip > 0 else 0
        dorm_key = tipologia_dorm_map.get(tipologia)
        competencia = competencia_metrics.get(dorm_key) if dorm_key else None
        precio_mercado = round(competencia['precio_promedio'], 0) if competencia else None
        velocidad_mercado = round(competencia['velocidad_promedio'], 3) if competencia else None
        layout_overview.append({
            'tipologia': tipologia,
            'total_unidades': total_tip,
            'porcentaje_vendido': round(sold_pct, 1),
            'precio_m2_vendido': avg_precio_m2,
            'velocidad_venta': velocidad_venta,
            'absorcion': absorcion,
            'precio_promedio_mercado': precio_mercado,
            'velocidad_venta_mercado': velocidad_mercado,
            'dormitorios': dorm_key
        })

    layout_overview.sort(key=lambda item: item['tipologia'])

    unidades_con_alerta = set()
    for tipologia_units in tipologias_data.values():
        total_units = len(tipologia_units)
        sold_units_tipologia = sum(
            1 for u in tipologia_units if (safe_get(u, 'estado_comercial', '') or '').lower() == 'vendido'
        )
        if total_units > 0 and (sold_units_tipologia / total_units) * 100 >= 20.0:
            for u in tipologia_units:
                estado_lower = (safe_get(u, 'estado_comercial', '') or '').lower()
                if estado_lower not in ['vendido', 'separado', 'proceso de separacion']:
                    unidades_con_alerta.add(safe_get(u, 'codigo', ''))

    sold_units = []
    available_units = []
    alert_units = []
    separated_units = []

    for unit in units:
        estado_lower = (safe_get(unit, 'estado_comercial', '') or '').lower()
        codigo = safe_get(unit, 'codigo', '')
        if estado_lower == 'vendido':
            sold_units.append(unit)
        elif codigo in unidades_con_alerta:
            alert_units.append(unit)
        elif estado_lower in ['separado', 'proceso de separacion']:
            separated_units.append(unit)
        else:
            available_units.append(unit)

    total_units = len(units)
    total_vendidas = len(sold_units)
    total_por_vender = total_units - total_vendidas
    total_ventas = sum(safe_get(u, 'precio_venta', 0) or 0 for u in sold_units)
    area_vendida = sum(safe_get(u, 'area_techada', 0) or 0 for u in sold_units)
    meta_provisional = sum(safe_get(u, 'precio_lista', 0) or 0 for u in units)

    precio_m2_disponible_valores = [
        safe_get(u, 'precio_m2', 0) for u in available_units if safe_get(u, 'precio_m2', 0)
    ]
    precio_m2_promedio = round(
        sum(precio_m2_disponible_valores) / len(precio_m2_disponible_valores), 2
    ) if precio_m2_disponible_valores else 0

    # Evolutivo mensual
    monthly_summary = defaultdict(lambda: {
        'units': 0,
        'ticket_sum': 0,
        'ticket_count': 0,
        'price_m2_sum': 0,
        'price_m2_count': 0
    })
    for unit in sold_units:
        fecha_venta_str = safe_get(unit, 'fecha_venta', '')
        if not fecha_venta_str:
            continue
        try:
            fecha_venta = datetime.strptime(fecha_venta_str, '%Y-%m-%d').date()
        except ValueError:
            continue
        month_key = fecha_venta.strftime('%Y-%m')
        monthly_summary[month_key]['units'] += 1

        precio_venta = safe_get(unit, 'precio_venta', 0) or 0
        if precio_venta and precio_venta > 0:
            monthly_summary[month_key]['ticket_sum'] += precio_venta
            monthly_summary[month_key]['ticket_count'] += 1

        precio_m2 = safe_get(unit, 'precio_m2', 0)
        if precio_m2 and precio_m2 > 0:
            monthly_summary[month_key]['price_m2_sum'] += precio_m2
            monthly_summary[month_key]['price_m2_count'] += 1
        else:
            area = safe_get(unit, 'area_techada', 0) or 0
            if area:
                monthly_summary[month_key]['price_m2_sum'] += precio_venta / area
                monthly_summary[month_key]['price_m2_count'] += 1

    if start_date:
        month_sequence = generate_month_sequence(date(start_date.year, start_date.month, 1), today)
    else:
        month_sequence = sorted(monthly_summary.keys())

    evolutivo_labels = []
    evolutivo_units = []
    evolutivo_ticket = []
    evolutivo_precio_m2 = []

    for month_key in month_sequence:
        data_point = monthly_summary.get(month_key, {
            'units': 0,
            'ticket_sum': 0,
            'ticket_count': 0,
            'price_m2_sum': 0,
            'price_m2_count': 0
        })
        evolutivo_labels.append(month_key)
        evolutivo_units.append(data_point['units'])
        avg_ticket = (
            data_point['ticket_sum'] / data_point['ticket_count']
            if data_point['ticket_count'] > 0 else 0
        )
        evolutivo_ticket.append(round(avg_ticket, 2))
        avg_price_m2 = (
            data_point['price_m2_sum'] / data_point['price_m2_count']
            if data_point['price_m2_count'] > 0 else 0
        )
        evolutivo_precio_m2.append(round(avg_price_m2, 2))

    # Barras por dormitorio
    dorm_summary = defaultdict(lambda: {
        'sold_sum': 0, 'sold_count': 0,
        'available_sum': 0, 'available_count': 0,
        'alert_sum': 0, 'alert_count': 0
    })

    for unit in units:
        dorms = get_total_habitaciones_from_unit(unit, project_name)
        if dorms <= 0:
            continue
        label = f"{dorms} dor"
        estado_lower = (safe_get(unit, 'estado_comercial', '') or '').lower()
        codigo = safe_get(unit, 'codigo', '')

        if estado_lower == 'vendido':
            dorm_summary[label]['sold_sum'] += safe_get(unit, 'precio_venta', 0) or 0
            dorm_summary[label]['sold_count'] += 1
        elif codigo in unidades_con_alerta:
            dorm_summary[label]['alert_sum'] += safe_get(unit, 'precio_lista', 0) or 0
            dorm_summary[label]['alert_count'] += 1
        elif estado_lower in ['separado', 'proceso de separacion']:
            # Se incluyen en disponible para efectos de "por vender"
            dorm_summary[label]['available_sum'] += safe_get(unit, 'precio_lista', 0) or 0
            dorm_summary[label]['available_count'] += 1
        else:
            dorm_summary[label]['available_sum'] += safe_get(unit, 'precio_lista', 0) or 0
            dorm_summary[label]['available_count'] += 1

    dorm_bars = []
    for label, stats in dorm_summary.items():
        total = stats['sold_count'] + stats['available_count'] + stats['alert_count']
        sold_pct = round((stats['sold_count'] / total) * 100, 1) if total else 0
        sold_avg = stats['sold_sum'] / stats['sold_count'] if stats['sold_count'] else 0
        available_avg = stats['available_sum'] / stats['available_count'] if stats['available_count'] else 0
        alert_avg = stats['alert_sum'] / stats['alert_count'] if stats['alert_count'] else 0
        dorm_bars.append({
            'label': label,
            'sold_pct': sold_pct,
            'total_count': total,
            'sold_count': stats['sold_count'],
            'sold_avg': round(sold_avg, 2),
            'available_avg': round(available_avg, 2),
            'alert_avg': round(alert_avg, 2)
        })

    dorm_bars.sort(key=lambda item: parse_int(item['label'].split()[0], default=0))

    total_general = total_units if total_units > 0 else 1
    gauge_vendido_pct = round((total_vendidas / total_general) * 100, 2)
    gauge_por_vender_pct = round((total_por_vender / total_general) * 100, 2)
    gauge_incremento_pct = 0.0

    summary_cards = {
        'unidades_vendidas': total_vendidas,
        'progreso_temporal': progreso_temporal,
        'precio_m2': precio_m2_promedio,
        'ventas': total_ventas,
        'area_vendida': area_vendida
    }

    color_palette = {
        'text_primary': '#454769',
        'text_secondary': '#626481',
        'bar_primary': '#727ab5',
        'bar_secondary': '#a9b9da',
        'bar_gray': '#cbcbd0',
        'line_blue': '#4f81bd',
        'line_red': '#c0504d',
        'line_green': '#9bbb59'
    }

    evolutivo_data = {
        'labels': evolutivo_labels,
        'units': evolutivo_units,
        'ticket': evolutivo_ticket,
        'price_m2': evolutivo_precio_m2
    }

    gauge_data = {
        'vendido_pct': gauge_vendido_pct,
        'por_vender_pct': gauge_por_vender_pct,
        'incremento_pct': gauge_incremento_pct,
        'vendido_valor': total_vendidas,
        'por_vender_valor': total_por_vender,
        'meta_provisional': meta_provisional
    }

    return render_template(
        'dashboard.html',
        all_projects=all_projects,
        current_project=project_name,
        summary_cards=summary_cards,
        evolutivo=evolutivo_data,
        dorm_bars=dorm_bars,
        layout_overview=layout_overview,
        gauge=gauge_data,
        meta_provisional=meta_provisional,
        color_palette=color_palette
    )

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
    tipologia_dorm_map = build_tipologia_dorm_map(tipologias_data_grouped, project_name)
    competencia_metrics = load_competencia_metrics()
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
    
    # Obtener conexión para calcular velocidades
    conn = get_db_connection()
    
    for tipologia_name, units_in_tipo in tipologias_data_grouped.items():
        total_proformas = sum(safe_get(u, 'proformas_count', 0) or 0 for u in units_in_tipo)
        precios_m2 = [safe_get(u, 'precio_m2', 0) or 0 for u in units_in_tipo if safe_get(u, 'precio_m2', 0) and safe_get(u, 'precio_m2', 0) > 0]
        avg_precio_m2 = sum(precios_m2) / len(precios_m2) if precios_m2 else 0
        sold_units_tipo = [
            u for u in units_in_tipo
            if (safe_get(u, 'estado_comercial', '') or '').lower() == 'vendido'
        ]
        precio_venta_m2_values = []
        for u in sold_units_tipo:
            area_unit = safe_get(u, 'area_techada', 0) or 0
            precio_venta_unit = safe_get(u, 'precio_venta', 0) or 0
            if area_unit and area_unit > 0 and precio_venta_unit and precio_venta_unit > 0:
                precio_venta_m2_values.append(precio_venta_unit / area_unit)
        avg_precio_venta_m2_pen = (
            round((sum(precio_venta_m2_values) / len(precio_venta_m2_values)) * DEFAULT_EXCHANGE_RATE_PEN, 2)
            if precio_venta_m2_values else 0
        )
        total_count = len(units_in_tipo)
        available_count = sum(1 for u in units_in_tipo if safe_get(u, 'estado_comercial', '').lower() != 'vendido')
        has_alert = any(safe_get(u, 'codigo', '') in unidades_con_alerta for u in units_in_tipo)
        
        # Calcular velocidad de venta
        velocidad_promedio = calculate_velocity(units_in_tipo, project_name, conn)
        dorm_key = tipologia_dorm_map.get(tipologia_name)
        competencia = competencia_metrics.get(dorm_key) if dorm_key else None
        precio_mercado = round(competencia['precio_promedio'], 0) if competencia else None
        velocidad_mercado = round(competencia['velocidad_promedio'], 3) if competencia else None
        
        approval_table_data.append({
            'tipologia': tipologia_name,
            'unidades_disponibles_str': f"{available_count}/{total_count}",
            'total_proformas': total_proformas,
            'avg_precio_m2': avg_precio_m2,
            'velocidad_promedio': velocidad_promedio,
            'precio_venta_promedio_m2_pen': avg_precio_venta_m2_pen,
            'precio_promedio_mercado': precio_mercado,
            'velocidad_venta_mercado': velocidad_mercado,
            'dormitorios': dorm_key,
            'has_alert': has_alert,
        })
    
    conn.close()

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
                                   all_tipologias=all_tipologias, current_project=project_name,
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