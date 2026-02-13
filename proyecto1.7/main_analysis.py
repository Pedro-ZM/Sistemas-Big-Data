# Práctica 1.1 - Data Preparation con Polars y Plotly

import os
import json
import polars as pl
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import mysql.connector
from dotenv import load_dotenv
from decimal import Decimal
import requests

load_dotenv()

def get_db_connection():
    """Conexión a MySQL usando variables de entorno."""
    return mysql.connector.connect(
        host=os.getenv("MYSQL_HOST", "localhost"),
        user=os.getenv("MYSQL_USER", "root"),
        password=os.getenv("MYSQL_PASSWORD", ""),
        database=os.getenv("MYSQL_DATABASE", "fila_2")
    )

def load_data_to_polars():
    """Carga datos de MySQL en DataFrames de Polars."""
    def convert_row(row):
        return {k: (float(v) if isinstance(v, Decimal) else v) for k, v in row.items()}
    
    print("Conectando a la base de datos...")
    conexion = get_db_connection()
    cursor = conexion.cursor(dictionary=True)
    
    ipc_data, ipv_data = [], []
    
    try:
        cursor.execute("""
            SELECT ipc.id, ipc.COD, ipc.Nombre, 
                   data_ipc.Fecha, data_ipc.FK_Periodo, data_ipc.Anyo, data_ipc.Valor
            FROM ipc JOIN data_ipc ON ipc.id = data_ipc.id_ipc
        """)
        ipc_data = [convert_row(row) for row in cursor.fetchall()]
    except mysql.connector.Error as e:
        print(f"Tabla IPC no encontrada: {e}")
    
    try:
        cursor.execute("""
            SELECT ipv.id, ipv.COD, ipv.Nombre,
                   data_ipv.Fecha, data_ipv.FK_Periodo, data_ipv.Anyo, data_ipv.Valor
            FROM ipv JOIN data_ipv ON ipv.id = data_ipv.id_ipv
        """)
        ipv_data = [convert_row(row) for row in cursor.fetchall()]
    except mysql.connector.Error as e:
        print(f"Tabla IPV no encontrada: {e}")
    
    cursor.close()
    conexion.close()
    
    df_ipc = pl.DataFrame(ipc_data) if ipc_data else pl.DataFrame()
    df_ipv = pl.DataFrame(ipv_data) if ipv_data else pl.DataFrame()
    
    print(f"Datos cargados: {len(df_ipc)} IPC, {len(df_ipv)} IPV")
    return {"ipc": df_ipc, "ipv": df_ipv}

def clean_and_transform(dataframes):
    """Limpieza y transformación de datos."""
    print("Transformando datos...")
    df_ipc, df_ipv = dataframes["ipc"], dataframes["ipv"]
    
    # Limpieza IPC
    if not df_ipc.is_empty():
        df_ipc = df_ipc.with_columns([
            pl.col("Anyo").cast(pl.String).str.extract(r"(\d+)").cast(pl.Int64).alias("Anyo"),
            pl.col("Valor").cast(pl.Float64).alias("Valor")
        ]).drop_nulls(subset=["Valor", "Anyo"])
        df_ipc = df_ipc.sort("Anyo").with_columns([
            ((pl.col("Valor") - pl.col("Valor").shift(1)) / pl.col("Valor").shift(1) * 100).alias("Variacion_Interanual_IPC")
        ])
    
    # Limpieza IPV
    if not df_ipv.is_empty():
        df_ipv = df_ipv.with_columns([
            pl.col("Anyo").cast(pl.String).str.extract(r"(\d+)").cast(pl.Int64).alias("Anyo"),
            pl.col("Valor").cast(pl.Float64).alias("Valor")
        ]).drop_nulls(subset=["Valor", "Anyo"])
        df_ipv = df_ipv.sort("Anyo").with_columns([
            ((pl.col("Valor") - pl.col("Valor").shift(1)) / pl.col("Valor").shift(1) * 100).alias("Variacion_Interanual_IPV")
        ])
    
    # Dataset combinado
    df_comparativa = pl.DataFrame()
    if not df_ipc.is_empty() and not df_ipv.is_empty():
        ipc_anual = df_ipc.group_by("Anyo").agg([
            pl.col("Valor").mean().alias("IPC_Promedio"),
            pl.col("Variacion_Interanual_IPC").mean().alias("Variacion_IPC")
        ])
        ipv_anual = df_ipv.group_by("Anyo").agg([
            pl.col("Valor").mean().alias("IPV_Promedio"),
            pl.col("Variacion_Interanual_IPV").mean().alias("Variacion_IPV")
        ])
        df_comparativa = ipc_anual.join(ipv_anual, on="Anyo", how="inner").sort("Anyo")
        df_comparativa = df_comparativa.with_columns([
            (pl.col("IPV_Promedio") / pl.col("IPC_Promedio")).alias("Ratio_IPV_IPC")
        ])
    
    df_variaciones = df_comparativa.select(["Anyo", "Variacion_IPC", "Variacion_IPV"]).drop_nulls() if not df_comparativa.is_empty() else pl.DataFrame()
    
    print("Transformaciones completadas")
    return {"ipc": df_ipc, "ipv": df_ipv, "comparativa": df_comparativa, "variaciones": df_variaciones}

def export_to_csv(dataframes):
    """Exporta DataFrames a CSV."""
    print("Exportando CSVs...")
    os.makedirs("data_output", exist_ok=True)
    
    exports = [
        ("ipc", "data_output/Evolucion_IPC.csv"),
        ("ipv", "data_output/Evolucion_IPV.csv"),
        ("comparativa", "data_output/Comparativa_IPC_IPV.csv"),
        ("variaciones", "data_output/Variaciones_Interanuales.csv")
    ]
    
    for key, path in exports:
        if not dataframes[key].is_empty():
            dataframes[key].write_csv(path)
            print(f"  {path}")

def fetch_ipc_por_ccaa():
    """Obtiene datos del IPC por CCAA desde la API del INE (tabla 50940)."""
    print("Obteniendo IPC por CCAA desde la API del INE...")
    url = "https://servicios.ine.es/wstempus/js/ES/DATOS_TABLA/50940?nult=1"
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()

    # Mapeo de nombres del INE a nombres del GeoJSON
    ine_to_geojson = {
        "Andalucía": "Andalucia",
        "Aragón": "Aragon",
        "Asturias, Principado de": "Asturias",
        "Balears, Illes": "Baleares",
        "Canarias": "Canarias",
        "Cantabria": "Cantabria",
        "Castilla y León": "Castilla-Leon",
        "Castilla - La Mancha": "Castilla-La Mancha",
        "Cataluña": "Cataluña",
        "Comunitat Valenciana": "Valencia",
        "Extremadura": "Extremadura",
        "Galicia": "Galicia",
        "Madrid, Comunidad de": "Madrid",
        "Murcia, Región de": "Murcia",
        "Navarra, Comunidad Foral de": "Navarra",
        "País Vasco": "Pais Vasco",
        "Rioja, La": "La Rioja",
        "Ceuta": "Ceuta",
        "Melilla": "Melilla",
    }

    # Filtrar solo series del índice general con variación anual
    resultados = []
    for serie in data:
        nombre = serie.get("Nombre", "")
        if "ndice general" in nombre and "Variaci" in nombre:
            ccaa_ine = nombre.split(".")[0].strip()
            if ccaa_ine in ine_to_geojson:
                valor = serie["Data"][0]["Valor"] if serie.get("Data") else None
                if valor is not None:
                    resultados.append({
                        "CCAA": ine_to_geojson[ccaa_ine],
                        "IPC_Variacion": float(valor)
                    })

    df = pl.DataFrame(resultados)
    print(f"  Datos obtenidos para {len(df)} comunidades autónomas")
    return df


def create_visualizations(dataframes, df_ipc_ccaa=None):
    """Crea visualizaciones con Plotly."""
    print("Generando visualizaciones...")
    os.makedirs("visualizations", exist_ok=True)
    
    df_comp, df_var = dataframes["comparativa"], dataframes["variaciones"]
    if df_comp.is_empty():
        print("No hay datos para visualizaciones de evolución y correlación")
    else:
        pdf_comp = df_comp.to_pandas()
    
        # Grafico 1: Evolucion temporal
        fig1 = make_subplots(rows=2, cols=1, subplot_titles=("Evolucion del IPC", "Evolucion del IPV"), vertical_spacing=0.15)
        fig1.add_trace(go.Scatter(x=pdf_comp["Anyo"], y=pdf_comp["IPC_Promedio"], mode="lines+markers", name="IPC", line=dict(color="#FF6B6B", width=3)), row=1, col=1)
        fig1.add_trace(go.Scatter(x=pdf_comp["Anyo"], y=pdf_comp["IPV_Promedio"], mode="lines+markers", name="IPV", line=dict(color="#4ECDC4", width=3)), row=2, col=1)
        fig1.update_layout(title="Evolucion Temporal de Indices Economicos", template="plotly_dark", height=700)
        fig1.write_html("visualizations/evolucion_temporal.html")
        print("  visualizations/evolucion_temporal.html")
    
        # Grafico 2: Correlacion
        pdf_clean = pdf_comp.dropna(subset=["IPC_Promedio", "IPV_Promedio", "Ratio_IPV_IPC"])
        fig2 = px.scatter(pdf_clean, x="IPC_Promedio", y="IPV_Promedio", color="Anyo", hover_data=["Anyo", "Ratio_IPV_IPC"],
                          title="Correlacion IPC vs IPV", labels={"IPC_Promedio": "IPC", "IPV_Promedio": "IPV"}, template="plotly_dark")
        fig2.update_traces(marker=dict(size=15, line=dict(width=1, color="white")))
        fig2.update_layout(height=600)
        fig2.write_html("visualizations/correlacion_ipc_ipv.html")
        print("  visualizations/correlacion_ipc_ipv.html")
    
    # Grafico 3: Mapa IPC por CCAA
    if df_ipc_ccaa is not None and not df_ipc_ccaa.is_empty():
        geojson_path = os.path.join(os.path.dirname(__file__), "spain_ccaa.geojson")
        with open(geojson_path, "r", encoding="utf-8") as f:
            geojson_ccaa = json.load(f)

        pdf_ccaa = df_ipc_ccaa.to_pandas()

        fig3 = px.choropleth(
            pdf_ccaa,
            geojson=geojson_ccaa,
            locations="CCAA",
            featureidkey="properties.name",
            color="IPC_Variacion",
            color_continuous_scale="RdYlGn_r",
            range_color=[pdf_ccaa["IPC_Variacion"].min() - 0.2, pdf_ccaa["IPC_Variacion"].max() + 0.2],
            labels={"IPC_Variacion": "Variación anual (%)", "CCAA": "Comunidad Autónoma"},
            title="Mapa del IPC - Variación Anual por Comunidad Autónoma (Último periodo)",
        )
        fig3.update_geos(
            fitbounds="locations",
            visible=False,
        )
        fig3.update_layout(
            template="plotly_dark",
            height=700,
            margin={"r": 0, "t": 60, "l": 0, "b": 0},
            coloraxis_colorbar=dict(title="IPC (%)", ticksuffix="%"),
        )
        fig3.write_html("visualizations/mapa_ipc_ccaa.html")
        print("  visualizations/mapa_ipc_ccaa.html")
    else:
        print("  No hay datos de IPC por CCAA para el mapa")

def main():
    try:
        raw_data = load_data_to_polars()
        clean_data = clean_and_transform(raw_data)
        export_to_csv(clean_data)
        df_ipc_ccaa = fetch_ipc_por_ccaa()
        create_visualizations(clean_data, df_ipc_ccaa)
        print("\nProceso completado:")
        print("  - data_output/*.csv")
        print("  - visualizations/*.html")
    except mysql.connector.Error as e:
        print(f"Error MySQL: {e}")
    except Exception as e:
        print(f"Error: {e}")
        raise

if __name__ == "__main__":
    main()
