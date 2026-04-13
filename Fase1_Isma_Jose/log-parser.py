import time
import re
import polars as pl
import threading
from dash import Dash, html, dcc, callback, Output, Input
import plotly.express as px
from datetime import datetime

# --- CONFIGURACIÓN Y BÚFER GLOBAL ---
archivo_log = "cu-lan-ho.log"
archivo_parquet = "data.parquet" # Nombre del archivo de salida 
historico_grafica = [] 
datos_totales_sesion = [] # Para el guardado persistente en Parquet
lock = threading.Lock()

# --- MOTOR DE LECTURA (HILO SECUNDARIO) ---
def motor_lectura():
    global historico_grafica, datos_totales_sesion
    
    proximo_guardado_parquet = time.time() + 30 # Cronómetro para el punto 6
    
    with open(archivo_log, 'r') as archivo:
        datos_segundo_actual = []
        inicio_segundo = time.time()
        
        while True:
            linea = archivo.readline()
            if not linea:
                time.sleep(0.01)
                continue
            
            # Punto 2: Filtrado SDAP [cite: 2306]
            if "SDAP" in linea and "TX PDU" in linea:
                match = re.search(r"ue=(\d+).*pdu_len=(\d+)", linea)
                if match:
                    datos_segundo_actual.append({"ue": match.group(1), "bytes": int(match.group(2))})
            
            ahora = time.time()
            
            # Punto 4: Agregación cada 1 segundo [cite: 2308]
            if ahora - inicio_segundo >= 1.0:
                if datos_segundo_actual:
                    df = pl.DataFrame(datos_segundo_actual)
                    resumen = df.group_by("ue").agg(pl.col("bytes").sum().alias("consumo"))
                    
                    with lock:
                        timestamp = datetime.now()
                        for fila in resumen.to_dicts():
                            dato = {
                                "Tiempo": timestamp,
                                "UE": f"Usuario {fila['ue']}",
                                "Consumo (Bytes)": fila['consumo']
                            }
                            historico_grafica.append(dato)
                            datos_totales_sesion.append(dato) # Guardamos para el histórico total
                        
                        # Mantenemos la gráfica fluida
                        if len(historico_grafica) > 500:
                            historico_grafica = historico_grafica[-500:]
                
                datos_segundo_actual = []
                inicio_segundo = ahora

            # Punto 6: Guardado en Parquet cada 30 segundos 
            if ahora >= proximo_guardado_parquet:
                if datos_totales_sesion:
                    df_parquet = pl.DataFrame(datos_totales_sesion)
                    df_parquet.write_parquet(archivo_parquet) # Método nativo de Polars 
                    print(f"--- [{timestamp.strftime('%H:%M:%S')}] Backup: {len(datos_totales_sesion)} registros guardados en Parquet ---")
                proximo_guardado_parquet = ahora + 30

# --- INTERFAZ DASH ---
app = Dash(__name__)
app.layout = html.Div([
    html.H1("Monitor de Tráfico 5G - Fase 1 Finalizada", style={'textAlign': 'center'}),
    dcc.Graph(id='grafica-trafico'),
    dcc.Interval(id='intervalo-actualizacion', interval=1000, n_intervals=0)
])

@app.callback(
    Output('grafica-trafico', 'figure'),
    Input('intervalo-actualizacion', 'n_intervals')
)
def actualizar_grafica(n):
    with lock:
        if not historico_grafica:
            return px.line(title="Esperando flujo de datos...")
        df_plot = pl.DataFrame(historico_grafica).sort("Tiempo")
    
    return px.line(df_plot, x="Tiempo", y="Consumo (Bytes)", color="UE", markers=True)

if __name__ == '__main__':
    threading.Thread(target=motor_lectura, daemon=True).start()
    app.run(host="127.0.0.1", port=8059, debug=False)