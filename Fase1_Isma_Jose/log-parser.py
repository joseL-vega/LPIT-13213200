import time
import re
import polars as pl
import threading
from dash import Dash, html, dcc, callback, Output, Input
import plotly.express as px
from datetime import datetime

# --- CONFIGURACIÓN Y BÚFER GLOBAL ---
archivo_log = "cu-lan-ho.log"
archivo_parquet = "data.parquet"
historico_grafica = [] 
datos_totales_sesion = []
lock = threading.Lock()

# --- FASE 2: LA AGENDA DE USUARIOS ---
# Diccionario para guardar: { 1: {'plmn': '21405', 'pci': '802', 'rnti': '0x4602'} }
info_usuarios = {} 

# --- MOTOR DE LECTURA (HILO SECUNDARIO) --- PUNTO 1
def motor_lectura():
    global historico_grafica, datos_totales_sesion, info_usuarios
    
    inicio_segundo = None
    proximo_guardado_parquet = None
    datos_segundo_actual = []
    
    with open(archivo_log, 'r') as archivo:
        while True:
            linea = archivo.readline()
            if not linea:
                time.sleep(0.01)
                continue
            
            try:
                tiempo_actual_log = datetime.fromisoformat(linea.split()[0])
            except (ValueError, IndexError):
                continue

            if inicio_segundo is None:
                inicio_segundo = tiempo_actual_log
                proximo_guardado_parquet = tiempo_actual_log.timestamp() + 30
            
            # ------------------------------------------------------------
            # FASE 2: CAZADOR DE IDENTIDADES (Enriquecimiento)
            # Buscamos la línea de creación del UE para nutrir la agenda
            if "Created new CU-CP UE" in linea:
                match_info = re.search(r"ue=(\d+).*?plmn=(\d+).*?pci=(\d+).*?rnti=(\w+)", linea)
                if match_info:
                    ue_id = int(match_info.group(1))
                    # Guardamos los datos en la agenda
                    info_usuarios[ue_id] = {
                        "plmn": match_info.group(2),
                        "pci": match_info.group(3),
                        "rnti": match_info.group(4)
                    }
            # ------------------------------------------------------------

            # FASE 1: FILTRADO SDAP (Descarga de datos) PUNTO 2
            if "SDAP" in linea and "TX PDU" in linea:
                match_datos = re.search(r"ue=(\d+).*pdu_len=(\d+)", linea)
                if match_datos:
                    datos_segundo_actual.append({"ue": int(match_datos.group(1)), "bytes": int(match_datos.group(2))})
            
            # FASE 1: AGREGACIÓN CADA 1 SEGUNDO PUNTO 4
            if (tiempo_actual_log - inicio_segundo).total_seconds() >= 1.0:
                if datos_segundo_actual:
                    df = pl.DataFrame(datos_segundo_actual)
                    resumen = df.group_by("ue").agg(pl.col("bytes").sum().alias("consumo"))
                    
                    with lock:
                        for fila in resumen.to_dicts():
                            ue_id = fila['ue']
                            
                            # --- FASE 2: EL CHIVATO (Consultamos la agenda) ---
                            # Si el usuario está en la agenda, cogemos sus datos. Si no, ponemos "Desconocido"
                            info = info_usuarios.get(ue_id, {"plmn": "?", "pci": "?", "rnti": "?"})
                            
                            # Creamos una etiqueta enriquecida súper pro para la gráfica
                            etiqueta_pro = f"UE {ue_id} [PLMN:{info['plmn']} | PCI:{info['pci']} | RNTI:{info['rnti']}]"
                            # ---------------------------------------------------
                            
                            dato = {
                                "Tiempo": inicio_segundo, 
                                "UE": etiqueta_pro, # Usamos la etiqueta enriquecida
                                "Consumo (Bytes)": fila['consumo']
                            }
                            historico_grafica.append(dato)
                            datos_totales_sesion.append(dato)
                        
                        if len(historico_grafica) > 500:
                            historico_grafica = historico_grafica[-500:]
                
                datos_segundo_actual = []
                inicio_segundo = tiempo_actual_log

            # FASE 1: GUARDADO EN PARQUET PUNTO 6
            if tiempo_actual_log.timestamp() >= proximo_guardado_parquet:
                if datos_totales_sesion:
                    df_parquet = pl.DataFrame(datos_totales_sesion)
                    df_parquet.write_parquet(archivo_parquet)
                proximo_guardado_parquet = tiempo_actual_log.timestamp() + 30

# --- INTERFAZ DASH --- PUNTO 5
app = Dash(__name__)
app.layout = html.Div([
    html.H1("Monitor de Tráfico 5G (Fases 1 y 2 Completadas)", style={'textAlign': 'center'}),
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