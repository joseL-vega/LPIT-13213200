import time
import re
import polars as pl

def leer_y_agregar_log(ruta_archivo):
    with open(ruta_archivo, 'r') as archivo:
        print(f"Iniciando lectura y agregación en tiempo real de: {ruta_archivo}")
        
        # Aquí guardaremos las descargas durante 1 segundo
        datos_temporales = [] 
        # Empezamos a contar el tiempo
        tiempo_inicio_ventana = time.time() 
        
        while True:
            linea = archivo.readline()
            
            if not linea:
                time.sleep(0.05) # Espera un poquito si no hay líneas nuevas
                continue
            
            linea_limpia = linea.strip()
            
            # 1. FILTRAR Y EXTRAER (Regex)
            # Buscamos las líneas de descarga SDAP
            if "SDAP" in linea_limpia and "TX PDU" in linea_limpia:
                # Pescamos el número de usuario y los bytes
                match = re.search(r"ue=(\d+).*pdu_len=(\d+)", linea_limpia)
                
                if match:
                    usuario_id = int(match.group(1))
                    bytes_descargados = int(match.group(2))
                    
                    # Guardamos el dato en nuestra lista temporal
                    datos_temporales.append({
                        "ue": usuario_id, 
                        "bytes": bytes_descargados
                    })
            
            # 2. AGREGAR CADA 1 SEGUNDO (Polars)
            tiempo_actual = time.time()
            # Si ha pasado 1 segundo o más desde la última vez...
            if tiempo_actual - tiempo_inicio_ventana >= 1.0:
                
                # Si hemos recopilado datos en este segundo, los procesamos
                if len(datos_temporales) > 0:
                    # Convertimos la lista a un DataFrame ultrarrápido de Polars
                    df = pl.DataFrame(datos_temporales)
                    
                    # Agrupamos por usuario y sumamos los bytes
                    df_agregado = df.group_by("ue").agg(
                        pl.col("bytes").sum().alias("total_bytes_por_segundo")
                    )
                    
                    # Imprimimos el resultado
                    print("\n--- Consumo en el último segundo ---")
                    print(df_agregado)
                    
                    # Vaciamos la lista para empezar a contar el siguiente segundo
                    datos_temporales = []
                
                # Reiniciamos el cronómetro
                tiempo_inicio_ventana = tiempo_actual

if __name__ == '__main__':
    archivo_log = "cu-lan-ho.log" 
    
    try:
        leer_y_agregar_log(archivo_log)
    except FileNotFoundError:
        print(f"Error: No se encuentra el archivo {archivo_log}. Asegúrate de arrancar primero log-sim.py")