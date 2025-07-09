from influxdb_client import InfluxDBClient
import pandas as pd
import matplotlib.pyplot as plt
import pytz
from datetime import datetime
import os

# === CONFIGURACIÓN ===
url = "http://localhost:8086"
token = "KIn4Lj8XRGhsS95jJWK32tPpm-qmtKtW9KbZSmn_ETg5exYU1lvriTt5zQfk0_SdwsoXB_q3WUtK3qY9NQ66zA=="
org = "mi_org"
bucket = "sensores"
measurement = "mqtt_consumer"
field = "R2"
topic = "datos/rpi"

# Zona horaria local
local_tz = pytz.timezone("Europe/Madrid")

# Directorio de salida CSV
output_dir = "export_csv"
os.makedirs(output_dir, exist_ok=True)

# RANGOS LOCALES
rangos_locales = [
    ("2025-07-09 13:20:38", "2025-07-09 13:25:38"),
    ("2025-07-08 10:13:27", "2025-07-08 10:23:27"),
    ("2025-07-08 10:26:59", "2025-07-08 10:36:59"),
    ("2025-07-08 10:40:44", "2025-07-08 10:50:54"),
    ("2025-07-08 10:54:15", "2025-07-08 11:04:15"),
    ("2025-07-08 11:08:36", "2025-07-08 11:18:36"),
    ("2025-07-08 11:22:49", "2025-07-08 11:32:49"),
    ("2025-07-08 11:35:51", "2025-07-08 11:45:51"),
    ("2025-07-08 11:48:24", "2025-07-08 11:58:24"),
]

etiquetas = ["A", "B", "C", "D", "E", "F", "G", "H", "I"]

# === CONEXIÓN INFLUXDB ===
client = InfluxDBClient(url=url, token=token, org=org)
query_api = client.query_api()

# === PROCESAR CADA RANGO ===
for idx, (start_local_str, stop_local_str) in enumerate(rangos_locales):
    etiqueta = etiquetas[idx]
    nombre_rango = f"{etiqueta} - Rango {idx + 1}"

    # Convertir a UTC
    start_dt_local = local_tz.localize(datetime.strptime(start_local_str, "%Y-%m-%d %H:%M:%S"))
    stop_dt_local = local_tz.localize(datetime.strptime(stop_local_str, "%Y-%m-%d %H:%M:%S"))
    start_utc = start_dt_local.astimezone(pytz.utc).isoformat()
    stop_utc = stop_dt_local.astimezone(pytz.utc).isoformat()

    print(f"\n📌 {nombre_rango}: {start_local_str} → {stop_local_str} (Madrid)")
    print(f"   🕒 UTC: {start_utc} → {stop_utc}")

    query = f'''
    from(bucket: "{bucket}")
      |> range(start: time(v: "{start_utc}"), stop: time(v: "{stop_utc}"))
      |> filter(fn: (r) => r["_measurement"] == "{measurement}")
      |> filter(fn: (r) => r["_field"] == "{field}")
      |> filter(fn: (r) => r["topic"] == "{topic}")
    '''

    tables = query_api.query(query)
    records = [record.values for table in tables for record in table.records]
    df = pd.DataFrame(records)

    if not df.empty:
        df['_time'] = pd.to_datetime(df['_time']).dt.tz_convert(local_tz)
        df.set_index('_time', inplace=True)
        df.sort_index(inplace=True)

        # Calcular frecuencia
        df['dt'] = df.index.to_series().diff().dt.total_seconds()
        frecuencia_media = 1 / df['dt'].mean() if df['dt'].mean() > 0 else float('nan')
        print(f"📈 Muestras: {len(df)} - Frecuencia estimada: {frecuencia_media:.2f} Hz")

        # Graficar
        plt.figure(figsize=(10, 4))
        plt.plot(df.index, df['_value'], label=nombre_rango)
        plt.title(f'{nombre_rango} - Frecuencia ≈ {frecuencia_media:.2f} Hz')
        plt.xlabel('Hora local (Madrid)')
        plt.ylabel(field)
        plt.grid(True)
        plt.tight_layout()
        plt.legend()
        plt.show()

        # Exportar CSV
        filename = f"{etiqueta}_frecuencia_{frecuencia_media:.2f}Hz.csv"
        filepath = os.path.join(output_dir, filename)
        df[['_value']].to_csv(filepath)
        print(f"💾 Exportado: {filepath}")
    else:
        print("⚠️ No se encontraron datos en este rango.")
