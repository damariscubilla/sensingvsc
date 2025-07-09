import json
import time
import os
import csv
import paho.mqtt.client as mqtt
from datetime import datetime

# Configuraci�n del broker y t�picos
BROKER_RPI = "localhost"
BROKERS_PC = ["138.100.69.38", "138.100.69.39", "138.100.69.56"]

TOPIC_SUB = "sensores/datos"
TOPIC_PUB = "datos/rpi"

# Par�metros del ADC
Vref = 4.096
ADC_MAX = 32768

# Almacenamiento de datos
recorded_data = []

def raw_to_voltage(raw):
    return (raw / ADC_MAX) * Vref

# Callback al conectar
def on_connect(client, userdata, flags, rc):
    print("[INFO] Conectado al broker local con c�digo:", rc)
    client.subscribe(TOPIC_SUB)

# Callback al recibir datos
def on_message(client, userdata, msg):
    global recorded_data
    try:
        data = json.loads(msg.payload.decode('utf-8'))
        ts = data.get("timestamp", time.time() * 1000)
        t_unix = ts / 1000.0

        # Datos RAW
        c1 = data['C1']
        c2 = data['C2']
        c3 = data['C3']
        c4 = data['C4']

        v1 = raw_to_voltage(c1)
        v2 = raw_to_voltage(c2)
        v3 = raw_to_voltage(c3)
        v4 = raw_to_voltage(c4)

        # Corriente de referencia
        I = v1 / 10.0 if abs(v1) > 1e-6 else 1e-6

        # Resistencias
        r1 = (v2 - v1) / I
        r2 = (v3 - v2) / I
        r3 = (v4 - v3) / I

        # Fotointerruptores
        f1 = int(data.get("F1", False))
        f2 = int(data.get("F2", False))
        f3 = int(data.get("F3", False))

        # Sensores de posici�n
        p1 = int(data.get("P1", False))
        p2 = int(data.get("P2", False))
        p3 = int(data.get("P3", False))

        # Guardar fila
        fila = [t_unix, c1, c2, c3, c4, f1, f2, f3, p1, p2, p3, r1, r2, r3]
        recorded_data.append(fila)

        # Publicar a todas las PCs
        pub_data = {
            "timestamp": t_unix,
            "R1": r1,
            "R2": r2,
            "R3": r3,
            "P1": p1,
            "P2": p2,
            "P3": p3
        }
        for client_pub in clients_pub:
            client_pub.publish(TOPIC_PUB, json.dumps(pub_data))

        print("[DEBUG] Publicado a PCs:", pub_data)

    except Exception as e:
        print("[ERROR] Procesando mensaje:", e)

def guardar_csv():
    if not recorded_data:
        print("[WARNING] No hay datos para guardar.")
        return

    now = datetime.now()
    date_str = now.strftime("%Y%m%d")
    directory = os.path.abspath("CSV")
    os.makedirs(directory, exist_ok=True)

    # Nombre incremental
    existing_files = [f for f in os.listdir(directory) if f.startswith(f"{date_str}-RPi") and f.endswith(".csv")]
    num = len(existing_files) + 1
    filename = os.path.join(directory, f"{date_str}-RPi{num}.csv")

    headers = ["timestamp", "C1", "C2", "C3", "C4", "F1", "F2", "F3", "P1", "P2", "P3", "R1", "R2", "R3"]

    try:
        with open(filename, mode='w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(headers)
            writer.writerows(recorded_data)
        print(f"[INFO] Datos guardados en CSV: {filename}")
    except Exception as e:
        print(f"[ERROR] No se pudo guardar el archivo CSV: {e}")

# Crear cliente para recibir
client_sub = mqtt.Client()
client_sub.on_connect = on_connect
client_sub.on_message = on_message
client_sub.connect(BROKER_RPI, 1883, 60)

# Crear clientes para publicar
clients_pub = []
for broker in BROKERS_PC:
    client_pub = mqtt.Client()
    try:
        client_pub.connect(broker, 1883, 60)
        clients_pub.append(client_pub)
        print(f"[INFO] Conectado a broker PC: {broker}")
    except Exception as e:
        print(f"[ERROR] No se pudo conectar a {broker}: {e}")

try:
    client_sub.loop_start()
    print("[INFO] Esperando datos MQTT... Presiona Ctrl+C para salir.")
    while True:
        time.sleep(1)

except KeyboardInterrupt:
    print("\n[INFO] Detenido por el usuario. Guardando archivo CSV...")
    guardar_csv()
    client_sub.loop_stop()
    client_sub.disconnect()
