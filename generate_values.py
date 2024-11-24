import paho.mqtt.client as mqtt
import json
import time
import random
import threading
import pandas as pd
import numpy as np 
from dotenv import load_dotenv
import os

load_dotenv()

token_temperatura = os.getenv("DEVICE_TEMPERATURA")
token_umidade = os.getenv("DEVICE_UMIDADE")
token_luminosidade = os.getenv("DEVICE_LUMINOSIDADE")
token_ruido = os.getenv("DEVICE_RUIDO")

path = os.environ["DEVICE_TEMPERATURA"]

THINGSBOARD_HOST = 'localhost' 
ACESS_TOKENS = [token_temperatura, token_umidade, token_luminosidade, token_ruido]
DEVICES = ['temperatura','umidade','luminosidade','ruido']
csv_path = 'devices.csv'

def on_connect(client, userdata, connect_flags, reason_code, properties):
    client_id = client._client_id.decode()
    if reason_code == 0:
        print(f"Cliente {client_id} conectado ao broker com sucesso.")
    else:
        print(f"Cliente {client_id} falhou ao conectar")

def on_disconnect(client, userdata, disconnect_flags, reason_code, properties):
    client_id = client._client_id.decode() 
    print(f"Cliente {client_id} desconectado do ThingsBoard")

def client_mqtt(i, client, media_total, desv_padrao):
    valor_anterior = media_total
    alpha = 0.8 # fator de suavizacao
    escala_ruido = desv_padrao * 0.1 # 10% do desvio padrão
    
    while True:
        try:
            ruido = np.random.normal(loc=0, scale=escala_ruido)
           # % de chance de ocorrer um evento inconsistente (coloquei como padrão 3%)
            if random.random() < 0.03:
                print(f"Evento inconsistente detectado no sensor {DEVICES[i]}")
                ruido += np.random.normal(loc=0, scale=desv_padrao)
            
            valor = alpha * valor_anterior + (1 - alpha) * media_total + ruido
            valor = max(min(valor, media_total + 3*desv_padrao), media_total - 3*desv_padrao)
            
            payload = json.dumps({ DEVICES[i]: valor })
            client.publish('v1/devices/me/telemetry', payload, qos=1)
            print(f"{DEVICES[i]} enviada: {valor}")
            
            valor_anterior = valor
            time.sleep(2)
            
        except KeyboardInterrupt:
            print("Finalizando programa...")
            break  
        except Exception as e:
            print(f"Erro: {e}")
            break
        
def main():
    threads = []
    clients = []
    
    print("Iniciando script de simulação de sensores...")

    df = pd.read_csv(csv_path, delimiter='|')
    df = df.dropna()

    # para pegar device aleatório
    # device_name = df['device'].sample(1).valors[0] 
    device_name = "sirrosteste_UCS_AMV-14"
    print(f"Simulando dados para a partir do dispositivo: {device_name}")

    device_df = df[df['device'] == device_name]

    for i in range(4):
        client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2,client_id=f"Sensor de {DEVICES[i]}")

        client.username_pw_set(ACESS_TOKENS[i])  
        client.on_connect = on_connect
        client.on_disconnect = on_disconnect
        
        client.connect(THINGSBOARD_HOST, 1883, 9999)
        client.loop_start()
        clients.append(client)
    
    for i in range(4):
        sub_df = device_df[DEVICES[i]]
        media_total = sub_df.mean()
        desv_padrao = sub_df.std()
        t = threading.Thread(target=client_mqtt, args=(i, clients[i], media_total, desv_padrao))
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

if __name__ == "__main__":
    main()
