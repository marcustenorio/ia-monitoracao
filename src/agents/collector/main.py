#Autor: MArcus Tenorio
#Horario: 14h36
import pandas as pd
from pyzabbix import ZabbixAPI, ZabbixAPIException
import os
import time
import sys

# Configurações via environment variables ou default
ZABBIX_URL = os.getenv("ZABBIX_URL", "http://zabbix-web:8080")
ZABBIX_USER = os.getenv("ZABBIX_USER", "Admin")
ZABBIX_PASS = os.getenv("ZABBIX_PASS", "zabbix")
OUTPUT_PATH = "/data/processed/anomalies_dataset.csv"

# Tempo máximo de espera pelo Zabbix Web (em segundos)
TIMEOUT = 120
INTERVAL = 5

print(f"Aguardando Zabbix API em {ZABBIX_URL}...")

start_time = time.time()
while True:
    try:
        zapi = ZabbixAPI(ZABBIX_URL)
        zapi.login(ZABBIX_USER, ZABBIX_PASS)
        print("Conectado ao Zabbix API com sucesso!")
        break
    except ZabbixAPIException:
        elapsed = time.time() - start_time
        if elapsed > TIMEOUT:
            print(f"Timeout ({TIMEOUT}s) atingido. Zabbix Web não disponível.")
            sys.exit(1)
        print(f"Zabbix API ainda não disponível. Tentando novamente em {INTERVAL}s...")
        time.sleep(INTERVAL)

# Listar todas triggers
triggers_all = zapi.trigger.get(
    output=["triggerid", "description", "priority", "lastchange"],
    selectHosts=["hostid", "name"]
)
print(f"Total de triggers encontradas: {len(triggers_all)}")

# Mostrar triggers no console
for t in triggers_all:
    hosts = [h['name'] for h in t.get('hosts', [])]
    print(f"TriggerID: {t['triggerid']}, Host(s): {hosts}, Desc: {t['description']}, Prioridade: {t['priority']}")

# Filtrar triggers do host "Zabbix server"
triggers = [t for t in triggers_all if any(h['name'] == "Zabbix server" for h in t.get('hosts', []))]
print(f"{len(triggers)} trigger(s) do host 'Zabbix server' encontrados.")

# Criar DataFrame e salvar CSV
df = pd.DataFrame(triggers)
os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
df.to_csv(OUTPUT_PATH, index=False)
print(f"Dados salvos em {OUTPUT_PATH}")

# Logout
zapi.user.logout()
print("Logout realizado com sucesso.")
