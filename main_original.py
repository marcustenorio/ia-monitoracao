import pandas as pd
from pyzabbix import ZabbixAPI, ZabbixAPIException
import os
import time
import requests
import sys

# Configurações via environment variables ou default
ZABBIX_URL = os.getenv("ZABBIX_URL", "http://zabbix-web:8080/api_jsonrpc.php")
ZABBIX_USER = os.getenv("ZABBIX_USER", "Admin")
ZABBIX_PASS = os.getenv("ZABBIX_PASS", "zabbix")
OUTPUT_PATH = "/data/processed/anomalies_dataset.csv"  # path dentro do container

# Tempo máximo de espera pelo Zabbix Web (em segundos)
TIMEOUT = 120
INTERVAL = 5

print(f"Aguardando Zabbix Web em {ZABBIX_URL}...")

start_time = time.time()
while True:
    try:
        # Teste de acesso HTTP à API
        response = requests.post(
            ZABBIX_URL,
            json={"jsonrpc": "2.0", "method": "apiinfo.version", "id": 1, "auth": None, "params": {}}
        )
        if response.status_code == 200:
            print("Zabbix Web acessível!")
            break
    except requests.exceptions.RequestException:
        pass

    elapsed = time.time() - start_time
    if elapsed > TIMEOUT:
        print(f"Timeout ({TIMEOUT}s) atingido. Zabbix Web não disponível.")
        sys.exit(1)

    print(f"Zabbix Web ainda não disponível. Tentando novamente em {INTERVAL}s...")
    time.sleep(INTERVAL)

# Conectar ao Zabbix API
try:
    #zapi = ZabbixAPI(ZABBIX_URL)
    #zapi.login(ZABBIX_USER, ZABBIX_PASS)
    zapi = ZabbixAPI(ZABBIX_URL)
    zapi.session.post(
        ZABBIX_URL,
        json={
            "jsonrpc": "2.0",
            "method": "user.login",
            "params": {
                "username": ZABBIX_USER,  # ← troca aqui
                "password": ZABBIX_PASS
            },
            "id": 1
        }
    ).json()
    auth = zapi.auth  # Se precisar manter a sessão
    print("Conectado ao Zabbix API (login manual).")


    print("Conectado ao Zabbix API.")
except ZabbixAPIException as e:
    print(f"Erro ao logar no Zabbix API: {e}")
    sys.exit(1)

# Debug: listar todas triggers
triggers_all = zapi.trigger.get(
    output=["triggerid", "description", "priority", "lastchange"],
    selectHosts=["hostid", "name"]
)
print(f"Total de triggers encontradas: {len(triggers_all)}")
for t in triggers_all:
    hosts = [h['name'] for h in t.get('hosts', [])]
    print(f"TriggerID: {t['triggerid']}, Host(s): {hosts}, Desc: {t['description']}, Prioridade: {t['priority']}")

# Filtrar triggers do host "Zabbix server"
triggers = [t for t in triggers_all if any(h['name'] == "Zabbix server" for h in t.get('hosts', []))]
print(f"{len(triggers)} trigger(s) do host 'Zabbix server' encontrados.")

# Criar DataFrame e salvar CSV
df = pd.DataFrame(triggers)
df.to_csv(OUTPUT_PATH, index=False)
print(f"Dados salvos em {OUTPUT_PATH}")

zapi.logout()
print("Logout realizado com sucesso.")
