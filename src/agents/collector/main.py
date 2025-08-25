# src/agents/collector/main.py
# Autor: Marcus Tenório
# Coleta contínua: triggers + séries temporais básicas (CPU user/system) do Zabbix

import os
import time
import sys
import pandas as pd
from pyzabbix import ZabbixAPI, ZabbixAPIException

# ================== Config ==================
ZABBIX_URL  = os.getenv("ZABBIX_URL",  "http://zabbix-web:8080")
ZABBIX_USER = os.getenv("ZABBIX_USER", "Admin")
ZABBIX_PASS = os.getenv("ZABBIX_PASS", "zabbix")

# Onde salvar (mantemos os mesmos caminhos para não quebrar o pipeline):
OUT_TABULAR = "/data/processed/anomalies_dataset.csv"
OUT_TS      = "/data/processed/anomalies_timeseries.csv"

# Streaming: intervalo e janela
COLLECT_INTERVAL_SEC = int(os.getenv("COLLECT_INTERVAL_SEC", "30"))  # ex.: 30s
ZBX_WINDOW_MIN = int(os.getenv("ZBX_WINDOW_MIN", "5"))               # últimos 5 minutos

# Quais itens de série temporal coletar por host (mantemos simples)
CPU_KEYS = [
    "system.cpu.util[,system]",
    "system.cpu.util[,user]"
]

# ================== Funções ==================
def connect_zabbix():
    zapi = ZabbixAPI(ZABBIX_URL)
    zapi.login(ZABBIX_USER, ZABBIX_PASS)
    return zapi

def collect_triggers(zapi: ZabbixAPI) -> pd.DataFrame:
    triggers_all = zapi.trigger.get(
        output=["triggerid", "description", "priority", "lastchange"],
        selectHosts=["hostid", "name"]
    ) or []
    df = pd.DataFrame(triggers_all)
    return df

def collect_timeseries(zapi: ZabbixAPI) -> pd.DataFrame:
    """
    Busca itens de CPU por host e agrega últimas leituras (ZBX_WINDOW_MIN).
    Estratégia simples: pegar últimos pontos (history.get) e montar um dataframe.
    """
    # Descobrir hosts
    hosts = zapi.host.get(output=["hostid", "name"]) or []
    rows = []
    now = int(time.time())
    since = now - (ZBX_WINDOW_MIN * 60)

    for h in hosts:
        hostid = h["hostid"]
        hostnm = h["name"]

        # Descobre itens do host que batem com nossas keys
        items = zapi.item.get(hostids=hostid, search={"key_": "system.cpu.util"}, output=["itemid", "name", "key_"]) or []

        # Filtra só as chaves desejadas
        for it in items:
            if it.get("key_") not in CPU_KEYS:
                continue

            iid = it["itemid"]
            key = it["key_"]

            # history.get: value_type 0 (float) costuma cobrir CPU util; se necessário adaptar
            hist = zapi.history.get(
                history=0,
                itemids=iid,
                time_from=since,
                time_till=now,
                sortfield="clock",
                sortorder="ASC"
            ) or []

            for p in hist:
                ts = int(p["clock"])
                val = float(p.get("value", 0.0))
                rows.append({
                    "ts": ts,
                    "ts_iso": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(ts)),
                    "host": hostnm,
                    "itemkey": key,
                    "value": val
                })

    df = pd.DataFrame(rows)
    if df.empty:
        return df

    # Score simples (placeholder) — aqui seu analyzer_timeseries/IF pode recalcular depois.
    # Mantemos as colunas esperadas pelo orchestrator.
    df["score"] = 0.0
    df["threshold"] = -0.1
    df["is_incident"] = False
    return df

def write_csv_safely(df: pd.DataFrame, path: str):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    # Escreve de forma atômica simples (evita arquivo vazio durante escrita)
    tmp = path + "._tmp"
    df.to_csv(tmp, index=False)
    os.replace(tmp, path)

# ================== Main (streaming) ==================
if __name__ == "__main__":
    # Primeiro: tenta conectar (com tolerância)
    start = time.time()
    timeout = 120
    while True:
        try:
            zapi = connect_zabbix()
            print(f"[collector] Conectado ao Zabbix API: {ZABBIX_URL}")
            break
        except ZabbixAPIException as e:
            if time.time() - start > timeout:
                print(f"[collector] Timeout conectando no Zabbix: {e}")
                sys.exit(1)
            print("[collector] Zabbix ainda não disponível, tentando de novo em 5s...")
            time.sleep(5)

    # Loop contínuo
    while True:
        cycle_t0 = time.time()
        try:
            # 1) Triggers (tabular)
            df_tr = collect_triggers(zapi)
            if not df_tr.empty:
                write_csv_safely(df_tr, OUT_TABULAR)
                print(f"[collector] triggers -> {OUT_TABULAR} (rows={len(df_tr)})")
            else:
                print("[collector] triggers vazias (nada a escrever)")

            # 2) Séries temporais recentes
            df_ts = collect_timeseries(zapi)
            if not df_ts.empty:
                write_csv_safely(df_ts, OUT_TS)
                print(f"[collector] timeseries (últimos {ZBX_WINDOW_MIN} min) -> {OUT_TS} (rows={len(df_ts)})")
            else:
                print(f"[collector] timeseries vazias (janela {ZBX_WINDOW_MIN} min)")

        except Exception as e:
            # Não cai o container; apenas loga e segue
            print(f"[collector][warn] erro no ciclo: {e}")

        # Intervalo entre ciclos
        elapsed = int(time.time() - cycle_t0)
        sleep_s = max(1, COLLECT_INTERVAL_SEC - elapsed)
        time.sleep(sleep_s)
