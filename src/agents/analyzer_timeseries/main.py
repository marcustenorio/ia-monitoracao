# src/agents/analyzer_timeseries/main.py
import os
import glob
import time
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from sklearn.ensemble import IsolationForest

RAW_DIR = os.getenv("TS_INPUT_DIR", "/data/raw/timeseries")
OUTPUT = os.getenv("TS_OUTPUT_CSV", "/data/processed/anomalies_timeseries.csv")

# janela em minutos considerada "contexto"
WINDOW_MIN = int(os.getenv("TS_WINDOW_MIN", "120"))
# tamanho da janela (em pontos) para calcular média móvel e std (ex.: 5 últimos pontos)
ROLL_N = int(os.getenv("TS_ROLL_N", "5"))
# threshold do score (mais baixo = mais sensível; IsolationForest usa decision_function)
THRESHOLD = float(os.getenv("TS_THRESHOLD", "-0.1"))

os.makedirs(os.path.dirname(OUTPUT), exist_ok=True)

def build_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    df com colunas: ts, value, host, itemkey (ordenado por ts)
    Gera: rolling_mean, rolling_std, diff, zscore_rolling
    """
    df = df.sort_values("ts").copy()
    df["rolling_mean"] = df["value"].rolling(ROLL_N, min_periods=1).mean()
    df["rolling_std"]  = df["value"].rolling(ROLL_N, min_periods=1).std().fillna(0.0)
    df["diff"] = df["value"].diff().fillna(0.0)
    # z-score local na janela
    df["zscore_rolling"] = (df["value"] - df["rolling_mean"]) / (df["rolling_std"].replace(0, np.nan))
    df["zscore_rolling"] = df["zscore_rolling"].fillna(0.0)
    return df

def detect_last_point_anomaly(df: pd.DataFrame) -> dict:
    """
    Treina IsolationForest na **janela** e avalia somente o último ponto.
    Retorna dict com score e flag incidente.
    """
    if len(df) < max(ROLL_N, 10):
        return {"score": None, "is_incident": False, "reason": "pouca_amostra"}

    feats = df[["value", "rolling_mean", "rolling_std", "diff", "zscore_rolling"]].values
    model = IsolationForest(
        n_estimators=200,
        contamination="auto",
        random_state=42
    )
    model.fit(feats)

    # decision_function: > 0 mais normal; < 0 aponta anomalia
    scores = model.decision_function(feats)
    last_score = float(scores[-1])
    return {
        "score": last_score,
        "is_incident": last_score <= THRESHOLD,
        "reason": f"decision_function<= {THRESHOLD}"
    }

def main():
    now = int(time.time())
    window_from = now - WINDOW_MIN * 60

    rows_out = []
    files = glob.glob(os.path.join(RAW_DIR, "*.csv"))
    print(f"[analyzer-ts] lendo {len(files)} séries em {RAW_DIR}, janela {WINDOW_MIN} min...")

    for path in files:
        try:
            df = pd.read_csv(path)
            if not {"ts", "value", "host", "itemkey"}.issubset(df.columns):
                continue
            df = df[df["ts"] >= window_from]
            if df.empty:
                continue

            df_feat = build_features(df)
            res = detect_last_point_anomaly(df_feat)
            if res["score"] is None:
                continue

            last = df_feat.iloc[-1]
            rows_out.append({
                "ts": int(last["ts"]),
                "ts_iso": datetime.utcfromtimestamp(int(last["ts"])).isoformat()+"Z",
                "host": str(last["host"]),
                "itemkey": str(last["itemkey"]),
                "value": float(last["value"]),
                "score": float(res["score"]),
                "threshold": THRESHOLD,
                "is_incident": bool(res["is_incident"])
            })
        except Exception as e:
            print(f"[analyzer-ts] erro em {path}: {e}")

    if rows_out:
        df_out = pd.DataFrame(rows_out).sort_values(["host","itemkey","ts"])
        df_out.to_csv(OUTPUT, index=False)
        print(f"[analyzer-ts] resultados -> {OUTPUT} (n={len(df_out)})")
    else:
        print("[analyzer-ts] sem resultados (amostras insuficientes ou sem arquivos).")

if __name__ == "__main__":
    main()

