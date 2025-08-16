import os
import json
import pandas as pd

from infrastructure.model_inference_service import ModelInferenceService
from infrastructure.action_bus import ActionBus

def main():
    # Entradas/saídas
    input_csv = os.getenv("ORCH_INPUT", "/data/processed/dataset_labeled.csv")
    model_path = os.getenv("MODEL_PATH", "/data/models/anomaly_model.pkl")
    pending_path = os.getenv("ACTIONS_PENDING", "/data/actions/pending_actions.jsonl")
    executed_path = os.getenv("ACTIONS_EXECUTED", "/data/actions/executed_actions.jsonl")

    # Parâmetros de decisão
    threshold = float(os.getenv("THRESHOLD", "0.7"))
    priority_min = float(os.getenv("PRIORITY_MIN", "0.7"))  # se priority normalizada >= priority_min, dispara ação

    if not os.path.exists(input_csv):
        raise FileNotFoundError(f"Dataset para orquestração não encontrado: {input_csv}")
    df = pd.read_csv(input_csv)

    # Carrega modelo
    infer = ModelInferenceService(model_path)
    preds = infer.predict_batch(df)

    df_out = df.copy()
    df_out["pred_label"] = preds["pred_label"]
    df_out["pred_score"] = preds["pred_score"]

    bus = ActionBus(pending_path, executed_path)

    n_actions = 0
    for idx, row in df_out.iterrows():
        score = float(row.get("pred_score", 0.0))
        priority = float(row.get("priority", 0.0))  # assumindo priority já normalizada no analyzer
        triggerid = row.get("triggerid", "")
        description = row.get("description", "")
        hosts = row.get("hosts", "")

        # Regra simples: se score alto OU prioridade alta => abrir incidente/ack
        if (score >= threshold) or (priority >= priority_min):
            action = {
                "type": "ACK_TRIGGER",              # ou "OPEN_INCIDENT", conforme política
                "source": "orchestrator",
                "triggerid": triggerid,
                "description": description,
                "priority": priority,
                "score": score,
                "host_info": hosts,
                "rationale": f"score>={threshold} ou priority>={priority_min}"
            }
            bus.publish(action)
            n_actions += 1

    print(json.dumps({
        "orchestrator": "done",
        "actions_published": n_actions,
        "threshold": threshold,
        "priority_min": priority_min
    }, indent=2, ensure_ascii=False))

if __name__ == "__main__":
    main()

