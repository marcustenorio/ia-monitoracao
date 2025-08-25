# src/agents/orchestrator/main.py
import os, json, time, uuid, hashlib
from datetime import datetime
import pandas as pd

TABULAR_INPUT   = os.getenv("ORCH_INPUT", "/data/processed/dataset_labeled.csv")
TS_INPUT        = os.getenv("ORCH_TS_INPUT", "/data/processed/anomalies_timeseries.csv")
PENDING_PATH    = os.getenv("ACTIONS_PENDING", "/data/actions/pending_actions.jsonl")
EXECUTED_PATH   = os.getenv("ACTIONS_EXECUTED", "/data/actions/executed_actions.jsonl")

THRESHOLD       = float(os.getenv("THRESHOLD", "0.7"))
PRIORITY_MIN    = float(os.getenv("PRIORITY_MIN", "0.7"))

TS_ENABLE       = os.getenv("TS_ENABLE", "true").lower() == "true"
TS_SCORE_FIELD  = os.getenv("TS_SCORE_FIELD", "score")
TS_FLAG_FIELD   = os.getenv("TS_FLAG_FIELD", "is_incident")
TS_MIN_SCORE    = float(os.getenv("TS_MIN_SCORE", "-0.1"))

LOOP_ENABLED    = os.getenv("ORCH_LOOP_ENABLED", "true").lower() == "true"
LOOP_SECONDS    = int(os.getenv("ORCH_LOOP_SECONDS", "60"))
STATE_PATH      = os.getenv("ORCH_STATE_PATH", "/data/actions/.orchestrator_state.json")
DEBUG           = os.getenv("ORCH_DEBUG", "false").lower() == "true"

def _now_iso(): return datetime.utcnow().isoformat() + "Z"

def _load_state():
    try:
        if os.path.exists(STATE_PATH):
            with open(STATE_PATH, "r") as f: return json.load(f)
    except Exception: pass
    return {"seen": []}

def _save_state(state):
    tmp = STATE_PATH + ".tmp"
    with open(tmp, "w") as f: json.dump(state, f)
    os.replace(tmp, STATE_PATH)

def _hash_id(*parts) -> str:
    h = hashlib.sha256()
    for p in parts: h.update(str(p).encode("utf-8"))
    return h.hexdigest()[:16]

def _publish_action(action: dict):
    os.makedirs(os.path.dirname(PENDING_PATH), exist_ok=True)
    with open(PENDING_PATH, "a") as f:
        f.write(json.dumps(action, ensure_ascii=False) + "\n")

def _debug_file_head(path, n=5):
    try:
        if os.path.exists(path):
            print(json.dumps({
                "debug": "file_stat",
                "path": path,
                "exists": True,
                "size_bytes": os.path.getsize(path)
            }))
            try:
                df = pd.read_csv(path)
                print(json.dumps({
                    "debug": "file_head",
                    "path": path,
                    "rows": len(df),
                    "cols": list(df.columns),
                    "head": df.head(min(n, len(df))).to_dict(orient="records")
                }, default=str))
            except Exception as e:
                print(json.dumps({"debug": "file_read_error", "path": path, "error": str(e)}))
        else:
            print(json.dumps({"debug": "file_stat", "path": path, "exists": False}))
    except Exception as e:
        print(json.dumps({"debug": "file_stat_error", "path": path, "error": str(e)}))

def _process_tabular(state):
    if not os.path.exists(TABULAR_INPUT):
        if DEBUG: print(json.dumps({"debug":"tabular_missing", "path": TABULAR_INPUT}))
        return 0
    try:
        df = pd.read_csv(TABULAR_INPUT)
    except Exception as e:
        print(json.dumps({"debug":"tabular_read_error", "error": str(e)}))
        return 0
    if DEBUG:
        print(json.dumps({"debug":"tabular_loaded", "rows": len(df), "cols": list(df.columns)}))

    pub = 0
    considered = 0
    passed = 0
    for _, row in df.iterrows():
        trig = row.get("triggerid")
        desc = row.get("description")
        prio = float(row.get("priority", 0))
        score = float(row.get("score", 0)) if "score" in df.columns else 0.0
        hosts = row.get("hosts")
        considered += 1
        cond = (score >= THRESHOLD) or (prio >= PRIORITY_MIN)
        if cond:
            passed += 1
            uid = _hash_id("tabular", trig, prio, desc)
            if uid in state["seen"]:
                continue
            action = {
                "type": "ACK_TRIGGER",
                "source": "orchestrator_tabular",
                "triggerid": trig,
                "description": desc,
                "priority": prio,
                "score": score,
                "host_info": str(hosts),
                "rationale": f"score>={THRESHOLD} ou priority>={PRIORITY_MIN}",
                "id": uid,
                "ts": _now_iso()
            }
            _publish_action(action)
            state["seen"].append(uid)
            pub += 1
    if DEBUG:
        print(json.dumps({"debug":"tabular_stats", "considered": considered, "passed": passed, "published": pub}))
    return pub

def _to_bool(v):
    if isinstance(v, bool): return v
    if isinstance(v, str): return v.lower() == "true"
    return bool(v)

def _process_timeseries(state):
    if not TS_ENABLE:
        if DEBUG: print(json.dumps({"debug":"ts_disabled"}))
        return 0
    if not os.path.exists(TS_INPUT):
        if DEBUG: print(json.dumps({"debug":"ts_missing", "path": TS_INPUT}))
        return 0
    try:
        df = pd.read_csv(TS_INPUT)
    except Exception as e:
        print(json.dumps({"debug":"ts_read_error", "error": str(e)}))
        return 0
    if DEBUG:
        print(json.dumps({"debug":"ts_loaded", "rows": len(df), "cols": list(df.columns)}))

    pub = 0
    considered = 0
    flagged_true = 0
    passed_threshold = 0
    for _, row in df.iterrows():
        considered += 1
        ts    = int(row.get("ts", 0))
        host  = row.get("host")
        key   = row.get("itemkey")
        val   = float(row.get("value", 0))
        score = float(row.get(TS_SCORE_FIELD, 0))
        flag  = _to_bool(row.get(TS_FLAG_FIELD))

        if flag: flagged_true += 1
        if score <= TS_MIN_SCORE: passed_threshold += 1

        if flag and (score <= TS_MIN_SCORE):
            uid = _hash_id("timeseries", host, key, ts, f"{score:.6f}")
            if uid in state["seen"]:
                continue
            action = {
                "type": "RAISE_INCIDENT",
                "source": "orchestrator_timeseries",
                "host": host,
                "itemkey": key,
                "value": val,
                "score": score,
                "rationale": f"IF decision_function <= {TS_MIN_SCORE}",
                "ts": ts,
                "ts_iso": datetime.utcfromtimestamp(ts).isoformat()+"Z",
                "id": uid,
                "published_at": _now_iso()
            }
            _publish_action(action)
            state["seen"].append(uid)
            pub += 1
    if DEBUG:
        print(json.dumps({
            "debug":"ts_stats",
            "considered": considered,
            "flag_true": flagged_true,
            "passed_threshold": passed_threshold,
            "published": pub,
            "min_score_required": TS_MIN_SCORE
        }))
    return pub

def run_once():
    print(json.dumps({"heartbeat":"start_cycle", "utc": _now_iso()}))
    if DEBUG:
        _debug_file_head(TABULAR_INPUT)
        _debug_file_head(TS_INPUT)

    state = _load_state()
    pub_tab = _process_tabular(state)
    pub_ts  = _process_timeseries(state)
    _save_state(state)

    summary = {
        "orchestrator": "done",
        "published_tabular": pub_tab,
        "published_timeseries": pub_ts,
        "total_published": pub_tab + pub_ts,
        "timestamp_utc": _now_iso()
    }
    print(json.dumps(summary, ensure_ascii=False))
    return pub_tab + pub_ts

def main():
    if os.getenv("ORCH_LOOP_ENABLED", "true").lower() != "true":
        run_once()
        return
    while True:
        try:
            run_once()
        except Exception as e:
            print(json.dumps({"orchestrator": "error", "message": str(e), "timestamp_utc": _now_iso()}))
        time.sleep(LOOP_SECONDS)

if __name__ == "__main__":
    main()
