#!/usr/bin/env python3
# scripts/generate_incident_report.py
# Gera relatório de INCIDENTES (RAISE_INCIDENT) a partir de pending/executed JSONL.

import argparse, json, os, csv
from datetime import datetime

def read_jsonl(path):
    rows = []
    if not os.path.exists(path):
        return rows
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line=line.strip()
            if not line: 
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return rows

def parse_pending(pending_rows):
    """Extrai INCIDENTES do pending_actions.jsonl (type=='RAISE_INCIDENT')."""
    out = []
    for r in pending_rows:
        if r.get("type") == "RAISE_INCIDENT":
            out.append({
                "id": r.get("id"),
                "datetime": r.get("published_at") or r.get("ts_iso") or "",
                "host": r.get("host"),
                "itemkey": r.get("itemkey"),
                "value": r.get("value"),
                "score": r.get("score"),
                "ts_iso": r.get("ts_iso"),
                "status": "pending",
            })
    return out

def parse_executed(executed_rows):
    """Extrai INCIDENTES do executed_actions.jsonl (action.type=='RAISE_INCIDENT')."""
    out = []
    for r in executed_rows:
        act = r.get("action", {})
        if act.get("type") == "RAISE_INCIDENT":
            out.append({
                "id": r.get("id"),
                "datetime": r.get("ts_executed") or act.get("ts_iso") or "",
                "host": act.get("host"),
                "itemkey": act.get("itemkey"),
                "value": act.get("value"),
                "score": act.get("score"),
                "ts_iso": act.get("ts_iso"),
                "status": "executed",
            })
    return out

def to_dt(s):
    # tenta normalizar datas para ordenar
    for fmt in ("%Y-%m-%dT%H:%M:%S.%fZ","%Y-%m-%dT%H:%M:%SZ","%Y%m%dT%H%M%SZ"):
        try:
            return datetime.strptime(s, fmt)
        except Exception:
            pass
    return None

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pending", default="data/actions/pending_actions.jsonl")
    ap.add_argument("--executed", default="data/actions/executed_actions.jsonl")
    ap.add_argument("--out-csv", default="data/reports/incidents_report.csv")
    ap.add_argument("--out-md", default="data/reports/incidents_report.md")
    args = ap.parse_args()

    os.makedirs(os.path.dirname(args.out_csv), exist_ok=True)

    pending = read_jsonl(args.pending)
    executed = read_jsonl(args.executed)

    rows_p = parse_pending(pending)
    rows_e = parse_executed(executed)

    # Dedup por (host, itemkey, ts_iso) mantendo executed > pending
    merged_map = {}
    for r in rows_p + rows_e:
        key = (r["host"], r["itemkey"], r["ts_iso"])
        # Se já existe e o novo é executed, sobrescreve
        if key in merged_map and merged_map[key]["status"] == "pending" and r["status"] == "executed":
            merged_map[key] = r
        elif key not in merged_map:
            merged_map[key] = r

    merged = list(merged_map.values())

    # Ordena por datetime asc
    def sort_key(r):
        dt = to_dt(r["datetime"])
        return dt or datetime.max
    merged.sort(key=sort_key)

    # CSV
    with open(args.out_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["datetime","host","itemkey","value","score","ts_iso","status"])
        for r in merged:
            w.writerow([r["datetime"], r["host"], r["itemkey"], r["value"], r["score"], r["ts_iso"], r["status"]])

    # Markdown (tabela resumida)
    with open(args.out_md, "w", encoding="utf-8") as f:
        f.write("# Incident Report (RAISE_INCIDENT)\n\n")
        f.write(f"- Total: **{len(merged)}**\n\n")
        f.write("| datetime | host | itemkey | value | score | ts_iso | status |\n")
        f.write("|---|---|---|---:|---:|---|---|\n")
        for r in merged:
            f.write(f"| {r['datetime']} | {r['host']} | `{r['itemkey']}` | {r['value']:.6f} | {r['score']:.6f} | {r['ts_iso']} | {r['status']} |\n")

    print(f"CSV salvo em: {args.out_csv}")
    print(f"Markdown salvo em: {args.out_md}")
    print(f"Total de INCIDENTES: {len(merged)}")

if __name__ == "__main__":
    main()

