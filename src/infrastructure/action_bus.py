import json
import os
import uuid
from datetime import datetime, timezone
from typing import Dict

class ActionBus:
    """
    Barramento simples via JSON Lines em disco.
    Orchestrator publica ações em pending_actions.jsonl.
    Executor lê, executa (simulado) e move para executed_actions.jsonl.
    """

    def __init__(self, pending_path: str, executed_path: str):
        self.pending = pending_path
        self.executed = executed_path
        os.makedirs(os.path.dirname(self.pending), exist_ok=True)
        os.makedirs(os.path.dirname(self.executed), exist_ok=True)

    def _now(self) -> str:
        return datetime.now(timezone.utc).isoformat()

    def publish(self, action: Dict) -> Dict:
        # Garante campos padrão
        if "id" not in action:
            action["id"] = str(uuid.uuid4())
        if "ts" not in action:
            action["ts"] = self._now()
        with open(self.pending, "a") as f:
            f.write(json.dumps(action, ensure_ascii=False) + "\n")
        return action

    def pop_all_pending(self):
        if not os.path.exists(self.pending):
            return []
        with open(self.pending, "r") as f:
            lines = [l.strip() for l in f if l.strip()]
        # zera a fila (one-shot)
        open(self.pending, "w").close()
        actions = []
        for ln in lines:
            try:
                actions.append(json.loads(ln))
            except json.JSONDecodeError:
                pass
        return actions

    def mark_executed(self, action: Dict, result: Dict):
        record = {
            "id": action.get("id"),
            "ts_executed": self._now(),
            "action": action,
            "result": result
        }
        with open(self.executed, "a") as f:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")

