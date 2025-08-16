import os
import json
from infrastructure.action_bus import ActionBus

def simulate_ack_trigger(action: dict) -> dict:
    """
    Simula um ACK na trigger do Zabbix. Aqui você pode:
      - Chamar a API do Zabbix para acknowledge
      - Abrir ticket (Jira/GLPI)
      - Efetuar restart/scale/out, etc.
    Mantemos simulado para não alterar o seu ambiente.
    """
    triggerid = action.get("triggerid")
    desc = action.get("description", "")
    # Exemplo de retorno simulado
    return {
        "status": "OK",
        "message": f"SIMULATED: acknowledged trigger {triggerid} - {desc}"
    }

def main():
    pending_path = os.getenv("ACTIONS_PENDING", "/data/actions/pending_actions.jsonl")
    executed_path = os.getenv("ACTIONS_EXECUTED", "/data/actions/executed_actions.jsonl")

    bus = ActionBus(pending_path, executed_path)
    actions = bus.pop_all_pending()

    executed = 0
    for action in actions:
        atype = action.get("type", "UNKNOWN")
        if atype == "ACK_TRIGGER":
            result = simulate_ack_trigger(action)
        else:
            result = {"status": "SKIPPED", "message": f"Tipo de ação não suportado: {atype}"}
        bus.mark_executed(action, result)
        executed += 1

    print(json.dumps({
        "executor": "done",
        "actions_processed": executed
    }, indent=2, ensure_ascii=False))

if __name__ == "__main__":
    main()

