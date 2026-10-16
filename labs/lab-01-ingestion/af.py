"""Мини-клиент REST API Airflow для demo-скрипта и сбора результатов.

python af.py runs <dag_id>                 состояния запусков
python af.py tasks <dag_id> <run_id>       состояния задач запуска
python af.py xcom <dag_id> <run_id> <task> return_value задачи
python af.py trigger <dag_id> '<conf json>'
python af.py wait <dag_id> [<dag_id>...]   ждать, пока не останется queued/running
"""
import json
import os
import sys
import time
import urllib.request

URL = os.getenv("AIRFLOW_URL", "http://localhost:18088")


def _req(method: str, path: str, body: dict | None = None, token: str | None = None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(URL + path, data=data, method=method,
                                 headers={"Content-Type": "application/json",
                                          **({"Authorization": f"Bearer {token}"} if token else {})})
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.loads(r.read() or b"{}")


TOKEN = None


def api(method: str, path: str, body: dict | None = None):
    global TOKEN
    if TOKEN is None:
        TOKEN = _req("POST", "/auth/token", {"username": os.getenv("AIRFLOW_USER", "admin"),
                                             "password": os.getenv("AIRFLOW_PASSWORD", "admin")})["access_token"]
    return _req(method, path, body, TOKEN)


def runs(dag_id: str) -> list[dict]:
    return api("GET", f"/api/v2/dags/{dag_id}/dagRuns?limit=100&order_by=logical_date")["dag_runs"]


def tasks(dag_id: str, run_id: str) -> list[dict]:
    return api("GET", f"/api/v2/dags/{dag_id}/dagRuns/{run_id}/taskInstances")["task_instances"]


def xcom(dag_id: str, run_id: str, task_id: str):
    path = f"/api/v2/dags/{dag_id}/dagRuns/{run_id}/taskInstances/{task_id}/xcomEntries/return_value"
    return api("GET", path)["value"]


def wait(dag_ids: list[str], timeout: int = 1800) -> None:
    t0 = time.time()
    while time.time() - t0 < timeout:
        active = {d: sum(r["state"] in ("queued", "running") for r in runs(d)) for d in dag_ids}
        if not any(active.values()):
            return
        print("ждём:", active, flush=True)
        time.sleep(20)
    raise TimeoutError(dag_ids)


if __name__ == "__main__":
    cmd, *a = sys.argv[1:]
    if cmd == "runs":
        for r in runs(a[0]):
            print(r["dag_run_id"], r["logical_date"], r["state"])
    elif cmd == "tasks":
        for t in tasks(*a):
            print(t["task_id"], t["state"], t.get("try_number"))
    elif cmd == "xcom":
        print(json.dumps(xcom(*a), ensure_ascii=False))
    elif cmd == "trigger":
        conf = json.loads(a[1]) if len(a) > 1 else {}
        print(api("POST", f"/api/v2/dags/{a[0]}/dagRuns", {"logical_date": None, "conf": conf})["dag_run_id"])
    elif cmd == "wait":
        wait(a)
