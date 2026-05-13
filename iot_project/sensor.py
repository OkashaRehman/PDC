import requests
import random
import time
import sys

from cluster_manager import get_servers

sensor_id = int(sys.argv[1])

def generate():
    return {
        "sensor_id": sensor_id,
        "temperature": round(random.uniform(20, 40), 2),
        "traffic": random.choice(["low", "medium", "high"])
    }

def get_leader():
    servers = get_servers()
    for s in servers:
        try:
            r = requests.get(s["url"] + "/status", timeout=1)
            data = r.json()
            if data["is_leader"]:
                return s
        except:
            pass
    return None

while True:
    leader = get_leader()
    if leader is None:
        print("No leader")
        time.sleep(1)
        continue
    try:
        data = generate()
        requests.post(leader["url"] + "/data", json=data)
        print(f"[Sensor {sensor_id}] {data}")
    except:
        print("Leader error")
    time.sleep(1)