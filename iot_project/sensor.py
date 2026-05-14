import requests
import random
import time
import sys

from cluster_manager import get_servers

sensor_id = int(sys.argv[1])

def generate():
    # Generate temps with significant deviation based on sensor ID for visible MapReduce results
    if sensor_id <= 6:
        # Cold sensors: 5-20°C
        temp = round(random.uniform(5, 20), 2)
    elif sensor_id <= 12:
        # Warm sensors: 25-35°C
        temp = round(random.uniform(25, 35), 2)
    else:
        # Hot sensors: 40-60°C
        temp = round(random.uniform(40, 60), 2)
    
    return {
        "sensor_id": sensor_id,
        "temperature": temp
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