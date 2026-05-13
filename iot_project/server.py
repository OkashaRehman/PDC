from flask import Flask, request, jsonify
from flask_cors import CORS
import requests
import threading
import time
import sys
from cluster_manager import register_server, remove_server as remove_from_cluster, get_servers

app = Flask(__name__)
CORS(app)

SERVER_ID = int(sys.argv[1])
MY_URL = f"http://127.0.0.1:{5000 + SERVER_ID}"

HEARTBEAT_INTERVAL = 3
TIMEOUT = 2

# Module-level globals
leader_id = None
data_store = []
logs = []
alive_servers = set()
shutdown_event = threading.Event()


@app.route("/data", methods=["POST"])
def receive_data():
    global data_store, logs

    if SERVER_ID != leader_id:
        return jsonify({"error": "Not leader"}), 403

    data = request.json
    data_store.append(data)

    log_msg = f"[LEADER {SERVER_ID}] Received: {data}"
    logs.append(log_msg)
    if len(logs) > 100:
        logs.pop(0)
    print(log_msg)

    replicate_data(data)

    return jsonify({"status": "stored"})


def get_server_url(sid):
    return f"http://127.0.0.1:{5000 + sid}"


def replicate_data(data):
    servers = get_servers()
    for s in servers:
        if s["id"] != SERVER_ID:
            try:
                requests.post(s["url"] + "/replicate", json=data, timeout=1)
            except:
                pass


@app.route("/replicate", methods=["POST"])
def replicate():
    global data_store, logs
    data = request.json
    data_store.append(data)

    log_msg = f"[SERVER {SERVER_ID}] Replicated: {data}"
    logs.append(log_msg)
    if len(logs) > 100:
        logs.pop(0)
    print(log_msg)
    return jsonify({"status": "ok"})


def heartbeat():
    global leader_id

    while True:
        time.sleep(HEARTBEAT_INTERVAL)

        if leader_id is None or SERVER_ID == leader_id:
            continue

        try:
            requests.get(get_server_url(leader_id) + "/ping", timeout=TIMEOUT)
        except:
            print(f"[SERVER {SERVER_ID}] Leader down. Starting election...")
            start_election()


@app.route("/ping", methods=["GET"])
def ping():
    return "alive"


def start_election():
    global leader_id

    servers = get_servers()
    server_ids = [s["id"] for s in servers]
    higher_servers = [sid for sid in server_ids if sid > SERVER_ID]

    response = False

    for sid in higher_servers:
        try:
            requests.post(get_server_url(sid) + "/election", timeout=1)
            response = True
        except:
            pass

    if not response:
        become_leader()


@app.route("/election", methods=["POST"])
def election():
    threading.Thread(target=start_election).start()
    return "OK"


def become_leader():
    global leader_id

    leader_id = SERVER_ID
    print(f"[SERVER {SERVER_ID}] I AM NEW LEADER")

    servers = get_servers()
    for s in servers:
        if s["id"] != SERVER_ID:
            try:
                requests.post(s["url"] + "/leader", json={"leader": SERVER_ID})
            except:
                pass


@app.route("/leader", methods=["POST"])
def update_leader():
    global leader_id, logs
    leader_id = request.json["leader"]
    log_msg = f"[SERVER {SERVER_ID}] New leader is {leader_id}"
    logs.append(log_msg)
    if len(logs) > 100:
        logs.pop(0)
    print(log_msg)
    return "OK"


@app.route("/status", methods=["GET"])
def status():
    global logs, data_store
    
    temps = [d.get("temperature") for d in data_store if "temperature" in d]
    traffic_counts = {"low": 0, "medium": 0, "high": 0}
    for d in data_store:
        if "traffic" in d:
            traffic_counts[d["traffic"]] = traffic_counts.get(d["traffic"], 0) + 1
    
    avg_temp = sum(temps) / len(temps) if temps else 0
    min_temp = min(temps) if temps else 0
    max_temp = max(temps) if temps else 0
    
    unique_sensors = len(set(d.get("sensor_id") for d in data_store if "sensor_id" in d))
    
    return jsonify({
        "server_id": SERVER_ID,
        "is_leader": SERVER_ID == leader_id,
        "records": len(data_store),
        "data": data_store[-100:],
        "logs": logs[-50:],
        "analytics": {
            "avg_temp": round(avg_temp, 2),
            "min_temp": round(min_temp, 2),
            "max_temp": round(max_temp, 2),
            "active_sensors": unique_sensors,
            "traffic_counts": traffic_counts
        }
    })


@app.route("/health", methods=["GET"])
def health_check():
    health = {}
    servers = get_servers()
    for s in servers:
        if s["id"] == SERVER_ID:
            continue
        try:
            requests.get(s["url"] + "/ping", timeout=1)
            health[s["id"]] = True
        except:
            health[s["id"]] = False
    return jsonify(health)


@app.route("/shutdown", methods=["GET"])
def shutdown():
    shutdown_event.set()
    return jsonify({"status": "shutdown"})


if __name__ == "__main__":
    register_server(SERVER_ID, MY_URL)
    
    time.sleep(2)
    
    servers = get_servers()
    server_ids = [s["id"] for s in servers]
    alive_servers = set(server_ids)

    if server_ids and SERVER_ID == max(server_ids):
        become_leader()
    else:
        print(f"[SERVER {SERVER_ID}] Waiting for leader...")

    threading.Thread(target=heartbeat, daemon=True).start()
    app.run(port=5000 + SERVER_ID)
