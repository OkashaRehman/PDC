import json
import os
from threading import Lock

lock = Lock()
CLUSTER_FILE = "cluster.json"

def read_cluster():
    with lock:
        if not os.path.exists(CLUSTER_FILE):
            return []
        with open(CLUSTER_FILE, "r") as f:
            try:
                return json.load(f)
            except:
                return []

def write_cluster(data):
    with lock:
        with open(CLUSTER_FILE, "w") as f:
            json.dump(data, f, indent=4)

def register_server(server_id, url):
    cluster = read_cluster()
    exists = any(s["id"] == server_id for s in cluster)
    if not exists:
        cluster.append({"id": server_id, "url": url})
        write_cluster(cluster)

def remove_server(server_id):
    cluster = read_cluster()
    cluster = [s for s in cluster if s["id"] != server_id]
    write_cluster(cluster)

def get_servers():
    return read_cluster()