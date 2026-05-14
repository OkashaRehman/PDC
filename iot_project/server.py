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
MAPREDUCE_INTERVAL = 5  # Run MapReduce every 5 seconds

# Module-level globals
leader_id = None
data_store = []  # Leader stores all sensor data here
logs = []
alive_servers = set()
shutdown_event = threading.Event()
mapreduce_results = {}  # {worker_id: {avg, min, max, count}}
final_analytics = {"avg_temp": 0, "min_temp": 0, "max_temp": 0}
avg_temp_history = []  # Track average temperature over time for charting


@app.route("/data", methods=["POST"])
def receive_data():
    """LEADER ONLY: Receive sensor data and store"""
    global data_store, logs

    if SERVER_ID != leader_id:
        return jsonify({"error": "Not leader"}), 403

    data = request.json
    data_store.append(data)

    log_msg = f"[LEADER {SERVER_ID}] Received data from sensor {data.get('sensor_id')}: {data.get('temperature')}°C"
    logs.append(log_msg)
    if len(logs) > 100:
        logs.pop(0)
    print(log_msg)

    return jsonify({"status": "stored"})


def get_server_url(sid):
    return f"http://127.0.0.1:{5000 + sid}"


@app.route("/map_chunk", methods=["POST"])
def map_chunk():
    """WORKER: Receive data chunk from leader, process it locally"""
    global logs
    
    payload = request.json
    chunk = payload.get("chunk", [])
    task_id = payload.get("task_id")
    leader_url = payload.get("leader_url")
    
    # MAP: Process chunk locally
    temps = [d.get("temperature") for d in chunk if "temperature" in d]
    
    if temps:
        local_result = {
            "avg": round(sum(temps) / len(temps), 2),
            "min": round(min(temps), 2),
            "max": round(max(temps), 2),
            "count": len(temps)
        }
    else:
        local_result = {"avg": 0, "min": 0, "max": 0, "count": 0}
    
    log_msg = f"[WORKER {SERVER_ID}] MAP: Received chunk {task_id} with {local_result['count']} records → LOCAL: avg={local_result['avg']}°C min={local_result['min']}°C max={local_result['max']}°C"
    logs.append(log_msg)
    if len(logs) > 100:
        logs.pop(0)
    print(log_msg)
    
    # Send result back to leader via /reduce
    if leader_url:
        try:
            print(f"[WORKER {SERVER_ID}] Sending result back to leader at {leader_url}/reduce...")
            requests.post(
                leader_url + "/reduce",
                json={"worker_id": SERVER_ID, "task_id": task_id, "result": local_result},
                timeout=2
            )
            print(f"[WORKER {SERVER_ID}] ✓ Result delivered to leader")
        except Exception as e:
            print(f"[WORKER {SERVER_ID}] ✗ Failed to send result to leader: {e}")
    
    return jsonify({"worker_id": SERVER_ID, "task_id": task_id, "result": local_result})


@app.route("/reduce", methods=["POST"])
def reduce():
    """LEADER: Collect results from workers and compute final aggregate"""
    global mapreduce_results, final_analytics, logs
    
    payload = request.json
    worker_id = payload.get("worker_id")
    task_id = payload.get("task_id")
    result = payload.get("result")
    
    # Store worker's result
    mapreduce_results[worker_id] = result
    
    print(f"[LEADER {SERVER_ID}] Received result from Worker {worker_id}: avg={result.get('avg')}, count={result.get('count')}")
    
    # Aggregate all results we have so far
    aggregate_results()
    
    return jsonify({"status": "ok"})


def aggregate_results():
    """LEADER: Aggregate all collected worker results into final analytics"""
    global mapreduce_results, final_analytics, logs
    
    if not mapreduce_results:
        return
    
    # Aggregate: take average of worker averages, min of worker mins, max of worker maxs
    all_avgs = []
    all_mins = []
    all_maxs = []
    
    for worker_id, result in mapreduce_results.items():
        if result.get("count", 0) > 0:
            all_avgs.append(result.get("avg", 0))
            all_mins.append(result.get("min", 0))
            all_maxs.append(result.get("max", 0))
    
    if all_avgs:
        final_analytics = {
            "avg_temp": round(sum(all_avgs) / len(all_avgs), 2),
            "min_temp": round(min(all_mins), 2),
            "max_temp": round(max(all_maxs), 2)
        }
        
        log_msg = f"\n[LEADER {SERVER_ID}] ═══ MapReduce REDUCE PHASE ═══"
        logs.append(log_msg)
        if len(logs) > 100:
            logs.pop(0)
        print(log_msg)
        
        log_msg = f"[LEADER {SERVER_ID}] Collected {len(mapreduce_results)} worker results"
        logs.append(log_msg)
        if len(logs) > 100:
            logs.pop(0)
        print(log_msg)
        
        for wid, result in mapreduce_results.items():
            log_msg = f"[LEADER {SERVER_ID}]   Worker {wid}: avg={result['avg']}°C (records: {result['count']})"
            logs.append(log_msg)
            if len(logs) > 100:
                logs.pop(0)
            print(log_msg)
        
        log_msg = f"[LEADER {SERVER_ID}] ✓ FINAL RESULT: avg={final_analytics['avg_temp']}°C min={final_analytics['min_temp']}°C max={final_analytics['max_temp']}°C"
        logs.append(log_msg)
        if len(logs) > 100:
            logs.pop(0)
        print(log_msg)
        print("="*80)
        
        # Track average temperature for history chart
        avg_temp_history.append(final_analytics['avg_temp'])
        if len(avg_temp_history) > 50:
            avg_temp_history.pop(0)


def heartbeat():
    global leader_id

    while not shutdown_event.is_set():
        try:
            time.sleep(HEARTBEAT_INTERVAL)

            if leader_id is None or SERVER_ID == leader_id:
                continue

            try:
                requests.get(get_server_url(leader_id) + "/ping", timeout=TIMEOUT)
            except:
                print(f"[SERVER {SERVER_ID}] Leader {leader_id} down. Starting election...")
                start_election()
        except Exception as e:
            print(f"[SERVER {SERVER_ID}] Heartbeat error: {e}")
            continue


@app.route("/node_removed", methods=["POST"])
def node_removed():
    """Notify this node that another node was removed from the cluster"""
    global leader_id, logs
    
    removed_id = request.json.get("removed_id")
    current_leader = request.json.get("current_leader")
    
    log_msg = f"[SERVER {SERVER_ID}] ⚠ Node {removed_id} was removed from cluster"
    logs.append(log_msg)
    if len(logs) > 100:
        logs.pop(0)
    print(log_msg)
    
    # If the removed node was the leader, trigger a new election
    if removed_id == leader_id and SERVER_ID != removed_id:
        print(f"[SERVER {SERVER_ID}] Leader {leader_id} was removed! Starting election...")
        threading.Thread(target=start_election, daemon=True).start()
        leader_id = None
    
    return jsonify({"status": "acknowledged"})


def mapreduce_coordinator():
    """LEADER ONLY: Periodically split data and distribute to workers for parallel processing"""
    global data_store, mapreduce_results, logs
    
    while not shutdown_event.is_set():
        time.sleep(MAPREDUCE_INTERVAL)
        
        if SERVER_ID != leader_id or not data_store:
            continue
        
        # Get current list of worker nodes from cluster.json
        servers = get_servers()
        worker_nodes = [s for s in servers if s["id"] != SERVER_ID]
        
        if not worker_nodes:
            log_msg = f"[LEADER {SERVER_ID}] No workers available for MapReduce"
            logs.append(log_msg)
            if len(logs) > 100:
                logs.pop(0)
            print(log_msg)
            continue
        
        # SPLIT: Divide data into chunks
        chunk_size = len(data_store) // len(worker_nodes)
        if chunk_size == 0:
            chunk_size = 1
        
        # Reset results for this MapReduce cycle (clear stale worker results)
        mapreduce_results = {}
        task_id = int(time.time() * 1000)  # Use millisecond timestamp for uniqueness
        
        log_msg = f"\n[LEADER {SERVER_ID}] ═══ MapReduce SPLIT PHASE (Task {task_id}) ═══"
        logs.append(log_msg)
        if len(logs) > 100:
            logs.pop(0)
        print(log_msg)
        
        log_msg = f"[LEADER {SERVER_ID}] Total records: {len(data_store)} → Dividing into {len(worker_nodes)} chunks"
        logs.append(log_msg)
        if len(logs) > 100:
            logs.pop(0)
        print(log_msg)
        
        # MAP: Send chunks to workers in parallel
        for i, worker in enumerate(worker_nodes):
            start_idx = i * chunk_size
            end_idx = start_idx + chunk_size if i < len(worker_nodes) - 1 else len(data_store)
            chunk = data_store[start_idx:end_idx]
            
            log_msg = f"[LEADER {SERVER_ID}] → Sending {len(chunk)} records to Worker {worker['id']}"
            logs.append(log_msg)
            if len(logs) > 100:
                logs.pop(0)
            print(log_msg)
            
            def send_map_task(worker_url, chunk, task_id, leader_url, worker_id):
                try:
                    resp = requests.post(
                        worker_url + "/map_chunk",
                        json={"chunk": chunk, "task_id": task_id, "leader_url": leader_url},
                        timeout=3
                    )
                    if resp.status_code == 200:
                        print(f"[LEADER {SERVER_ID}] ✓ MAP sent to Worker {worker_id}")
                    else:
                        print(f"[LEADER {SERVER_ID}] ✗ Worker {worker_id} returned status {resp.status_code}")
                except requests.exceptions.ConnectionError:
                    print(f"[LEADER {SERVER_ID}] ✗ Cannot reach Worker {worker_id} (connection failed - may be offline)")
                except requests.exceptions.Timeout:
                    print(f"[LEADER {SERVER_ID}] ✗ Worker {worker_id} timeout - may be offline or overloaded")
                except Exception as e:
                    print(f"[LEADER {SERVER_ID}] ✗ Failed to send MAP to Worker {worker_id}: {e}")
            
            # Send in parallel thread
            threading.Thread(target=send_map_task, args=(worker["url"], chunk, task_id, MY_URL, worker["id"]), daemon=True).start()
        
        # Wait a bit for workers to send results back
        time.sleep(2)
        
        # Check if we got results from at least one worker
        if len(mapreduce_results) == 0:
            log_msg = f"[LEADER {SERVER_ID}] ⚠ No worker results received for task {task_id}"
            logs.append(log_msg)
            if len(logs) > 100:
                logs.pop(0)
            print(log_msg)


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
    global logs, data_store, final_analytics, avg_temp_history
    
    return jsonify({
        "server_id": SERVER_ID,
        "is_leader": SERVER_ID == leader_id,
        "records": len(data_store),
        "data": data_store[-100:],
        "logs": logs[-50:],
        "analytics": final_analytics if SERVER_ID == leader_id else {"avg_temp": 0, "min_temp": 0, "max_temp": 0},
        "mapreduce_results": mapreduce_results if SERVER_ID == leader_id else {},
        "avg_temp_history": avg_temp_history if SERVER_ID == leader_id else []
    })


@app.route("/avg_temp_history", methods=["GET"])
def get_avg_temp_history():
    """Return average temperature history for charting"""
    global avg_temp_history
    if SERVER_ID != leader_id:
        return jsonify([]), 403
    return jsonify(avg_temp_history)


@app.route("/clear_data", methods=["POST"])
def clear_data():
    """LEADER ONLY: Clear stored data when sensors are stopped"""
    global data_store, mapreduce_results, avg_temp_history, logs, final_analytics
    
    if SERVER_ID != leader_id:
        return jsonify({"error": "Not leader"}), 403
    
    data_store = []
    mapreduce_results = {}
    avg_temp_history = []
    final_analytics = {"avg_temp": 0, "min_temp": 0, "max_temp": 0}
    
    log_msg = f"[LEADER {SERVER_ID}] 🔄 Data cleared (sensors stopped)"
    logs.append(log_msg)
    if len(logs) > 100:
        logs.pop(0)
    print(log_msg)
    
    return jsonify({"status": "data cleared"})


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
    print(f"[SERVER {SERVER_ID}] Shutdown requested...")
    shutdown_event.set()
    
    # Remove self from cluster registry
    try:
        remove_from_cluster(SERVER_ID)
        print(f"[SERVER {SERVER_ID}] ✓ Removed from cluster registry")
    except Exception as e:
        print(f"[SERVER {SERVER_ID}] Warning: Failed to remove from cluster: {e}")
    
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

    # Start heartbeat monitor thread
    threading.Thread(target=heartbeat, daemon=True).start()
    
    # Start MapReduce coordinator thread (for leader)
    threading.Thread(target=mapreduce_coordinator, daemon=True).start()
    
    app.run(port=5000 + SERVER_ID)
