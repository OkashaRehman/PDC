from flask import Flask, render_template_string, jsonify, request
from flask_cors import CORS
import subprocess
import requests

from cluster_manager import get_servers, register_server, remove_server as remove_from_cluster

app = Flask(__name__)
CORS(app)

sensor_processes = []
server_processes = {}  # dict of {server_id: subprocess.Popen}
next_server_id = 1

HTML = """
<!DOCTYPE html>
<html>
<head>
<title>PDC IoT Cluster</title>
<link href="https://fonts.googleapis.com/css2?family=Share+Tech+Mono&family=Exo+2:wght@300;600;800&display=swap" rel="stylesheet">
<script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
<style>
:root {
    --bg:#050d1a; --panel:#0a1628; --border:#1a3a5c;
    --accent:#00e5ff; --accent2:#ff6b35; --accent3:#a259ff;
    --green:#00ff88; --red:#ff3b5c; --text:#c9e0f5; --muted:#4a7fa5;
    --mono:'Share Tech Mono',monospace; --sans:'Exo 2',sans-serif;
}
*{box-sizing:border-box;margin:0;padding:0;}
body{background:var(--bg);color:var(--text);font-family:var(--sans);min-height:100vh;overflow-x:hidden;}
body::before{content:'';position:fixed;inset:0;background-image:linear-gradient(rgba(0,229,255,0.03) 1px,transparent 1px),linear-gradient(90deg,rgba(0,229,255,0.03) 1px,transparent 1px);background-size:40px 40px;pointer-events:none;z-index:0;}
.everything{position:relative;z-index:1;}
nav{background:linear-gradient(90deg,#050d1a 0%,#0a1e35 50%,#050d1a 100%);border-bottom:1px solid var(--border);padding:14px 28px;display:flex;align-items:center;gap:16px;}
nav .logo{font-family:var(--mono);font-size:11px;color:var(--accent);letter-spacing:3px;text-transform:uppercase;opacity:0.7;}
nav h1{font-size:20px;font-weight:800;letter-spacing:1px;color:#fff;}
nav h1 span{color:var(--accent);}
.nav-right{margin-left:auto;font-family:var(--mono);font-size:12px;color:var(--muted);display:flex;align-items:center;gap:8px;}
.pulse-dot{width:8px;height:8px;border-radius:50%;background:var(--green);animation:pulse 1.5s infinite;}
@keyframes pulse{0%,100%{opacity:1;transform:scale(1);}50%{opacity:0.4;transform:scale(0.8);}}
.page{padding:20px 24px;}
.top-row{display:grid;grid-template-columns:repeat(5,1fr);gap:16px;margin-bottom:16px;}
.mid-row{display:grid;grid-template-columns:1fr 1fr 1.4fr;gap:16px;margin-bottom:16px;}
.bot-row{display:grid;grid-template-columns:1.5fr 1fr;gap:16px;}
.card{background:var(--panel);border:1px solid var(--border);border-radius:12px;padding:18px 20px;position:relative;overflow:hidden;}
.card::before{content:'';position:absolute;top:0;left:0;right:0;height:2px;background:linear-gradient(90deg,transparent,var(--accent),transparent);opacity:0.5;}
.card.accent2::before{background:linear-gradient(90deg,transparent,var(--accent2),transparent);}
.card.accent3::before{background:linear-gradient(90deg,transparent,var(--accent3),transparent);}
.card.green-top::before{background:linear-gradient(90deg,transparent,var(--green),transparent);}
.card-label{font-family:var(--mono);font-size:10px;letter-spacing:2px;color:var(--muted);text-transform:uppercase;margin-bottom:8px;}
.card-value{font-size:36px;font-weight:800;color:var(--accent);line-height:1;}
.card-value.green{color:var(--green);}
.card-value.orange{color:var(--accent2);}
.card-value.purple{color:var(--accent3);}
.card-sub{font-family:var(--mono);font-size:11px;color:var(--muted);margin-top:4px;}
.sec-title{font-family:var(--mono);font-size:11px;letter-spacing:2px;color:var(--accent);text-transform:uppercase;margin-bottom:14px;display:flex;align-items:center;gap:8px;}
.sec-title::after{content:'';flex:1;height:1px;background:var(--border);}
.btn-row{display:flex;flex-wrap:wrap;gap:8px;margin-top:10px;}
button{font-family:var(--mono);font-size:11px;letter-spacing:1px;padding:8px 14px;border:1px solid;border-radius:6px;cursor:pointer;background:transparent;text-transform:uppercase;transition:all 0.2s;}
button:hover{filter:brightness(1.3);}
.btn-green{color:var(--green);border-color:var(--green);}
.btn-red{color:var(--red);border-color:var(--red);}
.btn-blue{color:var(--accent);border-color:var(--accent);}
.btn-orange{color:var(--accent2);border-color:var(--accent2);}
button:active{transform:scale(0.97);}
.nodes-grid{display:flex;flex-wrap:wrap;gap:10px;}
.node{background:#0d1f36;border:1px solid var(--border);border-radius:10px;padding:12px 16px;min-width:130px;position:relative;transition:border-color 0.3s;}
.node.leader{border-color:var(--accent);background:#071e33;box-shadow:0 0 18px rgba(0,229,255,0.15);}
.node-id{font-size:15px;font-weight:800;color:#fff;}
.node-badge{font-family:var(--mono);font-size:9px;letter-spacing:1px;color:var(--accent);background:rgba(0,229,255,0.1);border:1px solid rgba(0,229,255,0.3);border-radius:4px;padding:1px 6px;display:inline-block;margin-top:4px;}
.node-records{font-family:var(--mono);font-size:11px;color:var(--muted);margin-top:6px;}
.node-dot{width:8px;height:8px;border-radius:50%;background:var(--green);position:absolute;top:12px;right:12px;}
.mr-pipeline{display:flex;align-items:center;gap:8px;margin:10px 0;flex-wrap:wrap;}
.mr-stage{background:#0d1f36;border:1px solid var(--border);border-radius:8px;padding:8px 12px;font-family:var(--mono);font-size:11px;text-align:center;min-width:70px;}
.mr-stage .stage-name{color:var(--accent3);font-size:9px;letter-spacing:1px;text-transform:uppercase;}
.mr-stage .stage-val{color:#fff;font-size:14px;font-weight:bold;margin-top:2px;}
.mr-arrow{color:var(--muted);font-size:16px;}
.traffic-bar{margin-bottom:10px;}
.traffic-label{display:flex;justify-content:space-between;font-family:var(--mono);font-size:11px;color:var(--muted);margin-bottom:4px;}
.bar-track{background:#0d1f36;border-radius:4px;height:8px;overflow:hidden;}
.bar-fill{height:100%;border-radius:4px;transition:width 0.5s ease;}
.bar-low{background:var(--green);}
.bar-medium{background:var(--accent2);}
.bar-high{background:var(--red);}
.health-row{display:flex;align-items:center;padding:7px 10px;border-radius:6px;margin-bottom:6px;background:#0d1f36;font-family:var(--mono);font-size:12px;gap:10px;}
.health-status{width:8px;height:8px;border-radius:50%;flex-shrink:0;}
.alive{background:var(--green);box-shadow:0 0 6px var(--green);}
.dead{background:var(--red);box-shadow:0 0 6px var(--red);}
.log-box{background:#020a14;border:1px solid var(--border);border-radius:8px;height:200px;overflow-y:auto;padding:10px 14px;font-family:var(--mono);font-size:11px;color:#4aff8c;line-height:1.7;}
.log-box::-webkit-scrollbar{width:4px;}
.log-box::-webkit-scrollbar-thumb{background:var(--border);border-radius:2px;}
#toast{position:fixed;bottom:28px;right:28px;background:var(--panel);border:1px solid var(--accent);color:var(--accent);font-family:var(--mono);font-size:12px;padding:12px 20px;border-radius:8px;opacity:0;transform:translateY(10px);transition:all 0.3s;z-index:999;}
#toast.show{opacity:1;transform:translateY(0);}
</style>
</head>
<body>
<div class="everything">
<nav>
    <div class="logo">PDC //</div>
    <h1>Fault-Tolerant IoT <span>Cluster</span></h1>
    <div class="nav-right"><div class="pulse-dot"></div>LIVE</div>
</nav>
<div class="page">
    <div class="top-row">
        <div class="card">
            <div class="card-label">Leader Node</div>
            <div class="card-value" id="leader">—</div>
            <div class="card-sub">Bully Algorithm</div>
        </div>
        <div class="card accent2">
            <div class="card-label">Cluster Size</div>
            <div class="card-value orange" id="clusterSize">0</div>
            <div class="card-sub">Active Nodes</div>
        </div>
        <div class="card green-top">
            <div class="card-label">Total Records</div>
            <div class="card-value green" id="records">0</div>
            <div class="card-sub">Leader Store</div>
        </div>
        <div class="card accent3">
            <div class="card-label">Avg Temperature</div>
            <div class="card-value purple" id="avgTemp">—</div>
            <div class="card-sub">MapReduce Result</div>
        </div>
        <div class="card">
            <div class="card-label">Active Sensors</div>
            <div class="card-value" id="activeSensors">—</div>
            <div class="card-sub">Unique Sensor IDs</div>
        </div>
    </div>
    <div class="mid-row">
        <div class="card accent3">
            <div class="sec-title">MapReduce Pipeline (Leader)</div>
            <div class="mr-pipeline">
                <div class="mr-stage"><div class="stage-name">INPUT</div><div class="stage-val" id="mrInput">0</div></div>
                <div class="mr-arrow">→</div>
                <div class="mr-stage"><div class="stage-name">SPLIT</div><div class="stage-val">4</div></div>
                <div class="mr-arrow">→</div>
                <div class="mr-stage"><div class="stage-name">MAP W1</div><div class="stage-val" id="w1">…</div></div>
                <div class="mr-stage"><div class="stage-name">MAP W2</div><div class="stage-val" id="w2">…</div></div>
                <div class="mr-stage"><div class="stage-name">MAP W3</div><div class="stage-val" id="w3">…</div></div>
                <div class="mr-stage"><div class="stage-name">MAP W4</div><div class="stage-val" id="w4">…</div></div>
                <div class="mr-arrow">→</div>
                <div class="mr-stage"><div class="stage-name">REDUCE</div><div class="stage-val" id="mrReduce">—</div></div>
            </div>
            <div style="margin-top:12px;">
                <div class="card-label">Temperature Range</div>
                <div style="display:flex;gap:20px;margin-top:6px;">
                    <div><div class="card-sub">MIN</div><div style="font-size:22px;font-weight:800;color:var(--accent)" id="minTemp">—</div></div>
                    <div><div class="card-sub">MAX</div><div style="font-size:22px;font-weight:800;color:var(--accent2)" id="maxTemp">—</div></div>
                </div>
            </div>
        </div>
        <div class="card accent2">
            <div class="sec-title">Traffic Distribution</div>
            <div class="traffic-bar">
                <div class="traffic-label"><span>LOW</span><span id="trafficLow">0</span></div>
                <div class="bar-track"><div class="bar-fill bar-low" id="barLow" style="width:0%"></div></div>
            </div>
            <div class="traffic-bar">
                <div class="traffic-label"><span>MEDIUM</span><span id="trafficMed">0</span></div>
                <div class="bar-track"><div class="bar-fill bar-medium" id="barMed" style="width:0%"></div></div>
            </div>
            <div class="traffic-bar">
                <div class="traffic-label"><span>HIGH</span><span id="trafficHigh">0</span></div>
                <div class="bar-track"><div class="bar-fill bar-high" id="barHigh" style="width:0%"></div></div>
            </div>
        </div>
        <div class="card">
            <div class="sec-title">Cluster Controls</div>
            <div class="card-label" style="margin-bottom:6px;">Sensors (20 workers)</div>
            <div class="btn-row">
                <button class="btn-green" onclick="startSensors()">▶ Start Sensors</button>
                <button class="btn-red" onclick="stopSensors()">■ Stop Sensors</button>
            </div>
            <div class="card-label" style="margin-top:14px;margin-bottom:6px;">Node Management</div>
            <div class="btn-row">
                <button class="btn-blue" onclick="addServer()">+ Add Node</button>
                <button class="btn-orange" onclick="removeServer()">− Remove Node</button>
            </div>
            <div class="card-label" style="margin-top:14px;margin-bottom:6px;">Fault Tolerance</div>
            <div style="font-family:var(--mono);font-size:11px;color:var(--muted);line-height:1.8;">
                • Bully election on leader failure<br>
                • Parallel heartbeat monitoring<br>
                • Concurrent data replication<br>
                • MapReduce every 5 seconds
            </div>
        </div>
    </div>
    <div class="bot-row">
        <div class="card">
            <div class="sec-title">Live Node Status</div>
            <div class="nodes-grid" id="nodesGrid">
                <div style="color:var(--muted);font-family:var(--mono);font-size:12px;">Waiting for servers…</div>
            </div>
            <div class="sec-title" style="margin-top:20px;">Parallel Health Check (Leader → Followers)</div>
            <div id="healthList"><div style="color:var(--muted);font-family:var(--mono);font-size:12px;">—</div></div>
        </div>
        <div style="display:flex;flex-direction:column;gap:16px;">
            <div class="card">
                <div class="sec-title">Temperature Feed</div>
                <canvas id="tempChart" height="130"></canvas>
            </div>
            <div class="card">
                <div class="sec-title">Leader Log</div>
                <div class="log-box" id="logBox"></div>
            </div>
        </div>
    </div>
</div>
</div>
<div id="toast"></div>
<script>
let chart;
let tempHistory = [];
const MAX_HISTORY = 40;

function toast(msg){const t=document.getElementById('toast');t.textContent=msg;t.classList.add('show');setTimeout(()=>t.classList.remove('show'),2500);}
function setEl(id,val){const el=document.getElementById(id);if(el)el.textContent=val;}

function updateAnalytics(analytics,records){
    if(!analytics)return;
    setEl('avgTemp',analytics.avg_temp?analytics.avg_temp+'°C':'—');
    setEl('minTemp',analytics.min_temp?analytics.min_temp+'°C':'—');
    setEl('maxTemp',analytics.max_temp?analytics.max_temp+'°C':'—');
    setEl('activeSensors',analytics.active_sensors||'—');
    setEl('mrInput',records||0);
    setEl('mrReduce',analytics.avg_temp?analytics.avg_temp+'°C':'—');
    const total=records||0,chunk=Math.floor(total/4);
    setEl('w1',chunk);setEl('w2',chunk);setEl('w3',chunk);setEl('w4',total-chunk*3);
    const tc=analytics.traffic_counts||{};
    const lo=tc.low||0,me=tc.medium||0,hi=tc.high||0,tot=lo+me+hi||1;
    setEl('trafficLow',lo);setEl('trafficMed',me);setEl('trafficHigh',hi);
    document.getElementById('barLow').style.width=(lo/tot*100)+'%';
    document.getElementById('barMed').style.width=(me/tot*100)+'%';
    document.getElementById('barHigh').style.width=(hi/tot*100)+'%';
}

function updateChart(data){
    const temps=data.map(d=>d.temperature);
    if(temps.length>0){tempHistory.push(...temps);if(tempHistory.length>MAX_HISTORY)tempHistory=tempHistory.slice(-MAX_HISTORY);}
    const histLabels=tempHistory.map((_,i)=>i+1);
    if(chart)chart.destroy();
    chart=new Chart(document.getElementById('tempChart'),{
        type:'line',
        data:{labels:histLabels,datasets:[{label:'Temperature (°C)',data:tempHistory,borderColor:'#00e5ff',backgroundColor:'rgba(0,229,255,0.08)',borderWidth:2,pointRadius:0,tension:0.4,fill:true}]},
        options:{animation:false,plugins:{legend:{display:false}},scales:{x:{display:false},y:{ticks:{color:'#4a7fa5',font:{family:'Share Tech Mono',size:10}},grid:{color:'rgba(26,58,92,0.5)'}}}}
    });
}

function updateLogs(logs){
    const box=document.getElementById('logBox');
    box.innerHTML=[...logs].reverse().map(l=>`<div>${l}</div>`).join('');
    box.scrollTop=0;
}

async function fetchHealth(leaderUrl){
    try{
        const r=await fetch(leaderUrl+'/health');
        const data=await r.json();
        const list=document.getElementById('healthList');
        list.innerHTML='';
        for(const[sid,alive]of Object.entries(data)){
            list.innerHTML+=`<div class="health-row"><div class="health-status ${alive?'alive':'dead'}"></div><span>Node ${sid}</span><span style="margin-left:auto;color:${alive?'var(--green)':'var(--red)'}">${alive?'ALIVE':'DEAD'}</span></div>`;
        }
        if(Object.keys(data).length===0)list.innerHTML='<div style="color:var(--muted);font-family:var(--mono);font-size:12px;">No followers</div>';
    }catch{document.getElementById('healthList').innerHTML='<div style="color:var(--muted);font-family:var(--mono);font-size:12px;">—</div>';}
}

async function fetchData(){
    try{
        const res=await fetch('/cluster_status');
        const cluster=await res.json();
        setEl('clusterSize',cluster.length);
        const grid=document.getElementById('nodesGrid');
        grid.innerHTML='';
        let leaderFound=false;
        for(const s of cluster){
            try{
                const r=await fetch(s.url+'/status');
                const data=await r.json();
                const isLeader=data.is_leader;
                grid.innerHTML+=`<div class="node ${isLeader?'leader':''}"><div class="node-dot"></div><div class="node-id">Node ${data.server_id}</div>${isLeader?'<div class="node-badge">LEADER</div>':''}<div class="node-records">${data.records} records</div><button class="btn-red" style="margin-top:8px;width:100%;padding:6px 0;font-size:10px;" onclick="removeNode(${data.server_id})">Remove</button></div>`;
                if(isLeader){
                    leaderFound=true;
                    setEl('leader','Node '+data.server_id);
                    setEl('records',data.records);
                    updateChart(data.data);
                    updateLogs(data.logs);
                    updateAnalytics(data.analytics,data.records);
                    await fetchHealth(s.url);
                }
            }catch{}
        }
        if(!leaderFound)setEl('leader','NONE');
    }catch{}
}

async function startSensors(){await fetch('/start_sensors');toast('Sensors started');}
async function stopSensors(){await fetch('/stop_sensors');toast('Sensors stopped');}
async function addServer(){await fetch('/add_server');toast('New node added to cluster');}
async function removeServer(){await fetch('/remove_server');toast('Node removed');}
async function removeNode(serverId){await fetch(`/remove_server?id=${serverId}`);toast(`Node ${serverId} removed`);}

setInterval(fetchData,2000);
fetchData();
</script>
</body>
</html>
"""

@app.route("/")
def home():
    return render_template_string(HTML)

@app.route("/cluster_status")
def cluster_status():
    return jsonify(get_servers())

@app.route("/start_sensors")
def start_sensors():
    global sensor_processes
    if len(sensor_processes) == 0:
        for i in range(1, 21):
            p = subprocess.Popen(["python", "sensor.py", str(i)])
            sensor_processes.append(p)
    return jsonify({"status": "started"})

@app.route("/stop_sensors")
def stop_sensors():
    global sensor_processes
    for p in sensor_processes:
        p.kill()
    sensor_processes = []
    return jsonify({"status": "stopped"})

@app.route("/add_server")
def add_server():
    global next_server_id, server_processes
    sid = next_server_id
    p = subprocess.Popen(["python", "server.py", str(sid)])
    server_processes[sid] = p
    next_server_id += 1
    return jsonify({"status": "server added", "id": sid})

@app.route("/remove_server")
def remove_server():
    global server_processes
    server_id = request.args.get('id', type=int)
    
    if server_id and server_id in server_processes:
        p = server_processes.pop(server_id)
        try:
            requests.get(f"http://127.0.0.1:{5000 + server_id}/shutdown", timeout=1)
        except Exception:
            pass
        import time as _t
        _t.sleep(0.4)
        p.kill()
        remove_from_cluster(server_id)
    elif len(server_processes) > 0:
        # Remove the last added server if no id specified
        server_id = max(server_processes.keys())
        p = server_processes.pop(server_id)
        try:
            requests.get(f"http://127.0.0.1:{5000 + server_id}/shutdown", timeout=1)
        except Exception:
            pass
        import time as _t
        _t.sleep(0.4)
        p.kill()
        remove_from_cluster(server_id)
    
    return jsonify({"status": "removed"})

if __name__ == "__main__":
    import json
    # Reset cluster and server counter on startup
    with open("cluster.json", "w") as f:
        json.dump([], f)
    next_server_id = 1
    app.run(port=8000)