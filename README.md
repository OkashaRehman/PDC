# PDC IoT Cluster

A distributed IoT cluster management system built with Flask that implements leader election, data replication, and real-time monitoring of sensors.

## Project Overview

This project demonstrates a distributed computing system with the following features:

- **Distributed Servers**: Multiple Flask-based servers running in parallel with automatic leader election
- **Data Replication**: Data from sensors is replicated across cluster nodes
- **Heartbeat Monitoring**: Servers monitor each other's health with periodic heartbeat checks
- **Web Dashboard**: Real-time web interface to monitor cluster status and sensor data
- **Sensor Integration**: IoT sensors that generate and transmit temperature and traffic data
- **Cluster Management**: Dynamic server registration and cluster coordination

## Project Structure

```
iot_project/
├── server.py              # Flask server with leader election and data replication
├── dashboard.py           # Web dashboard for cluster monitoring
├── sensor.py              # IoT sensor data generation and transmission
├── cluster_manager.py     # Cluster coordination and server management
├── config.py              # Configuration settings
└── cluster.json           # Cluster node definitions
```

## Requirements

- Python 3.7+
- Flask
- Flask-CORS
- Requests

## Installation & Setup

### 1. Create a Virtual Environment

```bash
python -m venv .venv
```

### 2. Activate Virtual Environment

**Windows (PowerShell):**
```powershell
.\.venv\Scripts\Activate.ps1
```

**Windows (Command Prompt):**
```cmd
.venv\Scripts\activate.bat
```

**Linux/macOS:**
```bash
source .venv/bin/activate
```

### 3. Install Dependencies

```bash
pip install flask flask-cors requests
```

## Running the Project

### Start the Dashboard (Web UI)

```bash
cd iot_project
python dashboard.py
```

The dashboard will be available at `http://localhost:5000` in your web browser.

### Start Cluster Servers

In separate terminal windows, start individual servers with unique IDs:

```bash
# Terminal 1 - Start Server 1
cd iot_project
python server.py 1

# Terminal 2 - Start Server 2
cd iot_project
python server.py 2

# Terminal 3 - Start Server 3
cd iot_project
python server.py 3
```

Each server runs on port `5000 + server_id` (e.g., Server 1 on port 5001, Server 2 on port 5002, etc.)

### Start Sensors

In additional terminal windows, start sensor processes to generate data:

```bash
# Terminal 4 - Start Sensor 1
cd iot_project
python sensor.py 1

# Terminal 5 - Start Sensor 2
cd iot_project
python sensor.py 2
```

## How It Works

1. **Cluster Initialization**: When servers start, they register themselves with the cluster manager
2. **Leader Election**: Servers perform heartbeat checks and elect a leader
3. **Data Collection**: Sensors generate temperature and traffic data, finding the leader server
4. **Data Replication**: The leader receives data and replicates it to follower servers
5. **Monitoring**: The dashboard displays real-time cluster status, server health, and sensor data
6. **Failover**: If the leader goes down, a new leader is automatically elected from remaining servers

## API Endpoints

### Server Endpoints

- `POST /data` - Submit sensor data to the leader
- `GET /status` - Get server status and leader information
- `GET /logs` - Retrieve server logs

### Dashboard Endpoints

- `GET /` - Main dashboard interface
- `GET /api/servers` - Get cluster server information
- `GET /api/data` - Get stored data
- `POST /api/spawn-sensor` - Start a new sensor process
- `POST /api/spawn-server` - Start a new server process

## Configuration

Edit `config.py` to modify:

- `HEARTBEAT_INTERVAL`: How often servers check each other's health (default: 3 seconds)
- `TIMEOUT`: How long to wait for a server response (default: 2 seconds)
- `SERVERS`: Dictionary of registered servers (populated from cluster.json)

## Dashboard Features

- **Cluster Overview**: View all servers and their status
- **Server Health**: Monitor heartbeat and connectivity
- **Data Visualization**: Charts showing sensor readings over time
- **Log Viewer**: Real-time log output from servers
- **Server Management**: Spawn new servers and sensors from the dashboard
- **System Metrics**: Monitor replication lag and data store size

## Troubleshooting

**No servers appear in the dashboard:**
- Ensure servers are running before accessing the dashboard
- Check that server IDs are unique
- Verify no firewall blocking localhost connections

**Sensors can't find the leader:**
- Ensure at least one server is running
- Check that servers have completed the leader election process
- Verify cluster.json is properly configured

**Data not replicating:**
- Ensure multiple servers are running
- Check server logs for replication errors
- Verify network connectivity between servers

## Development Notes

- The project uses in-memory data stores (not persistent)
- Leader election uses a simple heartbeat algorithm
- Suitable for educational and development purposes
- For production use, consider adding persistent storage and more robust consensus algorithms

## License

MIT

