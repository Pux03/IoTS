import subprocess
import time
import urllib.request
import json
import os
import sys

INGESTION_URL = "http://localhost:8000"
STORAGE_URL = "http://localhost:8001"
ANALYTICS_URL = "http://localhost:8002"

def run_cmd(cmd, env=None):
    """Runs a system command and returns stdout."""
    full_env = os.environ.copy()
    if env:
        full_env.update(env)
    
    print(f"Running command: {' '.join(cmd)}")
    result = subprocess.run(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=full_env,
        shell=True # Shell=True is needed on Windows for docker-compose / docker
    )
    if result.returncode != 0:
        print(f"Command failed with code {result.returncode}")
        print(f"Error output:\n{result.stderr}")
    return result.stdout

def api_request(url, method="GET", data=None):
    """Helper to perform HTTP requests to microservices."""
    req = urllib.request.Request(url, method=method)
    if data:
        req.add_header('Content-Type', 'application/json')
        json_data = json.dumps(data).encode('utf-8')
    else:
        json_data = None
        
    try:
        with urllib.request.urlopen(req, data=json_data, timeout=20) as response:
            return response.read().decode('utf-8')
    except Exception as e:
        print(f"API request failed to {url}: {e}")
        return None

def parse_prometheus_metrics(metrics_text, metric_name):
    if not metrics_text:
        return 0.0
    total = 0.0
    found = False
    for line in metrics_text.splitlines():
        if line.startswith("#") or not line.strip():
            continue
        if line.startswith(metric_name):
            parts = line.split()
            if len(parts) >= 2:
                try:
                    total += float(parts[-1])
                    found = True
                except ValueError:
                    pass
    return total if found else 0.0

def restart_containers(broker_type, config_val, disable_db=False):
    """Recreates and starts containers with specific environment configurations."""
    print("\n" + "="*50)
    print(f"Reconfiguring environment for: {broker_type.upper()} ({config_val})")
    print("="*50)
    
    # Set env vars for docker-compose
    env_vars = {
        "BROKER_TYPE": broker_type,
        "DISABLE_DB_WRITE": "true" if disable_db else "false"
    }
    
    if broker_type == "mqtt":
        env_vars["MQTT_QOS"] = str(config_val)
    else:
        env_vars["KAFKA_ACKS"] = str(config_val)
        
    # Recreate the containers
    run_cmd(["docker-compose", "down"], env=env_vars)
    time.sleep(2)
    run_cmd(["docker-compose", "up", "-d", "--build"], env=env_vars)
    
    # Wait for services to be healthy
    print("Waiting for microservices to wake up...")
    for i in range(15):
        time.sleep(2)
        h1 = api_request(f"{INGESTION_URL}/health")
        h2 = api_request(f"{STORAGE_URL}/health")
        h3 = api_request(f"{ANALYTICS_URL}/health")
        if h1 and h2 and h3:
            print("All services are healthy!")
            time.sleep(2) # Extra buffer for connection establishment
            return True
    print("Warning: Services did not become healthy in time.")
    return False

def get_current_metrics():
    """Fetches metrics from all services."""
    m_ingest = api_request(f"{INGESTION_URL}/metrics")
    m_storage = api_request(f"{STORAGE_URL}/metrics")
    m_analytics = api_request(f"{ANALYTICS_URL}/metrics")
    return m_ingest, m_storage, m_analytics

def run_scenario_a(devices, interval, duration_sec):
    """Runs Scenario A and measures throughput/loss."""
    print(f"--- Starting Scenario A: {devices} devices (interval: {interval}s) for {duration_sec}s ---")
    
    # Reset / get base metrics
    m1_base, m2_base, _ = get_current_metrics()
    sent_base = parse_prometheus_metrics(m1_base, "ingestion_messages_sent_total")
    stored_base = parse_prometheus_metrics(m2_base, "storage_messages_received_total")
    
    # Start simulation
    api_request(f"{INGESTION_URL}/scenario/a/start?devices={devices}&interval={interval}", method="POST")
    time.sleep(duration_sec)
    # Stop simulation
    api_request(f"{INGESTION_URL}/scenario/a/stop", method="POST")
    
    # Wait a moment for any outstanding messages to flush
    time.sleep(3)
    
    m1_end, m2_end, _ = get_current_metrics()
    sent_end = parse_prometheus_metrics(m1_end, "ingestion_messages_sent_total")
    stored_end = parse_prometheus_metrics(m2_end, "storage_messages_received_total")
    
    sent_diff = sent_end - sent_base
    stored_diff = stored_end - stored_base
    
    throughput = sent_diff / duration_sec
    loss = max(0.0, sent_diff - stored_diff)
    loss_pct = (loss / sent_diff * 100) if sent_diff > 0 else 0.0
    
    print(f"Result: Sent={sent_diff:.0f}, Stored={stored_diff:.0f}, Throughput={throughput:.2f} msg/s, Loss={loss_pct:.2f}%")
    return {
        "sent": sent_diff,
        "stored": stored_diff,
        "throughput": throughput,
        "loss_pct": loss_pct
    }

def run_scenario_b():
    """Runs Scenario B: network disconnect for 30 seconds."""
    print("--- Starting Scenario B: Network disconnect (30s outage) ---")
    
    # Start 100 devices publishing every 0.5s (200 msg/s)
    api_request(f"{INGESTION_URL}/scenario/a/start?devices=100&interval=0.5", method="POST")
    time.sleep(5)
    
    # Disconnect Data Ingestion container
    print("Disconnecting data-ingestion from network...")
    run_cmd(["docker", "network", "disconnect", "iot_network", "data-ingestion"])
    
    print("Simulating 30s outage...")
    time.sleep(30)
    
    # Reconnect container
    print("Reconnecting data-ingestion to network...")
    run_cmd(["docker", "network", "connect", "iot_network", "data-ingestion"])
    
    # Let it recover and run for 15s
    print("Waiting for recovery (15s)...")
    time.sleep(15)
    
    # Stop simulation
    api_request(f"{INGESTION_URL}/scenario/a/stop", method="POST")
    
    # Check status
    m1, m2, _ = get_current_metrics()
    sent = parse_prometheus_metrics(m1, "ingestion_messages_sent_total")
    stored = parse_prometheus_metrics(m2, "storage_messages_received_total")
    
    print(f"Scenario B completed. Ingestion total sent: {sent:.0f}, Storage total received: {stored:.0f}")
    return {"sent_total": sent, "stored_total": stored}

def run_scenario_c():
    """Runs Scenario C: sudden burst load."""
    print("--- Starting Scenario C: Burst Event Load (1000 msg/s for 5s) ---")
    # Clean up base storage
    _, m2_base, _ = get_current_metrics()
    stored_base = parse_prometheus_metrics(m2_base, "storage_messages_received_total")
    
    # Trigger burst
    api_request(f"{INGESTION_URL}/scenario/c/trigger?rate=1000&duration=5", method="POST")
    
    # Wait and track recovery time
    start_time = time.time()
    recovered = False
    recovery_time = 0.0
    
    for i in range(15):
        time.sleep(1)
        _, m2, _ = get_current_metrics()
        current_stored = parse_prometheus_metrics(m2, "storage_messages_received_total") - stored_base
        print(f"Elapsed: {i+1}s, Stored burst events: {current_stored:.0f}/5000")
        
        if current_stored >= 4900 and not recovered:
            recovery_time = time.time() - start_time
            recovered = True
            
    if not recovered:
        recovery_time = 15.0 # Max wait
        
    print(f"Scenario C completed. Recovery time: {recovery_time:.2f}s")
    return {"recovery_time": recovery_time}

def run_scenario_d():
    """Runs Scenario D: measures alert latency."""
    print("--- Starting Scenario D: Real-Time Alerting Latency ---")
    
    # Trigger critical alerts
    api_request(f"{INGESTION_URL}/scenario/d/trigger?count=20", method="POST")
    
    # Wait for tumbling window (10s) and a bit more
    print("Waiting for analytics tumbling window to process alerts...")
    time.sleep(12)
    
    # Read alert latency
    _, _, m3 = get_current_metrics()
    latency = parse_prometheus_metrics(m3, "analytics_e2e_latency_ms")
    alerts = parse_prometheus_metrics(m3, "analytics_alerts_total")
    
    print(f"Scenario D completed. Latest Alert Latency: {latency:.2f} ms. Alerts registered: {alerts:.0f}")
    return {"latency_ms": latency, "alerts_total": alerts}

def main():
    # Proširena lista konfiguracija koja uključuje kompletan opseg zahteva:
    # MQTT: QoS 0, QoS 1, QoS 2
    # Kafka: Acks 0, Acks 1, Acks all
    test_configs = [
        ("mqtt", "0"),
        ("mqtt", "1"),
        ("mqtt", "2"),
        ("kafka", "0"),
        ("kafka", "1"),
        ("kafka", "all")
    ]
    
    results = {}
    
    for broker, conf_val in test_configs:
        config_name = f"{broker}_conf_{conf_val}"
        
        # Recreate docker setup
        success = restart_containers(broker, conf_val)
        if not success:
            print(f"Skipping {config_name} due to setup failure.")
            continue
            
        print(f"Running benchmarks for configuration: {config_name}")
        
        # Izvršavanje Scenarija A za sve tri tražene grupe uređaja (100, 1000, 10000)
        sec_a_100 = run_scenario_a(devices=100, interval=1.0, duration_sec=10)
        sec_a_1000 = run_scenario_a(devices=1000, interval=1.0, duration_sec=10)
        sec_a_10000 = run_scenario_a(devices=10000, interval=1.0, duration_sec=10)
        
        # Run Scenario B
        sec_b = run_scenario_b()
        
        # Run Scenario C
        sec_c = run_scenario_c()
        
        # Run Scenario D
        sec_d = run_scenario_d()
        
        results[config_name] = {
            "scenario_a_100": sec_a_100,
            "scenario_a_1000": sec_a_1000,
            "scenario_a_10000": sec_a_10000,
            "scenario_b": sec_b,
            "scenario_c": sec_c,
            "scenario_d": sec_d
        }
        
    # Write results to file
    os.makedirs("benchmarks", exist_ok=True)
    with open("benchmarks/results.json", "w") as f:
        json.dump(results, f, indent=4)
        
    print("\n" + "="*50)
    print("BENCHMARK COMPLETED. RESULTS SAVED TO benchmarks/results.json")
    print("="*50)
    
    # Generisanje proširenog zbirnog izveštaja na konzoli (Prisutan i pregled za 1k throughput)
    print("\nComparative Summary:")
    print(f"{'Config':<20} | {'Throughput 1k (msg/s)':<22} | {'Loss 1k (%)':<12} | {'Burst Recovery (s)':<18} | {'Alert Latency (ms)':<18}")
    print("-"*98)
    for name, data in results.items():
        tp = data["scenario_a_1000"]["throughput"]
        loss = data["scenario_a_1000"]["loss_pct"]
        rec = data["scenario_c"]["recovery_time"]
        lat = data["scenario_d"]["latency_ms"]
        print(f"{name:<20} | {tp:<22.2f} | {loss:<12.2f} | {rec:<18.2f} | {lat:<18.2f}")

if __name__ == "__main__":
    main()