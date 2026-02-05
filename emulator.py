import os, sys, time, json, random, traceback
from datetime import datetime

# Force stdout/stderr to be unbuffered for ACI logging
sys.stdout.reconfigure(line_buffering=True)
sys.stderr.reconfigure(line_buffering=True)

print("=== MULTI-DEVICE EMULATOR STARTING ===", flush=True)

try:
    from azure.eventhub import EventHubProducerClient, EventData
    from azure.identity import ManagedIdentityCredential
    print("Imports successful", flush=True)
except Exception as e:
    print(f"Import error: {e}", flush=True)
    traceback.print_exc()
    time.sleep(60)
    sys.exit(1)

# Configuration - Using Managed Identity (no connection strings/secrets needed!)
EVENT_HUB_NAMESPACE = os.getenv('EVENT_HUB_NAMESPACE')  # e.g., masimo-eh-ns
EVENT_HUB_NAME = os.getenv('EVENT_HUB_NAME')
DEVICE_COUNT = int(os.getenv('DEVICE_COUNT', '100'))

print(f"EVENT_HUB_NAMESPACE: {EVENT_HUB_NAMESPACE}", flush=True)
print(f"EVENT_HUB_NAME: {EVENT_HUB_NAME}", flush=True)
print(f"DEVICE_COUNT: {DEVICE_COUNT}", flush=True)

# Generate deterministic device IDs that match FHIR Device resources
DEVICE_IDS = [f"MASIMO-RADIUS7-{i:04d}" for i in range(1, DEVICE_COUNT + 1)]
print(f"Devices: {DEVICE_IDS[0]} to {DEVICE_IDS[-1]}", flush=True)

class MasimoSimulator:
    """Simulates a single Masimo Radius-7 pulse oximeter"""
    def __init__(self, device_id: str):
        self.device_id = device_id
        # Initialize with slightly different baselines per device
        seed = hash(device_id) % 1000
        random.seed(seed)
        self.spo2 = 95.0 + random.uniform(0, 4)
        self.pr = 65.0 + random.uniform(0, 20)
        self.pi = 2.5 + random.uniform(0, 2)
        self.pvi = 10.0 + random.uniform(0, 8)
        random.seed()  # Re-randomize
        
    def generate_reading(self):
        # Simulate realistic vital sign variations
        self.spo2 += random.uniform(-0.5, 0.5)
        self.pr += random.uniform(-2, 2)
        self.pi += random.uniform(-0.1, 0.1)
        self.pvi += random.uniform(-1, 1)
        
        # Clamp to realistic ranges
        self.spo2 = max(88, min(100, self.spo2))
        self.pr = max(50, min(140, self.pr))
        self.pi = max(0.5, min(10, self.pi))
        self.pvi = max(5, min(30, self.pvi))
        
        payload = {
            "device_id": self.device_id,
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "telemetry": {
                "spo2": round(self.spo2, 1),
                "pr": int(self.pr),
                "pi": round(self.pi, 2),
                "pvi": int(self.pvi),
                "sphb": round(12.5 + random.uniform(-1, 1), 1),
                "signal_iq": random.randint(90, 100)
            }
        }
        return payload

def run():
    try:
        print("Connecting to Event Hub using Managed Identity...", flush=True)
        credential = ManagedIdentityCredential()
        
        # Use fully qualified namespace with credential
        fully_qualified_namespace = f"{EVENT_HUB_NAMESPACE}.servicebus.windows.net"
        print(f"FQNS: {fully_qualified_namespace}", flush=True)
        
        print("Creating producer...", flush=True)
        producer = EventHubProducerClient(
            fully_qualified_namespace=fully_qualified_namespace,
            eventhub_name=EVENT_HUB_NAME,
            credential=credential
        )
        
        # Create simulators for all devices
        simulators = {device_id: MasimoSimulator(device_id) for device_id in DEVICE_IDS}
        print(f"Created {len(simulators)} device simulators", flush=True)
        
        print("Entering producer context...", flush=True)
        with producer:
            print("Starting multi-device telemetry loop...", flush=True)
            cycle = 0
            while True:
                # Create a batch with readings from all devices
                batch = producer.create_batch()
                
                for device_id in DEVICE_IDS:
                    sim = simulators[device_id]
                    data = sim.generate_reading()
                    try:
                        batch.add(EventData(json.dumps(data)))
                    except ValueError:
                        # Batch is full, send it and create a new one
                        producer.send_batch(batch)
                        batch = producer.create_batch()
                        batch.add(EventData(json.dumps(data)))
                
                producer.send_batch(batch)
                cycle += 1
                
                # Log progress every 10 cycles
                if cycle % 10 == 0:
                    print(f"Cycle {cycle}: Sent telemetry for {len(DEVICE_IDS)} devices", flush=True)
                
                # Wait 1 second between cycles (all devices report every second)
                time.sleep(1)
                
    except Exception as e:
        print(f"!!! Fatal Error: {e}", flush=True)
        traceback.print_exc()
        sys.stdout.flush()
        sys.stderr.flush()
        print("Sleeping 120s for log capture...", flush=True)
        time.sleep(120)
        exit(1)

if __name__ == "__main__":
    run()
