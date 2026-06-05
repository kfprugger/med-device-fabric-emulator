import logging
import sys
sys.path.append("/Users/joey/git/med-device-fabric-emulator/orchestrator")
from shared.fabric_client import FabricClient

logging.basicConfig(level=logging.INFO)

fabric = FabricClient()
try:
    print("Calling GET /capacities...")
    caps = fabric.call("GET", "/capacities")
    print("Success!")
    print(caps)
except Exception as e:
    print(f"Failed: {e}")
