import json
import os
import random
import sys
import time
import traceback
import uuid
from datetime import datetime, timezone

# Force stdout/stderr to be unbuffered for ACI logging
sys.stdout.reconfigure(line_buffering=True)
sys.stderr.reconfigure(line_buffering=True)

print("=== CLAIM EMULATOR STARTING ===", flush=True)

try:
    from azure.eventhub import EventData, EventHubProducerClient
    from azure.identity import ManagedIdentityCredential
    print("Azure SDK imports successful", flush=True)
except Exception as e:
    EventData = None
    EventHubProducerClient = None
    ManagedIdentityCredential = None
    print(f"Azure SDK imports deferred/unavailable: {e}", flush=True)

EVENT_HUB_NAMESPACE = os.getenv("EVENT_HUB_NAMESPACE")
EVENT_HUB_NAME = os.getenv("EVENT_HUB_NAME")
CLAIM_EVENT_RATE_PER_MINUTE = max(1, int(os.getenv("CLAIM_EVENT_RATE_PER_MINUTE", "60")))
PATIENT_COUNT = max(1, int(os.getenv("PATIENT_COUNT", "1000")))
FRAUD_VELOCITY_PROVIDER_ID = os.getenv("FRAUD_VELOCITY_PROVIDER_ID", "TEST-PROVIDER")
FRAUD_AMOUNT_MULTIPLIER = max(1, int(os.getenv("FRAUD_AMOUNT_MULTIPLIER", "6")))

print(f"EVENT_HUB_NAMESPACE: {EVENT_HUB_NAMESPACE}", flush=True)
print(f"EVENT_HUB_NAME: {EVENT_HUB_NAME}", flush=True)
print(f"CLAIM_EVENT_RATE_PER_MINUTE: {CLAIM_EVENT_RATE_PER_MINUTE}", flush=True)
print(f"PATIENT_COUNT: {PATIENT_COUNT}", flush=True)
print(f"FRAUD_VELOCITY_PROVIDER_ID: {FRAUD_VELOCITY_PROVIDER_ID}", flush=True)
print(f"FRAUD_AMOUNT_MULTIPLIER: {FRAUD_AMOUNT_MULTIPLIER}", flush=True)

DIAGNOSIS_PROFILES = {
    "E11.9": {
        "procedures": [("99213", 36), ("99214", 30), ("80053", 22), ("99215", 8), ("99282", 2), ("99283", 2)],
        "claim_types": [("professional", 68), ("institutional", 10), ("pharmacy", 16), ("emergency", 6)],
    },
    "I50.9": {
        "procedures": [("99214", 26), ("99215", 16), ("93000", 20), ("99282", 4), ("99283", 8), ("99284", 16), ("99285", 10)],
        "claim_types": [("professional", 42), ("institutional", 30), ("emergency", 22), ("pharmacy", 6)],
    },
    "J44.9": {
        "procedures": [("99214", 32), ("99215", 10), ("99281", 3), ("99282", 7), ("99283", 18), ("99284", 15), ("99285", 5), ("93000", 10)],
        "claim_types": [("professional", 52), ("institutional", 18), ("emergency", 24), ("pharmacy", 6)],
    },
    "Z00.00": {
        "procedures": [("99213", 60), ("80053", 28), ("99214", 8), ("93000", 2), ("99281", 2)],
        "claim_types": [("professional", 82), ("institutional", 3), ("pharmacy", 12), ("emergency", 3)],
    },
    "I10": {
        "procedures": [("99213", 38), ("99214", 30), ("93000", 14), ("80053", 14), ("99215", 3), ("99282", 1)],
        "claim_types": [("professional", 68), ("institutional", 10), ("pharmacy", 16), ("emergency", 6)],
    },
}

DIAGNOSIS_WEIGHTS = [("E11.9", 24), ("I50.9", 16), ("J44.9", 14), ("Z00.00", 18), ("I10", 28)]
PROCEDURE_AMOUNT_RANGES = {
    "99213": (95.0, 185.0),
    "99214": (145.0, 285.0),
    "99215": (225.0, 475.0),
    "99281": (250.0, 650.0),
    "99282": (450.0, 950.0),
    "99283": (900.0, 1900.0),
    "99284": (1900.0, 4600.0),
    "99285": (3600.0, 9200.0),
    "93000": (25.0, 125.0),
    "80053": (14.0, 85.0),
}
PAYER_WEIGHTS = [("PAY-01", 36), ("PAY-02", 24), ("PAY-03", 18), ("PAY-04", 12), ("PAY-05", 10)]
REGIONAL_FACILITIES = [
    ("FAC-001", 36.1627, -86.7816),
    ("FAC-002", 35.1495, -90.0490),
    ("FAC-003", 35.0456, -85.3097),
    ("FAC-004", 36.3134, -82.3535),
    ("FAC-005", 36.5298, -87.3595),
]


def weighted_choice(weighted_values):
    total = sum(weight for _, weight in weighted_values)
    pick = random.uniform(0, total)
    upto = 0
    for value, weight in weighted_values:
        upto += weight
        if upto >= pick:
            return value
    return weighted_values[-1][0]


def choose_amount(claim_type: str, procedure_code: str) -> float:
    if claim_type == "institutional":
        if procedure_code in {"99284", "99285"}:
            base = random.uniform(9000.0, 32000.0)
        else:
            base = random.uniform(3500.0, 18000.0)
    elif claim_type == "pharmacy":
        base = random.uniform(35.0, 950.0)
    else:
        low, high = PROCEDURE_AMOUNT_RANGES[procedure_code]
        base = random.uniform(low, high)
    return round(base, 2)


def choose_patient_number(counter: int) -> int:
    if counter % 12 == 0:
        return ((counter * 7) % min(PATIENT_COUNT, 50)) + 1
    return ((counter * 37) % PATIENT_COUNT) + 1


def choose_provider_id(counter: int) -> str:
    if counter % 5 == 0:
        return FRAUD_VELOCITY_PROVIDER_ID
    return f"PROV-{((counter * 29) % 650) + 1:04d}"


def generate_claim(counter: int) -> dict:
    flags = []
    diagnosis_code = weighted_choice(DIAGNOSIS_WEIGHTS)
    profile = DIAGNOSIS_PROFILES[diagnosis_code]
    procedure_code = weighted_choice(profile["procedures"])
    claim_type = weighted_choice(profile["claim_types"])

    if procedure_code in {"99281", "99282", "99283", "99284", "99285"}:
        claim_type = "emergency"

    if counter % 25 == 0:
        procedure_code = "99215"
        claim_type = "professional" if claim_type == "pharmacy" else claim_type
        flags.append("upcoding")

    provider_id = choose_provider_id(counter)
    facility_id, latitude, longitude = REGIONAL_FACILITIES[counter % len(REGIONAL_FACILITIES)]
    if claim_type == "professional" or claim_type == "pharmacy":
        facility_id = f"FAC-{((counter * 17) % 220) + 100:03d}"

    claim_amount = choose_amount(claim_type, procedure_code)
    if diagnosis_code in {"I50.9", "J44.9"} and claim_type in {"institutional", "emergency"}:
        claim_amount = round(claim_amount * random.uniform(1.05, 1.35), 2)

    if counter % 20 == 0:
        claim_amount = round(claim_amount * FRAUD_AMOUNT_MULTIPLIER, 2)
        flags.append("amount_outlier")

    if provider_id == FRAUD_VELOCITY_PROVIDER_ID:
        flags.append("velocity_burst")

    if counter % 40 == 0:
        flags.append("denial_pattern")

    patient_number = choose_patient_number(counter)
    return {
        "event_id": str(uuid.uuid4()),
        "event_timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "event_type": "CLAIM_SUBMITTED",
        "claim_id": f"CLM-{uuid.uuid4().hex[:10].upper()}",
        "patient_id": f"PAT-{patient_number:06d}",
        "provider_id": provider_id,
        "facility_id": facility_id,
        "payer_id": weighted_choice(PAYER_WEIGHTS),
        "diagnosis_code": diagnosis_code,
        "procedure_code": procedure_code,
        "claim_type": claim_type,
        "claim_amount": claim_amount,
        "latitude": latitude,
        "longitude": longitude,
        "injected_fraud_flags": "|".join(flags),
    }


def run():
    try:
        if EventHubProducerClient is None or ManagedIdentityCredential is None or EventData is None:
            raise RuntimeError("Azure SDK packages azure-eventhub and azure-identity are required when running the sender loop")
        print("Connecting to Event Hub using Managed Identity...", flush=True)
        credential = ManagedIdentityCredential()

        fully_qualified_namespace = f"{EVENT_HUB_NAMESPACE}.servicebus.windows.net"
        print(f"FQNS: {fully_qualified_namespace}", flush=True)

        print("Creating producer...", flush=True)
        producer = EventHubProducerClient(
            fully_qualified_namespace=fully_qualified_namespace,
            eventhub_name=EVENT_HUB_NAME,
            credential=credential,
        )

        events_per_second = CLAIM_EVENT_RATE_PER_MINUTE / 60.0
        accumulator = 0.0
        counter = 0
        cycle = 0
        last_payload = None

        print("Entering producer context...", flush=True)
        with producer:
            print("Starting claim event loop...", flush=True)
            while True:
                accumulator += events_per_second
                count = int(accumulator)
                if count <= 0:
                    time.sleep(1)
                    continue
                accumulator -= count

                batch = producer.create_batch()
                sent = 0
                for _ in range(count):
                    counter += 1
                    payload = generate_claim(counter)
                    last_payload = payload
                    try:
                        batch.add(EventData(json.dumps(payload)))
                    except ValueError:
                        producer.send_batch(batch)
                        batch = producer.create_batch()
                        batch.add(EventData(json.dumps(payload)))
                    sent += 1

                producer.send_batch(batch)
                cycle += 1

                if cycle % 10 == 0 and last_payload is not None:
                    print(
                        f"Cycle {cycle}: sent {sent} claim events to {EVENT_HUB_NAMESPACE}/{EVENT_HUB_NAME}; "
                        f"last_provider={last_payload['provider_id']}; last_flags={last_payload['injected_fraud_flags']}",
                        flush=True,
                    )

                time.sleep(1)
    except Exception as e:
        print(f"!!! Fatal Error: {e}", flush=True)
        traceback.print_exc()
        time.sleep(60)
        raise


if __name__ == "__main__":
    run()
