#!/usr/bin/env python3
"""
Generate Prepackaged/Cached Synthea-style Clinical Bundles
Conforms to load_fhir.py validation and device_registry.json qualifying conditions.
"""
import os
import json
import uuid
from datetime import datetime, timedelta
import random

# Qualifying SNOMED codes from device_registry.json
QUALIFYING_CONDITIONS = [
    {"code": "195967001", "display": "Asthma"},
    {"code": "13645005", "display": "Chronic obstructive lung disease"},
    {"code": "84114007", "display": "Heart failure"},
    {"code": "233604007", "display": "Pneumonia"},
    {"code": "59621000", "display": "Essential hypertension"},
    {"code": "162864005", "display": "Body mass index 30+ - obesity"},
    {"code": "840539006", "display": "COVID-19"}
]

ATLANTA_HOSPITALS = [
    "emory-university-hospital",
    "piedmont-atlanta-hospital",
    "grady-memorial-hospital",
    "northside-hospital",
    "wellstar-kennestone-hospital",
    "choa-egleston",
    "choa-scottish-rite",
    "choa-hughes-spalding"
]

def generate_patient(idx):
    patient_uuid = str(uuid.uuid4())
    encounter_uuid = str(uuid.uuid4())
    condition_uuid = str(uuid.uuid4())
    practitioner_npi = f"99998{10000 + idx}"
    
    # Assign hospital
    hospital = ATLANTA_HOSPITALS[idx % len(ATLANTA_HOSPITALS)]
    is_choa = "choa" in hospital or "childrens" in hospital
    
    # Age assignment: CHOA patients must be pediatric (< 21) to pass validation, others can be adult
    if is_choa:
        age_years = random.randint(2, 18)
    else:
        age_years = random.randint(25, 75)
        
    birth_date = (datetime.now() - timedelta(days=age_years*365.25 + random.randint(0, 365))).strftime("%Y-%m-%d")
    
    gender = random.choice(["male", "female"])
    first_name = random.choice(["James", "Mary", "John", "Patricia", "Robert", "Jennifer", "Michael", "Elizabeth", "William", "Linda"])
    last_name = random.choice(["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez", "Martinez"])
    
    # Qualifying condition
    cond = random.choice(QUALIFYING_CONDITIONS)
    
    bundle = {
        "resourceType": "Bundle",
        "type": "transaction",
        "entry": [
            {
                "fullUrl": f"urn:uuid:{patient_uuid}",
                "resource": {
                    "resourceType": "Patient",
                    "id": patient_uuid,
                    "active": True,
                    "name": [
                        {
                            "use": "official",
                            "family": last_name,
                            "given": [first_name]
                        }
                    ],
                    "gender": gender,
                    "birthDate": birth_date,
                    "address": [
                        {
                            "use": "home",
                            "line": [f"{random.randint(100, 9999)} Peachtree St NE"],
                            "city": "Atlanta",
                            "state": "GA",
                            "postalCode": "30309",
                            "country": "US"
                        }
                    ]
                }
            },
            {
                "fullUrl": f"urn:uuid:{encounter_uuid}",
                "resource": {
                    "resourceType": "Encounter",
                    "id": encounter_uuid,
                    "status": "finished",
                    "class": {
                        "system": "http://terminology.hl7.org/CodeSystem/v3-ActCode",
                        "code": "AMB",
                        "display": "ambulatory"
                    },
                    "subject": {
                        "reference": f"urn:uuid:{patient_uuid}"
                    },
                    "participant": [
                        {
                            "type": [
                                {
                                    "coding": [
                                        {
                                            "system": "http://terminology.hl7.org/CodeSystem/v3-ParticipationType",
                                            "code": "PPRF",
                                            "display": "primary performer"
                                        }
                                    ]
                                }
                            ],
                            "individual": {
                                "reference": f"Practitioner?identifier=http://hl7.org/fhir/sid/us-npi|{practitioner_npi}"
                            }
                        }
                    ],
                    "serviceProvider": {
                        "reference": f"Organization/{hospital}",
                        "display": hospital.replace("-", " ").title()
                    },
                    "location": [
                        {
                            "location": {
                                "reference": f"Location?identifier=http://example.org/location-ids|loc-{hospital}",
                                "display": f"Location {hospital.replace('-', ' ').title()}"
                            },
                            "status": "completed"
                        }
                    ]
                }
            },
            {
                "fullUrl": f"urn:uuid:{condition_uuid}",
                "resource": {
                    "resourceType": "Condition",
                    "id": condition_uuid,
                    "clinicalStatus": {
                        "coding": [
                            {
                                "system": "http://terminology.hl7.org/CodeSystem/condition-clinical",
                                "code": "active"
                            }
                        ]
                    },
                    "verificationStatus": {
                        "coding": [
                            {
                                "system": "http://terminology.hl7.org/CodeSystem/condition-ver-status",
                                "code": "confirmed"
                            }
                        ]
                    },
                    "category": [
                        {
                            "coding": [
                                {
                                    "system": "http://terminology.hl7.org/CodeSystem/condition-category",
                                    "code": "encounter-diagnosis",
                                    "display": "Encounter Diagnosis"
                                }
                            ]
                        }
                    ],
                    "code": {
                        "coding": [
                            {
                                "system": "http://snomed.info/sct",
                                "code": cond["code"],
                                "display": cond["display"]
                            }
                        ],
                        "text": cond["display"]
                    },
                    "subject": {
                        "reference": f"urn:uuid:{patient_uuid}"
                    },
                    "encounter": {
                        "reference": f"urn:uuid:{encounter_uuid}"
                    },
                    "recordedDate": datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
                }
            }
        ]
    }
    
    # Add a simple pulse oximetry observation for rich clinical realism
    obs_uuid = str(uuid.uuid4())
    bundle["entry"].append({
        "fullUrl": f"urn:uuid:{obs_uuid}",
        "resource": {
            "resourceType": "Observation",
            "id": obs_uuid,
            "status": "final",
            "category": [
                {
                    "coding": [
                        {
                            "system": "http://terminology.hl7.org/CodeSystem/observation-category",
                            "code": "vital-signs",
                            "display": "Vital Signs"
                        }
                    ]
                }
            ],
            "code": {
                "coding": [
                    {
                        "system": "http://loinc.org",
                        "code": "2708-6",
                        "display": "Oxygen saturation in Arterial blood by Pulse oximetry"
                    }
                ],
                "text": "Oxygen saturation"
            },
            "subject": {
                "reference": f"urn:uuid:{patient_uuid}"
            },
            "encounter": {
                "reference": f"urn:uuid:{encounter_uuid}"
            },
            "effectiveDateTime": datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ"),
            "valueQuantity": {
                "value": round(random.uniform(93.0, 99.0), 1),
                "unit": "%",
                "system": "http://unitsofmeasure.org",
                "code": "%"
            }
        }
    })
    
    filename = f"{first_name}_{last_name}_{patient_uuid}.json"
    return filename, bundle

def main():
    out_dir = "/Users/joey/git/med-device-fabric-emulator/synthea/prepackaged"
    os.makedirs(out_dir, exist_ok=True)
    
    print(f"Generating 10 clinical bundle files in {out_dir}...")
    for idx in range(10):
        fname, bundle = generate_patient(idx)
        filepath = os.path.join(out_dir, fname)
        with open(filepath, "w") as f:
            json.dump(bundle, f, indent=2)
        print(f"  Generated {fname}")
        
    print("Done generating prepackaged clinical bundles!")

if __name__ == "__main__":
    main()
