#!/usr/bin/env python3
"""
Generate Prepackaged/Cached Synthea-style Clinical Bundles
Conforms to load_fhir.py validation and device_registry.json qualifying conditions.
Includes rich clinical resources (MedicationRequest, Procedure, Heart Rate and BP Observations)
to fully hydrate all OMOP CDM tables (drug_exposure, procedure_occurrence, measurement, observation, person).
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

MEDICATIONS = [
    {"code": "313226", "display": "Albuterol 0.09 MG/ACTUAT Inhalant Powder", "system": "http://www.nlm.nih.gov/research/umls/rxnorm"},
    {"code": "310963", "display": "Fluticasone propionate 0.044 MG/ACTUAT Inhalant Suspension", "system": "http://www.nlm.nih.gov/research/umls/rxnorm"},
    {"code": "855332", "display": "Lisinopril 10 MG Oral Tablet", "system": "http://www.nlm.nih.gov/research/umls/rxnorm"},
    {"code": "866514", "display": "Metoprolol succinate 50 MG G Ext-Release Oral Tablet", "system": "http://www.nlm.nih.gov/research/umls/rxnorm"},
    {"code": "197361", "display": "Amlodipine 5 MG Oral Tablet", "system": "http://www.nlm.nih.gov/research/umls/rxnorm"}
]

PROCEDURES = [
    {"code": "43075005", "display": "Inhalation therapy", "system": "http://snomed.info/sct"},
    {"code": "3981000175107", "display": "Oxygen administration", "system": "http://snomed.info/sct"},
    {"code": "5300003", "display": "Artificial respiration", "system": "http://snomed.info/sct"},
    {"code": "182764009", "display": "Systemic arterial blood pressure measurement", "system": "http://snomed.info/sct"},
    {"code": "312850006", "display": "History taking", "system": "http://snomed.info/sct"}
]

# Realistic per-patient encounter class mix for utilization/readmission analytics.
# Each patient receives the primary clinical-anchor encounter plus a deterministic
# series of historical encounters spanning ambulatory (AMB), emergency (EMER), and
# inpatient (IMP) classes across multiple years so downstream OMOP visit_occurrence
# and cost-by-service-category analytics are populated across every visit class.
ENCOUNTER_CLASS_CYCLE = [
    ("AMB", "ambulatory"),
    ("EMER", "emergency"),
    ("IMP", "inpatient encounter"),
    ("AMB", "ambulatory"),
    ("EMER", "emergency"),
    ("IMP", "inpatient encounter"),
]

RACE_ETHNICITY_PROFILES = [
    {
        "race_code": "2106-3",
        "race_display": "White",
        "ethnicity_code": "2186-5",
        "ethnicity_display": "Not Hispanic or Latino",
    },
    {
        "race_code": "2054-5",
        "race_display": "Black or African American",
        "ethnicity_code": "2186-5",
        "ethnicity_display": "Not Hispanic or Latino",
    },
    {
        "race_code": "2028-9",
        "race_display": "Asian",
        "ethnicity_code": "2186-5",
        "ethnicity_display": "Not Hispanic or Latino",
    },
    {
        "race_code": "2076-8",
        "race_display": "Native Hawaiian or Other Pacific Islander",
        "ethnicity_code": "2186-5",
        "ethnicity_display": "Not Hispanic or Latino",
    },
    {
        "race_code": "2106-3",
        "race_display": "White",
        "ethnicity_code": "2135-2",
        "ethnicity_display": "Hispanic or Latino",
    },
]

def patient_demographic_extensions(idx):
    profile = RACE_ETHNICITY_PROFILES[idx % len(RACE_ETHNICITY_PROFILES)]
    return [
        {
            "url": "http://hl7.org/fhir/us/core/StructureDefinition/us-core-race",
            "extension": [
                {
                    "url": "ombCategory",
                    "valueCoding": {
                        "system": "urn:oid:2.16.840.1.113883.6.238",
                        "code": profile["race_code"],
                        "display": profile["race_display"],
                    },
                },
                {"url": "text", "valueString": profile["race_display"]},
            ],
        },
        {
            "url": "http://hl7.org/fhir/us/core/StructureDefinition/us-core-ethnicity",
            "extension": [
                {
                    "url": "ombCategory",
                    "valueCoding": {
                        "system": "urn:oid:2.16.840.1.113883.6.238",
                        "code": profile["ethnicity_code"],
                        "display": profile["ethnicity_display"],
                    },
                },
                {"url": "text", "valueString": profile["ethnicity_display"]},
            ],
        },
    ]

PAYER_PROFILES = [
    {"id": "payer-medicare", "name": "Medicare", "category": "Medicare", "type_code": "EHCPOL", "type_display": "extended healthcare"},
    {"id": "payer-medicaid", "name": "Georgia Medicaid", "category": "Medicaid", "type_code": "MCPOL", "type_display": "managed care policy"},
    {"id": "payer-commercial", "name": "BrakeKat Commercial Health", "category": "Commercial", "type_code": "DENTPRG", "type_display": "commercial health plan"},
    {"id": "payer-uninsured", "name": "Self Pay", "category": "Uninsured", "type_code": "SELF-PAY", "type_display": "self-pay"},
]

CARE_GOAL_PROFILES = [
    {"text": "Improve respiratory symptom control", "target": "Maintain oxygen saturation at or above 94 percent"},
    {"text": "Reduce avoidable acute care utilization", "target": "Complete follow-up care plan activities within 30 days"},
    {"text": "Improve medication adherence", "target": "Take prescribed respiratory or cardiac medication as directed"},
]

def payer_organization_resource(payer):
    return {
        "resourceType": "Organization",
        "id": payer["id"],
        "active": True,
        "name": payer["name"],
        "type": [{
            "coding": [{
                "system": "http://terminology.hl7.org/CodeSystem/organization-type",
                "code": "pay",
                "display": "Payer",
            }],
            "text": "Payer",
        }],
    }

def coverage_resource(coverage_uuid, patient_uuid, payer):
    return {
        "resourceType": "Coverage",
        "id": coverage_uuid,
        "status": "active",
        "type": {
            "coding": [{
                "system": "http://terminology.hl7.org/CodeSystem/v3-ActCode",
                "code": payer["type_code"],
                "display": payer["type_display"],
            }],
            "text": payer["name"],
        },
        "beneficiary": {"reference": f"urn:uuid:{patient_uuid}"},
        "payor": [{"reference": f"Organization/{payer['id']}", "display": payer["name"]}],
        "period": {
            "start": (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d"),
            "end": (datetime.now() + timedelta(days=365)).strftime("%Y-%m-%d"),
        },
    }

def goal_resource(goal_uuid, patient_uuid, goal_profile):
    return {
        "resourceType": "Goal",
        "id": goal_uuid,
        "lifecycleStatus": "active",
        "description": {"text": goal_profile["text"]},
        "subject": {"reference": f"urn:uuid:{patient_uuid}"},
        "startDate": datetime.now().strftime("%Y-%m-%d"),
        "target": [{
            "measure": {"text": goal_profile["target"]},
            "dueDate": (datetime.now() + timedelta(days=90)).strftime("%Y-%m-%d"),
        }],
    }

def care_plan_resource(care_plan_uuid, patient_uuid, encounter_uuid, condition_uuid, goal_uuid, cond):
    return {
        "resourceType": "CarePlan",
        "id": care_plan_uuid,
        "status": "active",
        "intent": "plan",
        "category": [{
            "coding": [{
                "system": "http://hl7.org/fhir/us/core/CodeSystem/careplan-category",
                "code": "assess-plan",
                "display": "Assessment and Plan of Treatment",
            }],
            "text": "Assessment and Plan of Treatment",
        }],
        "title": f"Care management plan for {cond['display']}",
        "description": f"Synthetic care plan for {cond['display']} cohort analytics.",
        "subject": {"reference": f"urn:uuid:{patient_uuid}"},
        "encounter": {"reference": f"urn:uuid:{encounter_uuid}"},
        "period": {
            "start": datetime.now().strftime("%Y-%m-%d"),
            "end": (datetime.now() + timedelta(days=30)).strftime("%Y-%m-%d"),
        },
        "addresses": [{"reference": f"urn:uuid:{condition_uuid}"}],
        "goal": [{"reference": f"urn:uuid:{goal_uuid}"}],
        "activity": [{
            "detail": {
                "kind": "ServiceRequest",
                "code": {"text": "Care manager outreach"},
                "status": "scheduled",
            }
        }],
    }



def build_encounter_resource(encounter_uuid, patient_uuid, hospital, practitioner_npi, class_code, class_display, start_dt, end_dt):
    """Build a single FHIR Encounter of the given class, wired to the patient and hospital.

    IMP/EMER encounters carry a period and hospitalization block (admit source +
    discharge disposition) so downstream OMOP visit_occurrence gets length-of-stay,
    readmission, and discharge attributes; AMB encounters are lightweight visits.
    """
    is_facility = class_code in ("IMP", "EMER")
    resource = {
        "resourceType": "Encounter",
        "id": encounter_uuid,
        "status": "finished",
        "class": {
            "system": "http://terminology.hl7.org/CodeSystem/v3-ActCode",
            "code": class_code,
            "display": class_display,
        },
        "period": {
            "start": start_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "end": end_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
        },
        "subject": {"reference": f"urn:uuid:{patient_uuid}"},
        "participant": [
            {
                "type": [
                    {
                        "coding": [
                            {
                                "system": "http://terminology.hl7.org/CodeSystem/v3-ParticipationType",
                                "code": "PPRF",
                                "display": "primary performer",
                            }
                        ]
                    }
                ],
                "individual": {
                    "reference": f"Practitioner?identifier=http://hl7.org/fhir/sid/us-npi|{practitioner_npi}"
                },
            }
        ],
        "serviceProvider": {
            "reference": f"Organization/{hospital}",
            "display": hospital.replace("-", " ").title(),
        },
        "location": [
            {
                "location": {
                    "reference": f"Location?identifier=http://example.org/location-ids|loc-{hospital}",
                    "display": f"Location {hospital.replace('-', ' ').title()}",
                },
                "status": "completed",
            }
        ],
    }
    if is_facility:
        resource["hospitalization"] = {
            "admitSource": {"text": "Emergency" if class_code == "EMER" else "Physician referral"},
            "dischargeDisposition": {"text": "Home"},
        }
    return resource


def generate_patient(idx):
    patient_uuid = str(uuid.uuid4())
    encounter_uuid = str(uuid.uuid4())
    condition_uuid = str(uuid.uuid4())
    coverage_uuid = str(uuid.uuid4())
    goal_uuid = str(uuid.uuid4())
    care_plan_uuid = str(uuid.uuid4())
    practitioner_npi = f"99998{10000 + idx}"
    
    # Assign hospital
    hospital = ATLANTA_HOSPITALS[idx % len(ATLANTA_HOSPITALS)]
    is_choa = "choa" in hospital or "childrens" in hospital
    # Keep a deterministic inpatient cohort for utilization/readmission analytics.
    is_inpatient = idx % 4 == 0
    admission_date = (datetime.now() - timedelta(days=30 + (idx % 7))).strftime("%Y-%m-%dT%H:%M:%SZ")
    discharge_date = (datetime.now() - timedelta(days=25 + (idx % 5))).strftime("%Y-%m-%dT%H:%M:%SZ")
    
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
    payer = PAYER_PROFILES[idx % len(PAYER_PROFILES)]
    payer_org = payer_organization_resource(payer)
    goal_profile = CARE_GOAL_PROFILES[idx % len(CARE_GOAL_PROFILES)]
    coverage = coverage_resource(coverage_uuid, patient_uuid, payer)
    goal = goal_resource(goal_uuid, patient_uuid, goal_profile)
    care_plan = care_plan_resource(care_plan_uuid, patient_uuid, encounter_uuid, condition_uuid, goal_uuid, cond)

    
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
                    "extension": patient_demographic_extensions(idx),
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
                        "code": "IMP" if is_inpatient else "AMB",
                        "display": "inpatient encounter" if is_inpatient else "ambulatory"
                    },
                    **({
                        "period": {"start": admission_date, "end": discharge_date},
                        "hospitalization": {
                            "admitSource": {"text": "Emergency"},
                            "dischargeDisposition": {"text": "Home"}
                        }
                    } if is_inpatient else {}),
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
            }
            ,
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
            ,
            {
                "fullUrl": f"urn:uuid:{coverage_uuid}",
                "resource": coverage
            },
            {
                "fullUrl": f"urn:uuid:{goal_uuid}",
                "resource": goal
            },
            {
                "fullUrl": f"urn:uuid:{care_plan_uuid}",
                "resource": care_plan
            }
            ,
            {
                "fullUrl": f"Organization/{payer['id']}",
                "resource": payer_org
            }
        ]
    }
    
    # 1. Add Oxygen Saturation Observation (vital-signs)
    obs_oxy_uuid = str(uuid.uuid4())
    bundle["entry"].append({
        "fullUrl": f"urn:uuid:{obs_oxy_uuid}",
        "resource": {
            "resourceType": "Observation",
            "id": obs_oxy_uuid,
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
    
    # 2. Add Heart Rate Observation (LOINC 8867-4 -> maps to OMOP measurement!)
    obs_hr_uuid = str(uuid.uuid4())
    bundle["entry"].append({
        "fullUrl": f"urn:uuid:{obs_hr_uuid}",
        "resource": {
            "resourceType": "Observation",
            "id": obs_hr_uuid,
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
                        "code": "8867-4",
                        "display": "Heart rate"
                    }
                ],
                "text": "Heart rate"
            },
            "subject": {
                "reference": f"urn:uuid:{patient_uuid}"
            },
            "encounter": {
                "reference": f"urn:uuid:{encounter_uuid}"
            },
            "effectiveDateTime": datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ"),
            "valueQuantity": {
                "value": round(random.uniform(60.0, 100.0), 1),
                "unit": "beats/min",
                "system": "http://unitsofmeasure.org",
                "code": "/min"
            }
        }
    })

    # 3. Add Blood Pressure Observation (LOINC 8480-6 -> maps to OMOP measurement!)
    obs_bp_uuid = str(uuid.uuid4())
    bundle["entry"].append({
        "fullUrl": f"urn:uuid:{obs_bp_uuid}",
        "resource": {
            "resourceType": "Observation",
            "id": obs_bp_uuid,
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
                        "code": "8480-6",
                        "display": "Systolic blood pressure"
                    }
                ],
                "text": "Systolic blood pressure"
            },
            "subject": {
                "reference": f"urn:uuid:{patient_uuid}"
            },
            "encounter": {
                "reference": f"urn:uuid:{encounter_uuid}"
            },
            "effectiveDateTime": datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ"),
            "valueQuantity": {
                "value": round(random.uniform(110.0, 140.0), 1),
                "unit": "mmHg",
                "system": "http://unitsofmeasure.org",
                "code": "mm[Hg]"
            }
        }
    })

    # 4. Add MedicationRequest (RxNorm code -> maps to OMOP drug_exposure!)
    med = random.choice(MEDICATIONS)
    med_uuid = str(uuid.uuid4())
    bundle["entry"].append({
        "fullUrl": f"urn:uuid:{med_uuid}",
        "resource": {
            "resourceType": "MedicationRequest",
            "id": med_uuid,
            "status": "active",
            "intent": "order",
            "medicationCodeableConcept": {
                "coding": [
                    {
                        "system": med["system"],
                        "code": med["code"],
                        "display": med["display"]
                    }
                ],
                "text": med["display"]
            },
            "subject": {
                "reference": f"urn:uuid:{patient_uuid}"
            },
            "encounter": {
                "reference": f"urn:uuid:{encounter_uuid}"
            },
            "authoredOn": datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
        }
    })

    # 5. Add Procedure (SNOMED code -> maps to OMOP procedure_occurrence!)
    proc = random.choice(PROCEDURES)
    proc_uuid = str(uuid.uuid4())
    bundle["entry"].append({
        "fullUrl": f"urn:uuid:{proc_uuid}",
        "resource": {
            "resourceType": "Procedure",
            "id": proc_uuid,
            "status": "completed",
            "code": {
                "coding": [
                    {
                        "system": proc["system"],
                        "code": proc["code"],
                        "display": proc["display"]
                    }
                ],
                "text": proc["display"]
            },
            "subject": {
                "reference": f"urn:uuid:{patient_uuid}"
            },
            "encounter": {
                "reference": f"urn:uuid:{encounter_uuid}"
            },
            "performedDateTime": datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
        }
    })

    # 6. Add a deterministic series of historical encounters spanning AMB, EMER, and IMP
    #    classes across multiple years. This gives downstream OMOP visit_occurrence a
    #    realistic mix of ambulatory, emergency, and inpatient utilization (and the cost
    #    joins that hang off each visit) instead of a single anchor encounter.
    for enc_idx, (class_code, class_display) in enumerate(ENCOUNTER_CLASS_CYCLE):
        # Spread encounters over the trailing years; emergency/inpatient stays span days.
        start_dt = datetime.now() - timedelta(days=180 * (enc_idx + 1) + (idx % 30))
        if class_code == "IMP":
            end_dt = start_dt + timedelta(days=2 + (idx % 4), hours=6)
        elif class_code == "EMER":
            end_dt = start_dt + timedelta(hours=3 + (idx % 6))
        else:
            end_dt = start_dt + timedelta(minutes=15)
        extra_encounter_uuid = str(uuid.uuid4())
        bundle["entry"].append({
            "fullUrl": f"urn:uuid:{extra_encounter_uuid}",
            "resource": build_encounter_resource(
                extra_encounter_uuid, patient_uuid, hospital, practitioner_npi,
                class_code, class_display, start_dt, end_dt,
            ),
        })
    
    filename = f"{first_name}_{last_name}_{patient_uuid}.json"
    return filename, bundle

def main():
    out_dir = "/Users/joey/git/med-device-fabric-emulator/synthea/prepackaged"
    
    # Delete existing files in output directory first to prevent naming collisions
    print(f"Cleaning existing files in {out_dir}...")
    if os.path.exists(out_dir):
        for file in os.listdir(out_dir):
            file_path = os.path.join(out_dir, file)
            if os.path.isfile(file_path):
                os.remove(file_path)
                
    os.makedirs(out_dir, exist_ok=True)
    
    print(f"Generating 10 comprehensive clinical bundle files in {out_dir}...")
    for idx in range(10):
        fname, bundle = generate_patient(idx)
        filepath = os.path.join(out_dir, fname)
        with open(filepath, "w") as f:
            json.dump(bundle, f, indent=2)
        print(f"  Generated {fname}")
        
    print("Done generating comprehensive clinical bundles!")

if __name__ == "__main__":
    main()
