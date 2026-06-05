import os
import json

def create_omop_visuals():
    base_dir = "/Users/joey/git/med-device-fabric-emulator/phase-2/omop-research-report/OMOP Academic Research Dashboard.Report"
    
    # ═══════════════════════════════════════════════════════════════════════
    # PAGE 1: COHORT FEASIBILITY & ATTRITION
    # ═══════════════════════════════════════════════════════════════════════
    p1_dir = os.path.join(base_dir, "definition", "pages", "cohort_feasibility_page01", "visuals")
    
    # 1. Total Patients Card
    os.makedirs(os.path.join(p1_dir, "card_patients"), exist_ok=True)
    card_patients = {
        "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/visualContainer/2.5.0/schema.json",
        "name": "card_patients",
        "position": {
            "x": 20, "y": 20, "z": 1000,
            "height": 120, "width": 220, "tabOrder": 1000
        },
        "visual": {
            "visualType": "card",
            "query": {
                "queryState": {
                    "Values": {
                        "projections": [
                            {
                                "field": {
                                    "Column": {
                                        "Expression": {
                                            "SourceRef": {
                                                "Entity": "person"
                                            }
                                        },
                                        "Property": "person_id"
                                    }
                                },
                                "queryRef": "person.person_id",
                                "nativeQueryRef": "person_id"
                            }
                        ]
                    }
                }
            },
            "drillFilterOtherVisuals": True
        }
    }
    with open(os.path.join(p1_dir, "card_patients", "visual.json"), "w", encoding="utf-8") as f:
        json.dump(card_patients, f, indent=2)

    # 2. Condition Occurrences Card
    os.makedirs(os.path.join(p1_dir, "card_conditions"), exist_ok=True)
    card_conditions = {
        "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/visualContainer/2.5.0/schema.json",
        "name": "card_conditions",
        "position": {
            "x": 260, "y": 20, "z": 2000,
            "height": 120, "width": 220, "tabOrder": 2000
        },
        "visual": {
            "visualType": "card",
            "query": {
                "queryState": {
                    "Values": {
                        "projections": [
                            {
                                "field": {
                                    "Column": {
                                        "Expression": {
                                            "SourceRef": {
                                                "Entity": "condition_occurrence"
                                            }
                                        },
                                        "Property": "condition_occurrence_id"
                                    }
                                },
                                "queryRef": "condition_occurrence.condition_occurrence_id",
                                "nativeQueryRef": "condition_occurrence_id"
                            }
                        ]
                    }
                }
            },
            "drillFilterOtherVisuals": True
        }
    }
    with open(os.path.join(p1_dir, "card_conditions", "visual.json"), "w", encoding="utf-8") as f:
        json.dump(card_conditions, f, indent=2)

    # 3. Drug Exposures Card
    os.makedirs(os.path.join(p1_dir, "card_drugs"), exist_ok=True)
    card_drugs = {
        "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/visualContainer/2.5.0/schema.json",
        "name": "card_drugs",
        "position": {
            "x": 500, "y": 20, "z": 3000,
            "height": 120, "width": 220, "tabOrder": 3000
        },
        "visual": {
            "visualType": "card",
            "query": {
                "queryState": {
                    "Values": {
                        "projections": [
                            {
                                "field": {
                                    "Column": {
                                        "Expression": {
                                            "SourceRef": {
                                                "Entity": "drug_exposure"
                                            }
                                        },
                                        "Property": "drug_exposure_id"
                                    }
                                },
                                "queryRef": "drug_exposure.drug_exposure_id",
                                "nativeQueryRef": "drug_exposure_id"
                            }
                        ]
                    }
                }
            },
            "drillFilterOtherVisuals": True
        }
    }
    with open(os.path.join(p1_dir, "card_drugs", "visual.json"), "w", encoding="utf-8") as f:
        json.dump(card_drugs, f, indent=2)

    # 4. Age Distribution Chart
    os.makedirs(os.path.join(p1_dir, "chart_birthyear"), exist_ok=True)
    chart_birthyear = {
        "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/visualContainer/2.7.0/schema.json",
        "name": "chart_birthyear",
        "position": {
            "x": 20, "y": 160, "z": 4000,
            "height": 450, "width": 700, "tabOrder": 4000
        },
        "visual": {
            "visualType": "barChart",
            "query": {
                "queryState": {
                    "Category": {
                        "projections": [
                            {
                                "field": {
                                    "Column": {
                                        "Expression": {
                                            "SourceRef": {
                                                "Entity": "person"
                                            }
                                        },
                                        "Property": "year_of_birth"
                                    }
                                },
                                "queryRef": "person.year_of_birth",
                                "nativeQueryRef": "year_of_birth"
                            }
                        ]
                    },
                    "Y": {
                        "projections": [
                            {
                                "field": {
                                    "Column": {
                                        "Expression": {
                                            "SourceRef": {
                                                "Entity": "person"
                                            }
                                        },
                                        "Property": "person_id"
                                    }
                                },
                                "queryRef": "person.person_id",
                                "nativeQueryRef": "person_id"
                            }
                        ]
                    }
                }
            },
            "drillFilterOtherVisuals": True
        }
    }
    with open(os.path.join(p1_dir, "chart_birthyear", "visual.json"), "w", encoding="utf-8") as f:
        json.dump(chart_birthyear, f, indent=2)

    # ═══════════════════════════════════════════════════════════════════════
    # PAGE 2: CLINICAL JOURNEYS & PATHWAYS
    # ═══════════════════════════════════════════════════════════════════════
    p2_dir = os.path.join(base_dir, "definition", "pages", "patient_journeys_page02", "visuals")
    os.makedirs(os.path.join(p2_dir, "table_journeys"), exist_ok=True)
    table_journeys = {
        "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/visualContainer/2.5.0/schema.json",
        "name": "table_journeys",
        "position": {
            "x": 20, "y": 20, "z": 1000,
            "height": 600, "width": 1100, "tabOrder": 1000
        },
        "visual": {
            "visualType": "table",
            "query": {
                "queryState": {
                    "Values": {
                        "projections": [
                            {
                                "field": {
                                    "Column": {
                                        "Expression": {
                                            "SourceRef": {
                                                "Entity": "person"
                                            }
                                        },
                                        "Property": "person_id"
                                    }
                                },
                                "queryRef": "person.person_id",
                                "nativeQueryRef": "person_id"
                            },
                            {
                                "field": {
                                    "Column": {
                                        "Expression": {
                                            "SourceRef": {
                                                "Entity": "drug_exposure"
                                            }
                                        },
                                        "Property": "drug_concept_id"
                                    }
                                },
                                "queryRef": "drug_exposure.drug_concept_id",
                                "nativeQueryRef": "drug_concept_id"
                            },
                            {
                                "field": {
                                    "Column": {
                                        "Expression": {
                                            "SourceRef": {
                                                "Entity": "drug_exposure"
                                            }
                                        },
                                        "Property": "drug_exposure_start_date"
                                    }
                                },
                                "queryRef": "drug_exposure.drug_exposure_start_date",
                                "nativeQueryRef": "drug_exposure_start_date"
                            }
                        ]
                    }
                }
            },
            "drillFilterOtherVisuals": True
        }
    }
    with open(os.path.join(p2_dir, "table_journeys", "visual.json"), "w", encoding="utf-8") as f:
        json.dump(table_journeys, f, indent=2)

    # ═══════════════════════════════════════════════════════════════════════
    # PAGE 3: COMORBIDITY & BASELINE CHARACTERISTICS
    # ═══════════════════════════════════════════════════════════════════════
    p3_dir = os.path.join(base_dir, "definition", "pages", "comorbidity_profiles_page03", "visuals")
    os.makedirs(os.path.join(p3_dir, "table_comorbidities"), exist_ok=True)
    table_comorbidities = {
        "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/visualContainer/2.5.0/schema.json",
        "name": "table_comorbidities",
        "position": {
            "x": 20, "y": 20, "z": 1000,
            "height": 600, "width": 1100, "tabOrder": 1000
        },
        "visual": {
            "visualType": "table",
            "query": {
                "queryState": {
                    "Values": {
                        "projections": [
                            {
                                "field": {
                                    "Column": {
                                        "Expression": {
                                            "SourceRef": {
                                                "Entity": "person"
                                            }
                                        },
                                        "Property": "person_id"
                                    }
                                },
                                "queryRef": "person.person_id",
                                "nativeQueryRef": "person_id"
                            },
                            {
                                "field": {
                                    "Column": {
                                        "Expression": {
                                            "SourceRef": {
                                                "Entity": "condition_occurrence"
                                            }
                                        },
                                        "Property": "condition_concept_id"
                                    }
                                },
                                "queryRef": "condition_occurrence.condition_concept_id",
                                "nativeQueryRef": "condition_concept_id"
                            },
                            {
                                "field": {
                                    "Column": {
                                        "Expression": {
                                            "SourceRef": {
                                                "Entity": "condition_occurrence"
                                            }
                                        },
                                        "Property": "condition_start_date"
                                    }
                                },
                                "queryRef": "condition_occurrence.condition_start_date",
                                "nativeQueryRef": "condition_start_date"
                            }
                        ]
                    }
                }
            },
            "drillFilterOtherVisuals": True
        }
    }
    with open(os.path.join(p3_dir, "table_comorbidities", "visual.json"), "w", encoding="utf-8") as f:
        json.dump(table_comorbidities, f, indent=2)

    # ═══════════════════════════════════════════════════════════════════════
    # PAGE 4: MEASUREMENT DENSITY & OUTLIERS
    # ═══════════════════════════════════════════════════════════════════════
    p4_dir = os.path.join(base_dir, "definition", "pages", "lab_measurements_page04", "visuals")
    os.makedirs(os.path.join(p4_dir, "table_measurements"), exist_ok=True)
    table_measurements = {
        "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/visualContainer/2.5.0/schema.json",
        "name": "table_measurements",
        "position": {
            "x": 20, "y": 20, "z": 1000,
            "height": 600, "width": 1100, "tabOrder": 1000
        },
        "visual": {
            "visualType": "table",
            "query": {
                "queryState": {
                    "Values": {
                        "projections": [
                            {
                                "field": {
                                    "Column": {
                                        "Expression": {
                                            "SourceRef": {
                                                "Entity": "person"
                                            }
                                        },
                                        "Property": "person_id"
                                    }
                                },
                                "queryRef": "person.person_id",
                                "nativeQueryRef": "person_id"
                            },
                            {
                                "field": {
                                    "Column": {
                                        "Expression": {
                                            "SourceRef": {
                                                "Entity": "measurement"
                                            }
                                        },
                                        "Property": "measurement_concept_id"
                                    }
                                },
                                "queryRef": "measurement.measurement_concept_id",
                                "nativeQueryRef": "measurement_concept_id"
                            },
                            {
                                "field": {
                                    "Column": {
                                        "Expression": {
                                            "SourceRef": {
                                                "Entity": "measurement"
                                            }
                                        },
                                        "Property": "value_as_number"
                                    }
                                },
                                "queryRef": "measurement.value_as_number",
                                "nativeQueryRef": "value_as_number"
                            },
                            {
                                "field": {
                                    "Column": {
                                        "Expression": {
                                            "SourceRef": {
                                                "Entity": "measurement"
                                            }
                                        },
                                        "Property": "measurement_date"
                                    }
                                },
                                "queryRef": "measurement.measurement_date",
                                "nativeQueryRef": "measurement_date"
                            }
                        ]
                    }
                }
            },
            "drillFilterOtherVisuals": True
        }
    }
    with open(os.path.join(p4_dir, "table_measurements", "visual.json"), "w", encoding="utf-8") as f:
        json.dump(table_measurements, f, indent=2)

    print("Success: OMOP Academic Research Dashboard pages populated with fully valid visuals!")

if __name__ == "__main__":
    create_omop_visuals()
