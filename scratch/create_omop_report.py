import os
import json

def create_omop_report():
    base_dir = "/Users/joey/git/med-device-fabric-emulator/phase-2/omop-research-report"
    
    # 1. Create directory paths
    report_path = os.path.join(base_dir, "OMOP Academic Research Dashboard.Report")
    
    os.makedirs(os.path.join(report_path, "definition", "pages", "cohort_feasibility_page01"), exist_ok=True)
    os.makedirs(os.path.join(report_path, "definition", "pages", "patient_journeys_page02"), exist_ok=True)
    os.makedirs(os.path.join(report_path, "definition", "pages", "comorbidity_profiles_page03"), exist_ok=True)
    os.makedirs(os.path.join(report_path, "definition", "pages", "lab_measurements_page04"), exist_ok=True)
    
    # 2. Write Report files
    # Report .platform
    platform_json = {
        "$schema": "https://developer.microsoft.com/json-schemas/fabric/gitIntegration/platformProperties/2.0.0/schema.json",
        "metadata": {
            "type": "Report",
            "displayName": "OMOP Academic Research Dashboard"
        },
        "config": {
            "version": "2.0",
            "logicalId": "00000000-0000-0000-0000-000000000000"
        }
    }
    with open(os.path.join(report_path, ".platform"), "w", encoding="utf-8") as f:
        json.dump(platform_json, f, indent=2)
        
    # Report definition.pbir (connects directly to standard OMOP semantic model)
    pbir_json = {
        "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definitionProperties/2.0.0/schema.json",
        "version": "4.0",
        "datasetReference": {
            "byConnection": {
                "connectionString": "Data Source=powerbi://api.powerbi.com/v1.0/myorg/med-0528-f;initial catalog=healthcare1_msft_omop_semantic_model;integrated security=ClaimsToken;semanticmodelid=728137ca-0ed3-4821-824a-a58e58bb69bf"
            }
        }
    }
    with open(os.path.join(report_path, "definition.pbir"), "w", encoding="utf-8") as f:
        json.dump(pbir_json, f, indent=2)
        
    # Report definition/version.json
    version_json = {
        "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/versionMetadata/1.0.0/schema.json",
        "version": "2.0.0"
    }
    with open(os.path.join(report_path, "definition", "version.json"), "w", encoding="utf-8") as f:
        json.dump(version_json, f, indent=2)
        
    # Report definition/report.json (clean, standard CYP25 layout)
    report_json = {
        "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/report/3.2.0/schema.json",
        "themeCollection": {
            "baseTheme": {
                "name": "CY25SU12",
                "reportVersionAtImport": {
                    "visual": "2.5.0",
                    "report": "3.1.0",
                    "page": "2.3.0"
                },
                "type": "SharedResources"
            }
        },
        "objects": {
            "section": [
                {
                    "properties": {
                        "verticalAlignment": {
                            "expr": {
                                "Literal": {
                                    "Value": "'Top'"
                                }
                            }
                        }
                    }
                }
            ]
        },
        "resourcePackages": [
            {
                "name": "SharedResources",
                "type": "SharedResources",
                "items": [
                    {
                        "name": "CY25SU12",
                        "path": "BaseThemes/CY25SU12.json",
                        "type": "BaseTheme"
                    }
                ]
            }
        ],
        "settings": {
            "useStylableVisualContainerHeader": True,
            "exportDataMode": "AllowSummarized",
            "defaultDrillFilterOtherVisuals": True,
            "allowChangeFilterTypes": True,
            "useEnhancedTooltips": True,
            "useDefaultAggregateDisplayName": True
        }
    }
    with open(os.path.join(report_path, "definition", "report.json"), "w", encoding="utf-8") as f:
        json.dump(report_json, f, indent=2)
        
    # Report pages.json
    pages_json = {
        "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/pagesMetadata/1.0.0/schema.json",
        "pageOrder": [
            "cohort_feasibility_page01",
            "patient_journeys_page02",
            "comorbidity_profiles_page03",
            "lab_measurements_page04"
        ],
        "activePageName": "cohort_feasibility_page01"
    }
    with open(os.path.join(report_path, "definition", "pages", "pages.json"), "w", encoding="utf-8") as f:
        json.dump(pages_json, f, indent=2)
        
    # Page JSONs
    pages_meta = {
        "cohort_feasibility_page01": "Cohort Feasibility & Attrition",
        "patient_journeys_page02": "Clinical Journeys & Pathways",
        "comorbidity_profiles_page03": "Comorbidity & Baseline Characteristics",
        "lab_measurements_page04": "Measurement Density & Outliers"
    }
    for p_id, p_name in pages_meta.items():
        page_json = {
            "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/page/2.0.0/schema.json",
            "name": p_id,
            "displayName": p_name,
            "displayOption": "FitToPage",
            "height": 720,
            "width": 1280
        }
        with open(os.path.join(report_path, "definition", "pages", p_id, "page.json"), "w", encoding="utf-8") as f:
            json.dump(page_json, f, indent=2)
            
    print("Success: OMOP Academic Research Dashboard PBIP structure updated for direct Fabric upload!")

if __name__ == "__main__":
    create_omop_report()
