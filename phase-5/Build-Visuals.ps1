<#
.SYNOPSIS
    Generates visual containers for the CMS Quality Scorecard PBIP pages.
    Run this whenever the table/measure schema changes.

.DESCRIPTION
    Each page gets a small set of cards, slicers, charts, and tables wired to
    the SemanticModel produced by phase-5/materialize_claims_quality.py. The
    visuals reference measures from `_Measures` and columns from the fact/dim
    tables built by the materialization notebook.
#>
$ErrorActionPreference = "Stop"

$schema = "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/visualContainer/2.5.0/schema.json"
$pagesDir = Join-Path $PSScriptRoot "cms-quality-report\CMS Quality Scorecard.Report\definition\pages"

if (-not (Test-Path $pagesDir)) {
    throw "Pages directory not found: $pagesDir"
}

function Write-VisualJson {
    param(
        [string]$PageDir,
        [string]$Name,
        [hashtable]$Visual
    )
    $visualDir = Join-Path $PageDir "visuals\$Name"
    New-Item -ItemType Directory -Path $visualDir -Force | Out-Null
    $obj = [ordered]@{
        '$schema' = $schema
        name      = $Name
        position  = $Visual.position
        visual    = $Visual.body
    }
    $json = $obj | ConvertTo-Json -Depth 30
    $path = Join-Path $visualDir "visual.json"
    [IO.File]::WriteAllText($path, $json, [Text.UTF8Encoding]::new($false))
    Write-Host "  ✓ $Name"
}

function Pos ([int]$x, [int]$y, [int]$w, [int]$h, [int]$z) {
    return @{ x = $x; y = $y; width = $w; height = $h; z = $z; tabOrder = $z }
}

# Field shape helpers — each must produce valid Fabric `projection` entries.
function MeasureField ([string]$measureName) {
    return @{
        field = @{
            Measure = @{
                Expression = @{ SourceRef = @{ Entity = "_Measures" } }
                Property   = $measureName
            }
        }
        queryRef       = "_Measures.$measureName"
        nativeQueryRef = $measureName
    }
}

function ColumnField ([string]$entity, [string]$property) {
    return @{
        field = @{
            Column = @{
                Expression = @{ SourceRef = @{ Entity = $entity } }
                Property   = $property
            }
        }
        queryRef       = "$entity.$property"
        nativeQueryRef = $property
    }
}

# Visual factory functions
function CardBody ([string]$measureName) {
    return @{
        visualType = "card"
        query = @{
            queryState = @{
                Values = @{ projections = @( MeasureField $measureName ) }
            }
        }
        drillFilterOtherVisuals = $true
    }
}

function SlicerBody ([string]$entity, [string]$column) {
    return @{
        visualType = "slicer"
        query = @{
            queryState = @{
                Values = @{ projections = @( ColumnField $entity $column ) }
            }
        }
        drillFilterOtherVisuals = $true
    }
}

# Bar/column chart with a category and one or more measures
function BarChartBody ([string]$catEntity, [string]$catColumn, [string[]]$measures, [string]$visualType = "barChart") {
    $catProj = ColumnField $catEntity $catColumn
    $measureProjs = $measures | ForEach-Object { MeasureField $_ }
    return @{
        visualType = $visualType
        query = @{
            queryState = @{
                Category = @{ projections = @( $catProj ) }
                Y        = @{ projections = @( $measureProjs ) }
            }
        }
        drillFilterOtherVisuals = $true
    }
}

# Table / matrix with arbitrary projections
function TableBody ([object[]]$projections, [string]$visualType = "tableEx") {
    return @{
        visualType = $visualType
        query = @{
            queryState = @{
                Values = @{ projections = $projections }
            }
        }
        drillFilterOtherVisuals = $true
    }
}

# Text-style title using a card with a placeholder dataless intro is messy;
# Fabric textbox visualType is `textbox`. We avoid them since rich text needs
# full markup payload — page titles are already provided via displayName.

# ============================================================================
# PAGE 1 — Quality Measures Overview
# ============================================================================
Write-Host "Page 1: Quality Measures Overview"
$p1 = Join-Path $pagesDir "quality_overview_page01"
# 4 KPI cards across the top
Write-VisualJson $p1 "card_quality_rate"      @{ position = (Pos  10  20 280 130 1000); body = (CardBody "Quality Rate") }
Write-VisualJson $p1 "card_patients_measured" @{ position = (Pos 300  20 280 130 1100); body = (CardBody "Patients Measured") }
Write-VisualJson $p1 "card_open_gaps"         @{ position = (Pos 590  20 280 130 1200); body = (CardBody "Open Care Gaps") }
Write-VisualJson $p1 "card_measures_met"      @{ position = (Pos 880  20 280 130 1300); body = (CardBody "Measures Met") }
# Quality rate by measure (bar chart)
Write-VisualJson $p1 "chart_quality_by_measure" @{
    position = (Pos 10 160 580 530 2000)
    body = (BarChartBody "agg_quality_summary" "measure_name" @("Quality Rate") "barChart")
}
# Quality rate by year (column chart)
Write-VisualJson $p1 "chart_quality_by_year" @{
    position = (Pos 600 160 560 530 2100)
    body = (BarChartBody "agg_quality_summary" "measurement_year" @("Quality Rate") "columnChart")
}

# ============================================================================
# PAGE 2 — Measure Deep-Dive
# ============================================================================
Write-Host ""
Write-Host "Page 2: Measure Deep-Dive"
$p2 = Join-Path $pagesDir "measure_deepdive_page02"
Write-VisualJson $p2 "slicer_measure" @{ position = (Pos 10 20 250 120 1000); body = (SlicerBody "agg_quality_summary" "measure_name") }
Write-VisualJson $p2 "slicer_year"    @{ position = (Pos 270 20 250 120 1100); body = (SlicerBody "agg_quality_summary" "measurement_year") }
Write-VisualJson $p2 "slicer_payer"   @{ position = (Pos 530 20 250 120 1200); body = (SlicerBody "dim_payer" "payer_category") }

# Big detail table
$p2Projections = @(
    (ColumnField "agg_quality_summary" "measure_name"),
    (ColumnField "agg_quality_summary" "measurement_year"),
    (ColumnField "agg_quality_summary" "payer_category"),
    (ColumnField "agg_quality_summary" "denominator_count"),
    (ColumnField "agg_quality_summary" "numerator_count"),
    (ColumnField "agg_quality_summary" "quality_rate"),
    (ColumnField "agg_quality_summary" "benchmark_rate")
)
Write-VisualJson $p2 "table_measures" @{ position = (Pos 10 150 1150 540 2000); body = (TableBody $p2Projections "tableEx") }

# ============================================================================
# PAGE 3 — Claims Analytics
# ============================================================================
Write-Host ""
Write-Host "Page 3: Claims Analytics"
$p3 = Join-Path $pagesDir "claims_analytics_page03"
Write-VisualJson $p3 "card_total_claims"      @{ position = (Pos  10  20 280 130 1000); body = (CardBody "Total Claims") }
Write-VisualJson $p3 "card_total_billed"      @{ position = (Pos 300  20 280 130 1100); body = (CardBody "Total Billed") }
Write-VisualJson $p3 "card_total_paid"        @{ position = (Pos 590  20 280 130 1200); body = (CardBody "Total Paid") }
Write-VisualJson $p3 "card_collection_rate"   @{ position = (Pos 880  20 280 130 1300); body = (CardBody "Collection Rate") }

# Claims by payer
Write-VisualJson $p3 "chart_paid_by_payer" @{
    position = (Pos 10 160 570 530 2000)
    body = (BarChartBody "fact_claim" "payer_category" @("Total Paid","Total Billed") "barChart")
}
# Denial rate trend
Write-VisualJson $p3 "chart_denial_rate" @{
    position = (Pos 590 160 570 530 2100)
    body = (BarChartBody "fact_claim" "claim_type" @("Denial Rate","Collection Rate") "columnChart")
}

# ============================================================================
# PAGE 4 — Medication Adherence
# ============================================================================
Write-Host ""
Write-Host "Page 4: Medication Adherence"
$p4 = Join-Path $pagesDir "medication_adherence_page04"
Write-VisualJson $p4 "card_avg_pdc"          @{ position = (Pos  10 20 280 130 1000); body = (CardBody "Avg PDC Score") }
Write-VisualJson $p4 "card_non_adherent"     @{ position = (Pos 300 20 280 130 1100); body = (CardBody "Non-Adherent Patients") }

Write-VisualJson $p4 "slicer_med_class" @{ position = (Pos 590 20 280 130 1200); body = (SlicerBody "agg_medication_adherence" "medication_class") }
Write-VisualJson $p4 "slicer_adherence" @{ position = (Pos 880 20 280 130 1300); body = (SlicerBody "agg_medication_adherence" "adherence_category") }

# PDC distribution by class
Write-VisualJson $p4 "chart_pdc_by_class" @{
    position = (Pos 10 160 570 530 2000)
    body = (BarChartBody "agg_medication_adherence" "medication_class" @("Avg PDC Score") "barChart")
}
# Patient-level table
$p4Projections = @(
    (ColumnField "agg_medication_adherence" "patient_id"),
    (ColumnField "agg_medication_adherence" "medication_class"),
    (ColumnField "agg_medication_adherence" "adherence_category"),
    (ColumnField "agg_medication_adherence" "pdc_score"),
    (ColumnField "agg_medication_adherence" "gap_days"),
    (ColumnField "agg_medication_adherence" "total_fills")
)
Write-VisualJson $p4 "table_adherence" @{ position = (Pos 590 160 570 530 2100); body = (TableBody $p4Projections "tableEx") }

# ============================================================================
# PAGE 5 — Care Gap Closure
# ============================================================================
Write-Host ""
Write-Host "Page 5: Care Gap Closure"
$p5 = Join-Path $pagesDir "care_gap_closure_page05"
Write-VisualJson $p5 "card_open_gaps"     @{ position = (Pos  10 20 280 130 1000); body = (CardBody "Open Care Gaps") }
Write-VisualJson $p5 "slicer_gap_status"  @{ position = (Pos 300 20 280 130 1100); body = (SlicerBody "care_gaps" "gap_status") }
Write-VisualJson $p5 "slicer_gap_type"    @{ position = (Pos 590 20 280 130 1200); body = (SlicerBody "care_gaps" "gap_type") }
Write-VisualJson $p5 "slicer_measure"     @{ position = (Pos 880 20 280 130 1300); body = (SlicerBody "care_gaps" "measure_id") }

# Care gaps by measure
Write-VisualJson $p5 "chart_gaps_by_measure" @{
    position = (Pos 10 160 570 530 2000)
    body = (BarChartBody "care_gaps" "measure_id" @("Open Care Gaps") "barChart")
}
# Patient-level care gap table
$p5Projections = @(
    (ColumnField "care_gaps" "patient_id"),
    (ColumnField "care_gaps" "measure_id"),
    (ColumnField "care_gaps" "gap_type"),
    (ColumnField "care_gaps" "gap_status"),
    (ColumnField "care_gaps" "days_overdue"),
    (ColumnField "care_gaps" "recommended_action")
)
Write-VisualJson $p5 "table_gaps" @{ position = (Pos 590 160 570 530 2100); body = (TableBody $p5Projections "tableEx") }

# ============================================================================
# PAGE 6 — Payer Performance
# ============================================================================
Write-Host ""
Write-Host "Page 6: Payer Performance"
$p6 = Join-Path $pagesDir "payer_performance_page06"
# 4 payer-stratified KPI cards
Write-VisualJson $p6 "card_qr_medicare"    @{ position = (Pos  10 20 280 130 1000); body = (CardBody "Quality Rate (Medicare)") }
Write-VisualJson $p6 "card_qr_medicaid"    @{ position = (Pos 300 20 280 130 1100); body = (CardBody "Quality Rate (Medicaid)") }
Write-VisualJson $p6 "card_qr_commercial"  @{ position = (Pos 590 20 280 130 1200); body = (CardBody "Quality Rate (Commercial)") }
Write-VisualJson $p6 "card_qr_uninsured"   @{ position = (Pos 880 20 280 130 1300); body = (CardBody "Quality Rate (Uninsured)") }

# Payer-stratified bar chart
Write-VisualJson $p6 "chart_collection_by_payer" @{
    position = (Pos 10 160 570 530 2000)
    body = (BarChartBody "dim_payer" "payer_category" @("Collection Rate","Denial Rate") "barChart")
}
# Payer paid amounts
Write-VisualJson $p6 "chart_paid_by_payer" @{
    position = (Pos 590 160 570 530 2100)
    body = (BarChartBody "dim_payer" "payer_category" @("Total Paid","Total Billed") "columnChart")
}

Write-Host ""
Write-Host "✓ All visuals generated."
