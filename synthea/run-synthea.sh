#!/bin/bash
set -e

echo "=== SYNTHEA GENERATOR STARTING ==="
echo "Patient Count: $PATIENT_COUNT"
echo "Storage Account: $STORAGE_ACCOUNT"
echo "Container Name: $CONTAINER_NAME"

# Run Synthea for Atlanta, Georgia with our custom configuration
echo "Running Synthea..."
java $JAVA_OPTS -jar /synthea/synthea-with-dependencies.jar \
    -c /synthea/synthea.properties \
    -p $PATIENT_COUNT \
    --exporter.baseDirectory /output \
    Georgia Atlanta

echo "=== SYNTHEA GENERATION COMPLETE ==="
echo "Files generated:"
ls -la /output/fhir/ | head -20
echo "Total FHIR files: $(ls -1 /output/fhir/*.json 2>/dev/null | wc -l)"

# Upload to blob storage using User-Assigned Managed Identity
echo "=== UPLOADING TO BLOB STORAGE ==="
echo "Logging in with User-Assigned Managed Identity..."
az login --identity --client-id $AZURE_CLIENT_ID --allow-no-subscriptions

echo "Uploading FHIR bundles to blob storage..."
az storage blob upload-batch \
    --account-name $STORAGE_ACCOUNT \
    --destination $CONTAINER_NAME \
    --source /output/fhir \
    --pattern "*.json" \
    --auth-mode login \
    --overwrite

echo "=== UPLOAD COMPLETE ==="
BLOB_COUNT=$(az storage blob list --account-name $STORAGE_ACCOUNT --container-name $CONTAINER_NAME --auth-mode login --query "length(@)" -o tsv)
echo "Total blobs uploaded: $BLOB_COUNT"
