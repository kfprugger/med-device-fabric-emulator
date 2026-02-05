FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application files
COPY load_fhir.py .
COPY device_registry.json .
COPY atlanta_providers.json .

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV FHIR_SERVICE_URL=""
ENV SYNTHEA_OUTPUT_PATH="/synthea-output/fhir"
ENV DEVICE_COUNT="100"

CMD ["python", "-u", "load_fhir.py"]
