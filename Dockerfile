FROM python:3.9-slim
ENV PYTHONUNBUFFERED=1
RUN pip install azure-eventhub azure-identity azure-keyvault-secrets
COPY emulator.py /app/emulator.py
WORKDIR /app
CMD ["python", "-u", "emulator.py"]
