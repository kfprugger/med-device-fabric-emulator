FROM mcr.microsoft.com/cbl-mariner/base/python:3
ENV PYTHONUNBUFFERED=1
RUN ln -sf /usr/bin/python3 /usr/bin/python
RUN pip install azure-eventhub azure-identity azure-keyvault-secrets
COPY emulator.py /app/emulator.py
WORKDIR /app
CMD ["python", "-u", "emulator.py"]
