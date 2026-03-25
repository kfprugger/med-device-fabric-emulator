FROM mcr.microsoft.com/cbl-mariner/base/python:3
# CBL-Mariner uses python3, create symlink for compatibility
RUN ln -sf /usr/bin/python3 /usr/bin/python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . /app
WORKDIR /app
CMD ["python", "-u", "load_dicom.py"]
