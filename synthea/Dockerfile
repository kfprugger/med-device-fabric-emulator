FROM eclipse-temurin:17-jre

# Install wget, unzip, and Azure CLI
RUN apt-get update && apt-get install -y wget unzip curl apt-transport-https lsb-release gnupg ca-certificates \
    && curl -sL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor > /etc/apt/trusted.gpg.d/microsoft.gpg \
    && echo "deb [arch=amd64] https://packages.microsoft.com/repos/azure-cli/ $(lsb_release -cs) main" > /etc/apt/sources.list.d/azure-cli.list \
    && apt-get update && apt-get install -y azure-cli \
    && rm -rf /var/lib/apt/lists/*

# Download and extract Synthea
WORKDIR /synthea
RUN wget -q https://github.com/synthetichealth/synthea/releases/download/master-branch-latest/synthea-with-dependencies.jar

# Copy configuration files
COPY synthea.properties /synthea/synthea.properties
COPY atlanta_providers.json /synthea/atlanta_providers.json
COPY run-synthea.sh /synthea/run-synthea.sh

RUN chmod +x /synthea/run-synthea.sh

# Create output directory
RUN mkdir -p /output

# Set environment variables
ENV PATIENT_COUNT=10000
ENV JAVA_OPTS="-Xms4g -Xmx7g"

ENTRYPOINT ["/synthea/run-synthea.sh"]
