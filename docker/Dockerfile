# Dockerfile for Airflow with AWS EMR Support
# Optimized for running on EC2 with boto3 EMR integration

FROM apache/airflow:2.8.0-python3.11

# Switch to root for system packages
USER root

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    unzip \
    vim \
    && rm -rf /var/lib/apt/lists/*

# Install AWS CLI v2
RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip" \
    && unzip awscliv2.zip \
    && ./aws/install \
    && rm -rf aws awscliv2.zip

# Switch back to airflow user
USER airflow

# Copy requirements and install Python dependencies
COPY requirements.txt /opt/airflow/requirements.txt
RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt

# Copy DAGs and helper scripts
COPY airflow/*.py /opt/airflow/dags/

# Set environment variables
ENV AIRFLOW__CORE__LOAD_EXAMPLES=False
ENV AIRFLOW__CORE__DAGS_FOLDER=/opt/airflow/dags
ENV AIRFLOW__WEBSERVER__EXPOSE_CONFIG=True

# Default command (overridden by docker-compose)
CMD ["airflow", "standalone"]
