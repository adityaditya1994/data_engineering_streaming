# Dockerfile for Production Airflow
# Optimized for orchestration only (submits to EMR)

FROM apache/airflow:2.8.0-python3.11

USER root

# Install system dependencies (minimal)
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    unzip \
    vim \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Copy requirements and install Python dependencies
COPY requirements.txt /opt/airflow/requirements.txt
RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt

# Copy DAGs
COPY airflow/*.py /opt/airflow/dags/

# Set environment variables
ENV AIRFLOW__CORE__LOAD_EXAMPLES=False
ENV AIRFLOW__CORE__DAGS_FOLDER=/opt/airflow/dags
ENV AIRFLOW__WEBSERVER__EXPOSE_CONFIG=True
