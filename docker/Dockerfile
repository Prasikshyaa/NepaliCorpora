# Dockerfile for Nepali Corpus Project with Airflow
#
# This Dockerfile builds a custom Airflow image with all project dependencies
# pre-installed and the code baked in. Supervisor just runs docker-compose up.

FROM apache/airflow:2.8.1-python3.10

# Run as root to install packages
USER root

# Install system dependencies
RUN apt-get update && apt-get install -y \
    git \
    curl \
    wget \
    build-essential \
    libssl-dev \
    libffi-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Switch to airflow user
USER airflow

# Copy requirements.txt
COPY requirements.txt /tmp/requirements.txt

# Install Python dependencies
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r /tmp/requirements.txt

# Set working directory
WORKDIR /opt/airflow

# Add project to Python path
ENV PYTHONPATH=/opt/airflow:$PYTHONPATH

# Set timezone
ENV TZ=Asia/Kathmandu

# Default command
CMD ["airflow", "scheduler"]
