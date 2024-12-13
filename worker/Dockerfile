# Use official Python slim image with Bullseye for stability and security
FROM python:3.11-slim-bullseye

# Prevent Python from writing bytecode and buffering outputs
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    # Set default Redis URL (hardcoded as requested)
    REDIS_URL=rediss://red-ctc7l3tumphs73b2aee0:kzQppr0FwtANbjuQqoo5GiwM53VuVFCz@singapore-redis.render.com:6379

# Set working directory
WORKDIR /app

# Install system dependencies with minimal layers
RUN apt-get update && apt-get install -y \
    build-essential \
    gcc \
    g++ \
    python3-dev \
    wget \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* \
    && rm -rf /var/cache/apt/archives/*

# Create directories for code execution with restricted permissions
RUN mkdir -p /tmp/code_execution \
    && chmod 750 /tmp/code_execution

# Copy application files
COPY worker.py requirements.txt ./

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Create a non-root user for enhanced security
RUN groupadd -r codeexec \
    && useradd -r -g codeexec -s /bin/bash codeexec \
    && chown -R codeexec:codeexec /app /tmp/code_execution

# Switch to non-root user
USER codeexec

# Run the worker script
CMD ["python", "worker.py"]