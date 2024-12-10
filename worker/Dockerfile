# Use an official Python runtime as a parent image
FROM python:3.9-slim-bullseye

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    gcc \
    g++ \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

# Set the working directory in the container
WORKDIR /app

# Copy the worker script and requirements file
COPY worker.py .
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Create a non-root user for security
RUN groupadd -r codeexec && useradd -r -g codeexec codeexec

# Set appropriate permissions
RUN mkdir -p /tmp/code_execution \
    && chown -R codeexec:codeexec /app /tmp/code_execution \
    && chmod 750 /tmp/code_execution

# Switch to non-root user
USER codeexec

# Run the worker script
CMD ["python", "worker.py"]