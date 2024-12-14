## Worker Scaling Manager Dockerfile
FROM python:3.9-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    openssh-client \
    docker.io \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Create requirements.txt based on the imports
RUN echo "redis\nparamiko\ndocker\nurllib3" > requirements.txt

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application code
COPY worker_scaling_manager.py .

# Ensure docker socket is accessible
VOLUME /var/run/docker.sock

# Set default environment variables
ENV REDIS_URL=rediss://red-ctc7l3tumphs73b2aee0:kzQppr0FwtANbjuQqoo5GiwM53VuVFCz@singapore-redis.render.com:6379
ENV VM_IPS=192.168.122.121,192.168.122.7
ENV VM_CREDENTIALS='{"192.168.122.121":{"username":"cloud","password":"cloud"}, "192.168.122.7":{"username":"cloud","password":"cloud"}}'
ENV WORKER_IMAGE=kathan07/worker
ENV QUEUE_NAME=code_execution_queue
ENV JOBS_PER_WORKER=5
ENV SCALING_INTERVAL=10

# Set up logging directory
RUN mkdir -p /var/log/worker-scaling-manager

# Make the script executable
RUN chmod +x worker_scaling_manager.py

# Run the scaling manager
CMD ["python", "-u", "worker_scaling_manager.py"]