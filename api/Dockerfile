# Use official Python slim image
FROM python:3.9-slim-bullseye

# Prevent Python from writing bytecode and buffer outputs
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# Set hardcoded environment variables
ENV SUPABASE_URL=https://yoisrszpgitgvciyzxkz.supabase.co \
    SUPABASE_KEY=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InlvaXNyc3pwZ2l0Z3ZjaXl6eGt6Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTczNDA2ODk2OCwiZXhwIjoyMDQ5NjQ0OTY4fQ.ERE78CKmsNfFG3uQ1I-jcmBzKxvcu3Xs2d8j6Qofi60 \
    REDIS_URL=rediss://red-ctc7l3tumphs73b2aee0:kzQppr0FwtANbjuQqoo5GiwM53VuVFCz@singapore-redis.render.com:6379
# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* \
    && rm -rf /var/cache/apt/archives/*

# Copy project files
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY main.py .

# Create non-root user
RUN groupadd -r fastapi && \
    useradd -r -g fastapi -s /bin/bash fastapi

# Set ownership and permissions
RUN chown -R fastapi:fastapi /app

# Switch to non-root user
USER fastapi

# Expose application port
EXPOSE 8000

# Run the application
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]