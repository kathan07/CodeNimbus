# Use an official Python runtime as a parent image
FROM python:3.9-slim-bullseye

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

# Set the working directory in the container
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Create uploads directory
RUN mkdir -p /app/uploads

# Copy requirements file
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the FastAPI application code
COPY main.py .

# Create a non-root user for security
RUN groupadd -r fastapi && useradd -r -g fastapi fastapi

# Set appropriate permissions
RUN chown -R fastapi:fastapi /app /app/uploads \
    && chmod 750 /app/uploads

# Switch to non-root user
USER fastapi

# Expose the port the app runs on
EXPOSE 8000

# Use environment variables for configuration
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]