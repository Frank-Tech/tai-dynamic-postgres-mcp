# Use official Python image
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install build tools and psycopg binary dependencies
RUN apt-get update && apt-get install -y gcc libpq-dev && rm -rf /var/lib/apt/lists/*

# Copy project files
COPY . .

# Install Python dependencies
RUN pip install --upgrade pip && pip install .

# Set environment variables (override via command line or docker-compose)
ENV PYTHONUNBUFFERED=1

# Default command (can be overridden)
CMD ["tai-postgres-mcp"]
