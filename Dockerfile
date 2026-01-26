FROM python:3.12-slim

WORKDIR /app

# Install system dependencies for snappy compression
RUN apt-get update && apt-get install -y \
    libsnappy-dev \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY analytics/ ./analytics/

# Run as non-root user
RUN useradd -m appuser
USER appuser

CMD ["python", "-m", "analytics.main"]
