# Use Python 3.10 slim image as base
FROM python:3.10-slim

# Set working directory inside container
WORKDIR /app

# Copy requirements first for better caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY upload_taxi_data.py .


# Set the command to run the script
ENTRYPOINT ["python", "upload_taxi_data.py"]