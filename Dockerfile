# Use Python 3.10 slim image as base
FROM python:3.10-slim

# Set working directory inside container
WORKDIR /app

# Install Python dependencies
RUN pip install google-cloud-storage pandas requests pyarrow

# Copy application code
COPY upload_data.py .


# Set the command to run the script
ENTRYPOINT ["python", "upload_data.py"]