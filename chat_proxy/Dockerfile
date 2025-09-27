# Use an official Python runtime as the base image
FROM python:3.10-slim

# Set environment variables to disable buffering and set a default value for the port
ENV PYTHONUNBUFFERED 1
ENV FLASK_APP=app.py
ENV FLASK_RUN_HOST=0.0.0.0
ENV FLASK_RUN_PORT=8080

# Create and set the working directory inside the container
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . /app/

# Install necessary system dependencies (like ffmpeg for audio processing)
RUN apt-get update && apt-get install -y \
    ffmpeg \
    && rm -rf /var/lib/apt/lists/*

# Install the Python dependencies
RUN pip install --upgrade pip
RUN pip install -r requirements_app.txt

# Expose the Flask port
EXPOSE 8080

# Command to run the Flask app
CMD ["flask", "run", "--host=0.0.0.0", "--port=8080"]
