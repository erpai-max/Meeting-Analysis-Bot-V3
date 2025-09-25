# Use an official Python runtime as a parent image
FROM python:3.10-slim

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code (just app.py in this case)
COPY app.py .

# Run the application with Gunicorn
CMD ["gunicorn", "--bind", "0.0.0.0:8080", "app:app"]
