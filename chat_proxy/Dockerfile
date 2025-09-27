# Use an official, slim Python runtime as the base image
FROM python:3.10-slim

# Set environment variables for Python and Flask
ENV PYTHONUNBUFFERED 1
ENV FLASK_APP=app.py
ENV FLASK_RUN_HOST=0.0.0.0

# Create and set the working directory inside the container
WORKDIR /app

# Copy all your project files into the container
COPY . /app/

# Install the Python dependencies
RUN pip install --upgrade pip

# --- FIX ---
# This is the key change. We are now installing packages from the dedicated
# requirements file for the web app, which includes gunicorn.
RUN pip install -r requirements_app.txt

# Expose the port that Render will assign
EXPOSE 8080

# The command to run your Flask app in production using the Gunicorn web server
CMD ["gunicorn", "--bind", "0.0.0.0:$PORT", "app:app"]
