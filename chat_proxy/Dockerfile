Use an official, slim Python runtime as the base image for a smaller final size.
FROM python:3.10-slim

Set environment variables for Python and Flask. This is good practice for production.
ENV PYTHONUNBUFFERED=1
ENV FLASK_APP=app.py

Create and set the working directory inside the container.
WORKDIR /app

Copy all files from the 'chat_proxy' folder (the build context) into the container's /app directory.
COPY . /app/

Install the Python dependencies from the app-specific requirements file.
This ensures only the packages needed for the chatbot are installed.
RUN pip install --no-cache-dir --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

Expose the port that Render will assign to the container. Gunicorn will listen on this port.
EXPOSE 8080

The command to run your Flask app in production using the Gunicorn web server.
This is the standard and recommended way to run Flask
