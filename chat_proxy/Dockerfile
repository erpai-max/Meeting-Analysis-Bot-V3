Use an official, slim Python runtime as the base image for a smaller final size.
FROM python:3.10-slim

Set environment variables for Python and Flask.
PYTHONUNBUFFERED ensures that print statements are sent straight to the logs.
ENV PYTHONUNBUFFERED 1
ENV FLASK_APP=app.py
ENV FLASK_RUN_HOST=0.0.0.0

Create and set the working directory inside the container.
WORKDIR /app

Copy all your project files (app.py, requirements files, etc.) into the container.
COPY . /app/

Install necessary system dependencies.
ffmpeg is required by your analysis scripts if they were to run here.
It's good practice to keep it if any part of your app might process media.
RUN apt-get update && apt-get install -y 

ffmpeg 

&& rm -rf /var/lib/apt/lists/*

Install the Python dependencies using pip.
RUN pip install --upgrade pip

--- FIX ---
This is the key change. We are now installing packages from the dedicated
requirements file for the web app, which includes gunicorn.
RUN pip install -r requirements_app.txt

Expose the port that Render will assign. Gunicorn will listen on this port.
EXPOSE 8080

The command to run your Flask app in production using the Gunicorn web server.
This is more robust and secure than the default Flask development server.
CMD ["gunicorn", "--bind", "0.0.0.0:$PORT", "app:app"]
