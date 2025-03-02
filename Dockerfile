# Use official Python image as the base
FROM python:3.10-slim

# Set the working directory in the container
WORKDIR /app

# Copy only necessary files first to leverage Docker cache
COPY pyproject.toml requirements.txt ./

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the entire application source code
COPY . .

ENV PYTHONUNBUFFERED=1