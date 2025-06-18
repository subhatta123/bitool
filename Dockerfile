# Use an official Python runtime as a parent image
FROM python:3.11-slim

# Set non-interactive frontend for package installers, which is safer for automated builds
ENV DEBIAN_FRONTEND=noninteractive

# Set the working directory in the container
WORKDIR /app

# --- Install System Dependencies ---
# Install prerequisites for adding new repositories and for pyodbc/psycopg2
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    gnupg \
    unixodbc \
    unixodbc-dev \
    libpq-dev \
    wkhtmltopdf \
    apt-transport-https \
    ca-certificates

# --- Install Microsoft ODBC Driver for SQL Server (Robust Method) ---
# This is done in a single RUN command to ensure all layers are built together, avoiding cache issues.
# It downloads the GPG key, adds it to the trusted keyring, configures the repository with the 'signed-by' attribute,
# updates the package list, and installs the driver. This is the most reliable method.
RUN curl -fsSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg \
    && echo "deb [arch=amd64 signed-by=/usr/share/keyrings/microsoft-prod.gpg] https://packages.microsoft.com/debian/11/prod bullseye main" > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y --no-install-recommends msodbcsql17

# Clean up apt-get files to reduce final image size
RUN rm -rf /var/lib/apt/lists/*

# Copy the requirements file into the container
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application source code into the container
COPY . .

# Expose the port that Streamlit will run on
EXPOSE 8501

# Define the command to run the application
CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.enableCORS=false"] 