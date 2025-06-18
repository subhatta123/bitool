# Use an official Python runtime as a parent image
FROM python:3.11-slim

# Set non-interactive frontend for package installers, which is safer for automated builds
ENV DEBIAN_FRONTEND=noninteractive

# Set the working directory in the container
WORKDIR /app

# Install system dependencies, including build tools and libraries for pyodbc and psycopg2
# Explicitly add 'unixodbc' runtime which is a dependency for the MS driver
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    gnupg \
    unixodbc \
    unixodbc-dev \
    libpq-dev \
    wkhtmltopdf

# --- Install Microsoft ODBC Driver for SQL Server ---
# This is done in a single RUN command to ensure all layers are built together, avoiding cache issues.
# It adds Microsoft's repository, updates the package list, and installs the driver.
RUN curl -fsSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg \
    && curl -fsSL https://packages.microsoft.com/config/debian/11/prod.list > /etc/apt/sources.list.d/mssql-release.list \
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