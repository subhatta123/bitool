# Use an official Python runtime as a parent image
FROM python:3.11-slim

# Set non-interactive frontend for package installers, which is safer for automated builds
ENV DEBIAN_FRONTEND=noninteractive

# Set the working directory in the container
WORKDIR /app

# --- Install System Dependencies ---
# Step 1: Install prerequisites for adding new repositories and for pyodbc/psycopg2
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    gnupg \
    unixodbc \
    unixodbc-dev \
    libpq-dev \
    wkhtmltopdf \
    apt-transport-https \
    ca-certificates

# --- Install Microsoft ODBC Driver for SQL Server (Robust, Multi-Step Method) ---
# Step 2: Download and install the GPG key
RUN curl -fsSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg

# Step 3: Add the Microsoft repository, explicitly referencing the key
RUN echo "deb [arch=amd64 signed-by=/usr/share/keyrings/microsoft-prod.gpg] https://packages.microsoft.com/debian/11/prod bullseye main" > /etc/apt/sources.list.d/mssql-release.list

# Step 4: Update package lists again and install the driver
RUN apt-get update \
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