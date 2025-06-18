# Use an official Python runtime as a parent image
FROM python:3.11-slim

# Set the working directory in the container
WORKDIR /app

# Install system dependencies, including build tools and libraries for pyodbc and psycopg2
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    gnupg \
    unixodbc-dev \
    libpq-dev \
    wkhtmltopdf

# --- Install Microsoft ODBC Driver for SQL Server ---
# Add Microsoft's official repository
RUN curl -fsSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg
RUN curl -fsSL https://packages.microsoft.com/config/debian/11/prod.list > /etc/apt/sources.list.d/mssql-release.list

# Update package lists and install the driver
# The ACCEPT_EULA variable is required for silent (unattended) installation
RUN apt-get update && \
    ACCEPT_EULA=Y apt-get install -y msodbcsql17

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