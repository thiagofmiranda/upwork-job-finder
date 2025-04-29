FROM python:3.11-slim

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

# Install system dependencies
RUN apt-get update && apt-get install -y \
    wget \
    unzip \
    libnss3 \
    libxss1 \
    libasound2 \
    libatk1.0-0 \
    libatk-bridge2.0-0 \
    libgtk-3-0 \
    libgbm-dev \
    libxshmfence-dev \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt /app/requirements.txt
RUN pip install --upgrade pip && pip install -r /app/requirements.txt

# Install Playwright and its dependencies
RUN pip install playwright && playwright install --with-deps chromium

# Copy application code
COPY app/ /app/

# Set working directory
WORKDIR /app/

# Define the command to run the application
CMD ["python", "bot.py"]