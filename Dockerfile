FROM python:3.11-slim

# Set timezone dynamically if requested, otherwise default to UTC
ENV TZ=UTC

# Update OS and install Cron
RUN apt-get update && apt-get install -y cron && rm -rf /var/lib/apt/lists/*

# Set working directory inside container
WORKDIR /app

# Copy application dependencies and configurations
COPY requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt

# Copy source repository
COPY . /app/

# Setup Cronjob File into the container's cron.d
COPY cronjob /etc/cron.d/stock-analyzer-cron

# Give execution rights and apply cron job
RUN chmod 0644 /etc/cron.d/stock-analyzer-cron \
    && crontab /etc/cron.d/stock-analyzer-cron

# Optional: Ensure our execution script runs securely
RUN touch /var/log/cron.log

# Command starts the cron daemon in the foreground, and streams the log
CMD cron && echo "Cron daemon started. Application running in background." && tail -f /var/log/cron.log
