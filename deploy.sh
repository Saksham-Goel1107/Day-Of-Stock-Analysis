#!/bin/bash
set -e

# ==============================================================================
# Deployment Script for 'Automating the Day of Stock' Analyzer
# ==============================================================================

echo "🚀 Starting Deployment Process..."

# 1. Pull Latest Code From Origin
echo "📥 Pulling latest code from git repository..."
git pull origin main

# 2. Rebuild And Restart the Docker Image Gracefully
echo "🐳 Rebuilding and starting the docker containers in detached mode..."
docker compose down
docker compose build --no-cache
docker compose up -d

# 3. Validation
echo "✅ Checking container status..."
docker compose ps

echo "================================================================================"
echo "🎉 DEPLOYMENT COMPLETE!"
echo "You can check the cron daemon logs via: docker compose logs -f"
echo "Or check the live output of your python script via: docker exec -it stock-analyzer-cron tail -f /var/log/cron.log"
echo "================================================================================"
