#!/bin/bash
#
# Nepali Corpus Pipeline - Quick Start Script
# 
# This script performs initial setup and starts the Airflow environment.
# Run this once to get started, then use 'make' commands for daily operations.
#
# Usage:
#   ./setup_and_start.sh
#

set -e  # Exit on error

echo "=========================================="
echo "Nepali Corpus Pipeline - Quick Setup"
echo "=========================================="
echo ""

# ============================================================================
# CHECK PREREQUISITES
# ============================================================================

echo "[1/7] Checking prerequisites..."

# Check Docker
if ! command -v docker &> /dev/null; then
    echo "❌ Docker not found. Please install Docker first:"
    echo "   https://docs.docker.com/get-docker/"
    exit 1
fi
echo "  ✓ Docker found: $(docker --version)"

# Check Docker Compose
if ! command -v docker-compose &> /dev/null; then
    echo "❌ Docker Compose not found. Please install Docker Compose:"
    echo "   https://docs.docker.com/compose/install/"
    exit 1
fi
echo "  ✓ Docker Compose found: $(docker-compose --version)"

# Check Python
if ! command -v python3 &> /dev/null; then
    echo "⚠️  Warning: Python 3 not found. Some manual commands may not work."
    echo "   (Airflow will still run in Docker)"
else
    echo "  ✓ Python 3 found: $(python3 --version)"
fi

echo ""

# ============================================================================
# CREATE ENVIRONMENT FILE
# ============================================================================

echo "[2/7] Setting up environment..."

if [ ! -f .env ]; then
    echo "  Creating .env file from template..."
    cp .env.example .env
    
    # Set AIRFLOW_UID based on platform
    if [[ "$OSTYPE" == "linux-gnu"* ]]; then
        AIRFLOW_UID=$(id -u)
        sed -i "s/AIRFLOW_UID=50000/AIRFLOW_UID=$AIRFLOW_UID/" .env
        echo "  ✓ Set AIRFLOW_UID=$AIRFLOW_UID (Linux)"
    elif [[ "$OSTYPE" == "darwin"* ]]; then
        echo "  ✓ Using default AIRFLOW_UID=50000 (macOS)"
    else
        echo "  ✓ Using default AIRFLOW_UID=50000"
    fi
    
    echo "  ✓ Created .env file"
else
    echo "  ✓ .env file already exists"
fi

echo ""

# ============================================================================
# CREATE DIRECTORIES
# ============================================================================

echo "[3/7] Creating directory structure..."

# Data directories
mkdir -p data/raw/articles
mkdir -p data/raw/scraped
mkdir -p data/raw/crawl_state
mkdir -p data/raw/scrape_state
mkdir -p data/raw/wikipedia/latest
mkdir -p data/processed/wikipedia

# Log directories
mkdir -p logs/ingestion
mkdir -p logs/wikipedia
mkdir -p logs/automation
mkdir -p logs/metrics

# Docker directories
mkdir -p docker/airflow-logs
mkdir -p docker/airflow-plugins

# Backup directory
mkdir -p backups

echo "  ✓ All directories created"
echo ""

# ============================================================================
# SET PERMISSIONS (Linux only)
# ============================================================================

echo "[4/7] Setting permissions..."

if [[ "$OSTYPE" == "linux-gnu"* ]]; then
    echo "  Fixing ownership for Docker volumes..."
    
    # Get current user UID
    CURRENT_UID=$(id -u)
    
    # Fix permissions for data and logs
    chmod -R 755 data/ logs/ docker/ 2>/dev/null || true
    
    echo "  ✓ Permissions set"
else
    echo "  ✓ Skipped (not Linux)"
fi

echo ""

# ============================================================================
# VALIDATE CONFIGURATION FILES
# ============================================================================

echo "[5/7] Validating configuration files..."

if [ -f configs/websites.yaml ]; then
    if command -v python3 &> /dev/null; then
        if python3 -c "import yaml; yaml.safe_load(open('configs/websites.yaml'))" 2>/dev/null; then
            echo "  ✓ configs/websites.yaml is valid"
        else
            echo "  ⚠️  Warning: configs/websites.yaml may have syntax errors"
        fi
    else
        echo "  ⚠️  Skipped validation (Python not found)"
    fi
else
    echo "  ❌ configs/websites.yaml not found!"
    exit 1
fi

if [ -f configs/wikipedia.yaml ]; then
    if command -v python3 &> /dev/null; then
        if python3 -c "import yaml; yaml.safe_load(open('configs/wikipedia.yaml'))" 2>/dev/null; then
            echo "  ✓ configs/wikipedia.yaml is valid"
        else
            echo "  ⚠️  Warning: configs/wikipedia.yaml may have syntax errors"
        fi
    else
        echo "  ⚠️  Skipped validation (Python not found)"
    fi
else
    echo "  ❌ configs/wikipedia.yaml not found!"
    exit 1
fi

echo ""

# ============================================================================
# START DOCKER SERVICES
# ============================================================================

echo "[6/7] Starting Airflow services..."
echo "  This may take 2-3 minutes on first run (downloading images)..."
echo ""

cd docker

# Pull images first (faster startup)
echo "  Pulling Docker images..."
docker-compose pull

# Start services
echo "  Starting services..."
docker-compose up -d

# Wait for services to be healthy
echo ""
echo "  Waiting for services to initialize..."
sleep 10

# Check if services are running
if docker-compose ps | grep -q "Up"; then
    echo "  ✓ Services started successfully"
else
    echo "  ⚠️  Warning: Some services may not be running"
    docker-compose ps
fi

cd ..

echo ""

# ============================================================================
# FINAL INSTRUCTIONS
# ============================================================================

echo "[7/7] Setup complete!"
echo ""
echo "=========================================="
echo "✓ Nepali Corpus Pipeline is Ready!"
echo "=========================================="
echo ""
echo "📊 Airflow UI: http://localhost:8081"
echo "   Username: airflow"
echo "   Password: airflow"
echo ""
echo "⏰ Wait 1-2 minutes for initialization to complete,"
echo "   then refresh the Airflow UI."
echo ""
echo "📋 Next Steps:"
echo "   1. Open http://localhost:8081 in your browser"
echo "   2. Log in with credentials above"
echo "   3. Enable the DAGs you want to run:"
echo "      • nepali_news_crawl_scrape (daily)"
echo "      • nepali_wikipedia_pipeline (monthly)"
echo "   4. Trigger a test run (or wait for schedule)"
echo ""
echo "🔧 Useful Commands:"
echo "   make status      # Check service status"
echo "   make logs        # View logs"
echo "   make stop        # Stop services"
echo "   make start       # Start services"
echo "   make help        # Show all commands"
echo ""
echo "📖 Documentation:"
echo "   See AUTOMATION_README.md for complete guide"
echo ""
echo "🐛 Troubleshooting:"
echo "   If services aren't running, check:"
echo "   • Docker is running: docker ps"
echo "   • Service logs: cd docker && docker-compose logs"
echo "   • Port 8081 is free: netstat -an | grep 8081"
echo ""
echo "Happy corpus building! 🇳🇵"
echo ""