#!/bin/bash
set -e

echo "🚀 Starting Jupyter Notebook Environment..."

# Ensure .env exists (created by start_local_airflow.sh, but check anyway)
if [ ! -f docker_notebook/.env ]; then
    echo "⚠️  docker_notebook/.env not found. Running setup..."
    ./start_local_airflow.sh
fi

cd docker_notebook
docker-compose up -d

echo "✅ Notebook Server Running!"
echo "👉 Access at: http://localhost:8888"
echo "   (Pre-configured with PySpark + S3 Access)"
echo ""
echo "📂 Notebooks mounted at: notebooks/"
echo "📂 Scripts mounted at: EMR/"
