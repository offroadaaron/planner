#!/usr/bin/env bash
set -e

echo "=== Calendar Planner — Install ==="

# Check Docker
if ! command -v docker &>/dev/null; then
  echo "ERROR: Docker is not installed."
  echo "  Install from https://docs.docker.com/get-docker/ then re-run this script."
  exit 1
fi

# Check Docker Compose (plugin form)
if ! docker compose version &>/dev/null 2>&1; then
  echo "ERROR: Docker Compose is not available."
  echo "  It is bundled with Docker Desktop, or install the plugin:"
  echo "  https://docs.docker.com/compose/install/"
  exit 1
fi

# Copy .env if it doesn't exist
if [ ! -f .env ]; then
  cp .env.example .env
  echo "Created .env from .env.example (edit if needed)."
fi

echo "Building and starting containers..."
docker compose up --build -d

echo ""
echo "Done! App is running at http://localhost:8001"
echo "Health check: http://localhost:8001/health"
echo ""
echo "To stop: docker compose down"
echo "To view logs: docker compose logs -f web"
