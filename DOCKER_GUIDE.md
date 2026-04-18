# Docker Containerization Guide - Black Gold App

## 📋 Overview
This guide walks you through containerizing and running the Black Gold (Crude Flow Terminal) application using Docker and Docker Compose.

---

## 📦 Files Created

1. **Dockerfile** - Defines the container image for the Streamlit app
2. **docker-compose.yml** - Orchestrates multi-container setup (app + Redis)
3. **.dockerignore** - Optimizes build by excluding unnecessary files
4. **.streamlit/config.toml** - Streamlit configuration for container environment
5. **.env.example** - Environment variables template

---

## 🚀 Quick Start (Recommended - Docker Compose)

### Prerequisites
- Install [Docker Desktop](https://www.docker.com/products/docker-desktop) for Windows
- Docker Desktop includes both `docker` and `docker-compose`

### Step 1: Environment Setup
```bash
# Copy the example env file
cp .env.example .env

# Edit .env if needed (optional - defaults work fine)
# REDIS_HOST=redis
# REDIS_PORT=6379
```

### Step 2: Build and Run
```bash
# Build images and start all services
docker-compose up -d

# Or with rebuild (useful after code changes)
docker-compose up -d --build
```

### Step 3: Access Your App
- Open browser and navigate to: **http://localhost:8501**
- You should see the Crude Flow Terminal UI

### Step 4: Monitor Logs
```bash
# View real-time logs
docker-compose logs -f app

# View Redis logs
docker-compose logs -f redis

# View all services
docker-compose logs -f
```

---

## 🛑 Stopping the Containers

```bash
# Stop all services (data is preserved)
docker-compose stop

# Stop and remove containers
docker-compose down

# Stop, remove containers, AND volumes (WARNING: data loss)
docker-compose down -v
```

---

## 🏗️ Manual Docker Build (Without Compose)

### Build the Image
```bash
docker build -t black_gold:latest .
```

### Run the Container
```bash
# Basic run
docker run -p 8501:8501 black_gold:latest

# With environment variables and volume mounting
docker run -p 8501:8501 \
  -e REDIS_HOST=localhost \
  -v C:\Users\Naba.Mantu\Documents\Projects\black_gold:/app \
  black_gold:latest
```

---

## 📊 Best Practices

### For Development
```bash
# Use docker-compose with volume mounting (changes live-reload)
docker-compose up

# Edit your Python files locally, changes reflect in container immediately
```

### For Production
```bash
# Use multi-stage builds (update Dockerfile for production)
# Remove volume mounts
# Use environment-specific .env files
docker-compose -f docker-compose.yml -f docker-compose.prod.yml up -d
```

---

## ⚙️ Configuration

### Streamlit Config (.streamlit/config.toml)
- Auto-loaded by Streamlit in container
- Contains theme, server, and logger settings
- Modify and rebuild for changes: `docker-compose up -d --build`

### Environment Variables
1. Copy `.env.example` to `.env`
2. Edit `.env` as needed
3. Docker-compose automatically loads from `.env`

---

## 🔍 Troubleshooting

### Container won't start
```bash
# Check logs
docker-compose logs app

# Common issues:
# - Port 8501 already in use: docker-compose down && docker-compose up
# - Insufficient disk space: docker system prune
```

### Can't access app at localhost:8501
```bash
# Restart Redis dependency first
docker-compose restart redis
docker-compose restart app

# Check container is running
docker-compose ps

# Check port binding
docker port black_gold_app
```

### DuckDB file not updating
```bash
# Ensure volume mount is correct in docker-compose.yml
# Check permissions on local ./data directory
mkdir -p ./data
chmod 755 ./data
```

### Redis connection issues
```bash
# Verify Redis is running
docker-compose ps

# Test Redis connection from app container
docker-compose exec app redis-cli -h redis ping
# Should return: PONG
```

---

## 📈 Scaling & Deployment

### Push to Docker Hub (optional)
```bash
# Tag image
docker tag black_gold:latest yourusername/black_gold:latest

# Login and push
docker login
docker push yourusername/black_gold:latest
```

### Deploy to Cloud
- **Azure Container Instances**: `az container create ...`
- **Azure App Service**: Docker container as deployment option
- **Kubernetes**: Use docker-compose to scaffold Helm charts

---

## 🧹 Cleanup

```bash
# Remove stopped containers
docker container prune

# Remove unused images
docker image prune

# Remove all unused objects (containers, images, networks, volumes)
docker system prune -a --volumes
```

---

## 📝 Next Steps

1. ✅ Run `docker-compose up -d`
2. ✅ Access http://localhost:8501
3. ✅ Test data ingestion and terminal features
4. ✅ Configure `.env` for your specific setup
5. ✅ Consider production deployment strategy

---

## 🆘 Need Help?

Check Docker logs:
```bash
docker-compose logs -f
```

Or verify each service:
```bash
docker-compose ps
docker-compose exec app streamlit --version
docker-compose exec redis redis-cli info
```
