# black_gold
Autonomous AI driven crude oil insights and market analyser.

# Ensure Docker Desktop is installed for Windows
# Download from: https://www.docker.com/products/docker-desktop

### Start App

`cd C:\Users\Naba.Mantu\Documents\Projects\black_gold`
`docker-compose up -d`

Open browser → http://localhost:8501

### Complete Step-by-Step Process
#### 1. Navigate to project
`cd C:\Users\Naba.Mantu\Documents\Projects\black_gold`

#### 2. Start all services
`docker-compose up -d`

#### 3. Check status
`docker-compose ps`

#### 4. View logs (real-time)
`docker-compose logs -f`

#### 5. Access app
`# Open: http://localhost:8501`

#### 6. Make code changes locally - they auto-reload
# Edit app.py, requirements.txt, etc. - no rebuild needed!

#### 7. Stop services
`docker-compose down`

#### 8. Rebuild if you change requirements.txt
`docker-compose up -d --build`