# ETL Pipeline Deployment Guide

## Architecture Overview

- **Frontend**: React app deployed on Vercel (https://etl-pipeline-snowy.vercel.app)
- **Backend**: FastAPI server deployed on VPS (178.32.191.152:8001)
- **Database**: PostgreSQL on VPS (localhost:5432)

## VPS Backend Deployment

### 1. Prerequisites
```bash
# Ensure Python 3.8+ and pip are installed
python3 --version
pip3 --version

# Install PostgreSQL if not already installed
sudo apt update
sudo apt install postgresql postgresql-contrib
```

### 2. Deploy Backend
```bash
# Navigate to backend directory
cd /opt/ETL-Pipeline/backend

# Install dependencies
pip3 install -r requirements.txt

# Check available ports (avoid conflicts)
python3 check_ports.py

# Deploy with production settings
python3 deploy.py
```

### 3. Alternative Manual Deployment
```bash
# Copy production environment
cp .env.production .env

# Start server manually
uvicorn main:app --host 0.0.0.0 --port 8001 --workers 4
```

### 4. Service Setup (Optional)
Create a systemd service for automatic startup:

```bash
# Create service file
sudo nano /etc/systemd/system/etl-pipeline.service
```

Service file content:
```ini
[Unit]
Description=ETL Pipeline API Server
After=network.target

[Service]
Type=simple
User=your-username
WorkingDirectory=/opt/ETL-Pipeline/backend
Environment=PATH=/usr/local/bin:/usr/bin:/bin
ExecStart=/usr/local/bin/uvicorn main:app --host 0.0.0.0 --port 8001 --workers 4
Restart=always

[Install]
WantedBy=multi-user.target
```

```bash
# Enable and start service
sudo systemctl daemon-reload
sudo systemctl enable etl-pipeline
sudo systemctl start etl-pipeline
```

## Vercel Frontend Deployment

### 1. Environment Variables
In Vercel dashboard, set these environment variables:
- `VITE_API_BASE_URL`: `http://178.32.191.152:8001`
- `VITE_WS_URL`: `ws://178.32.191.152:8001/ws`

### 2. Build Settings
- Build Command: `npm run build`
- Output Directory: `dist`
- Node Version: 18.x

## Database Configuration

### Default Settings
- Host: localhost
- Port: 5432
- Database: student1
- Username: student1
- Password: learnDB

### Update Settings
Edit `/opt/ETL-Pipeline/backend/.env.production` with your actual database credentials.

## Security Considerations

1. **Firewall**: Ensure port 8001 is open on your VPS
```bash
sudo ufw allow 8001
```

2. **SSL**: Consider setting up SSL/TLS for production
3. **Environment Variables**: Update SECRET_KEY in production
4. **CORS**: Remove wildcard (*) from CORS origins in production

## Monitoring & Logs

### Check Backend Status
```bash
# Health check
curl http://178.32.191.152:8001/health

# API documentation
curl http://178.32.191.152:8001/docs
```

### View Logs
```bash
# If using systemd service
sudo journalctl -u etl-pipeline -f

# If running manually
tail -f /var/log/etl-pipeline.log
```

## Troubleshooting

### Common Issues

1. **Port Conflicts**
   - Run `python3 check_ports.py` to find available ports
   - Update API_PORT in .env.production

2. **Database Connection Failed**
   - Check PostgreSQL is running: `sudo systemctl status postgresql`
   - Verify credentials in .env.production

3. **CORS Errors**
   - Ensure Vercel domain is in CORS origins
   - Check frontend VITE_API_BASE_URL points to correct VPS address

4. **WebSocket Connection Issues**
   - Verify WS_URL in frontend configuration
   - Check if WebSocket port is accessible

### Testing Connection
```bash
# Test API endpoint
curl -X GET http://178.32.191.152:8001/

# Test health check
curl -X GET http://178.32.191.152:8001/health

# Test WebSocket (using websocat or browser dev tools)
```

## Performance Optimization

- **Backend**: Use production ASGI server (uvicorn with workers)
- **Database**: Configure connection pooling
- **Frontend**: Enable Vercel's edge caching
- **Monitoring**: Set up application performance monitoring

## Backup & Recovery

1. **Database Backup**
```bash
pg_dump student1 > etl_backup.sql
```

2. **Code Backup**
```bash
# Repository is already on GitHub
git push origin main
```