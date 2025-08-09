#!/usr/bin/env python3
"""
VPS Deployment Script for ETL Pipeline Backend
============================================
Production deployment script for running on VPS server 178.32.191.152
"""

import subprocess
import sys
import os
from pathlib import Path

def check_requirements():
    """Check if all required dependencies are installed"""
    try:
        import uvicorn
        import fastapi
        import sqlalchemy
        print("✅ Core dependencies found")
    except ImportError as e:
        print(f"❌ Missing dependency: {e}")
        print("Run: pip install -r requirements.txt")
        return False
    return True

def setup_environment():
    """Set up production environment"""
    env_file = Path(".env.production")
    if not env_file.exists():
        print("❌ .env.production file not found")
        return False
    
    # Copy production env to .env
    subprocess.run(["cp", ".env.production", ".env"], check=True)
    print("✅ Production environment configured")
    return True

def check_database():
    """Check database connectivity"""
    try:
        from config import get_config
        from etl.load import connect_to_postgres
        
        config = get_config()
        engine = connect_to_postgres()
        
        with engine.connect() as conn:
            result = conn.execute("SELECT 1")
            print("✅ Database connection successful")
            return True
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        return False

def start_server():
    """Start the production server"""
    print("🚀 Starting ETL Pipeline backend server...")
    print("📡 Server will be available at: http://178.32.191.152:8001")
    print("📖 API Documentation: http://178.32.191.152:8001/docs")
    
    # Start uvicorn server
    subprocess.run([
        "uvicorn", "main:app",
        "--host", "0.0.0.0",
        "--port", "8001", 
        "--workers", "4",
        "--log-level", "info"
    ])

def main():
    """Main deployment function"""
    print("🏗️  ETL Pipeline VPS Deployment")
    print("=" * 40)
    
    # Change to backend directory
    os.chdir(Path(__file__).parent)
    
    if not check_requirements():
        sys.exit(1)
        
    if not setup_environment():
        sys.exit(1)
        
    if not check_database():
        print("⚠️  Database connection failed - server may have issues")
        
    start_server()

if __name__ == "__main__":
    main()