#!/usr/bin/env python3
"""
Port Availability Checker for VPS Deployment
==========================================
Checks which ports are available and suggests safe alternatives
"""

import socket
import subprocess
import sys

def check_port_available(port):
    """Check if a port is available"""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.settimeout(1)
            result = sock.connect_ex(('localhost', port))
            return result != 0  # Port is available if connection fails
    except Exception:
        return True

def get_listening_ports():
    """Get list of ports currently in use"""
    try:
        result = subprocess.run(['netstat', '-tlnp'], capture_output=True, text=True)
        lines = result.stdout.split('\n')
        ports = []
        for line in lines:
            if 'LISTEN' in line:
                parts = line.split()
                if len(parts) >= 4:
                    address = parts[3]
                    if ':' in address:
                        port = address.split(':')[-1]
                        if port.isdigit():
                            ports.append(int(port))
        return sorted(set(ports))
    except Exception:
        return []

def find_safe_ports(preferred_ports=[8001, 8002, 8003, 8080, 8081]):
    """Find safe ports to use"""
    print("🔍 Checking port availability...")
    print("=" * 40)
    
    listening_ports = get_listening_ports()
    if listening_ports:
        print(f"📡 Ports currently in use: {', '.join(map(str, listening_ports))}")
    else:
        print("📡 Could not detect listening ports (netstat might not be available)")
    
    print("\n🎯 Checking preferred ports:")
    available_ports = []
    
    for port in preferred_ports:
        is_available = check_port_available(port)
        status = "✅ Available" if is_available else "❌ In use"
        print(f"  Port {port}: {status}")
        if is_available:
            available_ports.append(port)
    
    if available_ports:
        recommended_port = available_ports[0]
        print(f"\n🎉 Recommended port: {recommended_port}")
        return recommended_port
    else:
        print("\n⚠️  All preferred ports are in use")
        # Try to find an available port in range 8000-9000
        for port in range(8000, 9001):
            if check_port_available(port):
                print(f"🎉 Alternative port found: {port}")
                return port
        
        print("❌ No available ports found in range 8000-9000")
        return None

def update_config_port(port):
    """Update the configuration with the recommended port"""
    try:
        # Update .env.production
        with open('.env.production', 'r') as f:
            content = f.read()
        
        # Replace the port
        import re
        content = re.sub(r'API_PORT=\d+', f'API_PORT={port}', content)
        
        with open('.env.production', 'w') as f:
            f.write(content)
        
        print(f"✅ Updated .env.production with port {port}")
        
        # Update deploy.py
        with open('deploy.py', 'r') as f:
            deploy_content = f.read()
        
        # Replace port references in deploy.py
        deploy_content = re.sub(r'http://178\.32\.191\.152:\d+', f'http://178.32.191.152:{port}', deploy_content)
        deploy_content = re.sub(r'"--port", "\d+"', f'"--port", "{port}"', deploy_content)
        
        with open('deploy.py', 'w') as f:
            f.write(deploy_content)
            
        print(f"✅ Updated deploy.py with port {port}")
        return True
        
    except Exception as e:
        print(f"❌ Failed to update configuration: {e}")
        return False

def main():
    """Main function"""
    print("🏗️  ETL Pipeline Port Checker")
    print("=" * 40)
    
    recommended_port = find_safe_ports()
    
    if recommended_port:
        print(f"\n🔧 Configuration Summary:")
        print(f"   Backend will run on: http://178.32.191.152:{recommended_port}")
        print(f"   API Docs available at: http://178.32.191.152:{recommended_port}/docs")
        print(f"   Frontend should connect to: http://178.32.191.152:{recommended_port}")
        
        # Ask if user wants to update config
        response = input(f"\n❓ Update configuration to use port {recommended_port}? (y/n): ")
        if response.lower() in ['y', 'yes']:
            if update_config_port(recommended_port):
                print("✅ Configuration updated successfully!")
            else:
                print("❌ Failed to update configuration")
        else:
            print("ℹ️  Configuration not updated. You can manually change API_PORT in .env.production")
    else:
        print("\n❌ Could not find an available port")
        print("   Please manually specify a port in .env.production")

if __name__ == "__main__":
    main()