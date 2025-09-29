@"
# Service Gateway - AI Customer Service Database Integration

Secure FastAPI gateway connecting Azure MCP customer service system to AS400 database.

## Features
- Secure customer data access
- Authentication & authorization
- Health monitoring & logging
- Circuit breaker protection
- Windows service deployment

## Quick Start
``````powershell
# Install service
.\install_service.ps1

# Start service
Start-Service ServiceGateway

# Check health
curl http://localhost:8080/health