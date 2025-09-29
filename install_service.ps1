# Create C:\service-gateway\install_service.ps1
# Run as Administrator
$serviceName = "ServiceGateway"
$serviceDisplayName = "Service Gateway API"
$serviceDescription = "Customer Service Database Gateway for AS400 Integration"
$scriptPath = "C:\service-gateway\start_service.bat"
$nssmPath = "C:\tools\nssm-2.24\win64\nssm.exe"

# Install service
& $nssmPath install $serviceName $scriptPath

# Configure service parameters
& $nssmPath set $serviceName DisplayName $serviceDisplayName
& $nssmPath set $serviceName Description $serviceDescription
& $nssmPath set $serviceName Start SERVICE_AUTO_START
& $nssmPath set $serviceName AppDirectory "C:\service-gateway"
& $nssmPath set $serviceName AppStdout "C:\service-gateway\logs\service.log"
& $nssmPath set $serviceName AppStderr "C:\service-gateway\logs\service_error.log"
& $nssmPath set $serviceName AppRotateFiles 1
& $nssmPath set $serviceName AppRotateOnline 1
& $nssmPath set $serviceName AppRotateBytes 10485760  # 10MB

# Set restart policy
& $nssmPath set $serviceName AppExit Default Restart
& $nssmPath set $serviceName AppRestartDelay 30000  # 30 seconds

Write-Host "Service installed successfully!"
Write-Host "Start with: Start-Service $serviceName"
Write-Host "Stop with: Stop-Service $serviceName"