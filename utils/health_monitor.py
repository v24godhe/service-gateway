import time
import psutil
import asyncio
from typing import Dict, Any, List
from datetime import datetime, timedelta
from database.styr_connector import StyrDatabaseConnector
from utils.fallback import fallback_manager

class HealthMonitor:
    def __init__(self):
        self.start_time = time.time()
        self.health_history = []
        self.max_history = 100
    
    async def comprehensive_health_check(self, db_connector: StyrDatabaseConnector) -> Dict[str, Any]:
        """Perform comprehensive system health check"""
        health_data = {
            "timestamp": datetime.now().isoformat(),
            "overall_status": "healthy",
            "components": {},
            "system_metrics": await self._get_system_metrics(),
            "uptime": self._get_uptime()
        }
        
        # Database health
        db_health = await self._check_database_health(db_connector)
        health_data["components"]["database"] = db_health
        
        # Service health
        service_health = await self._check_service_health()
        health_data["components"]["service"] = service_health
        
        # Circuit breaker status
        circuit_health = self._check_circuit_breaker_health()
        health_data["components"]["circuit_breaker"] = circuit_health
        
        # Memory health
        memory_health = self._check_memory_health()
        health_data["components"]["memory"] = memory_health
        
        # Disk health
        disk_health = self._check_disk_health()
        health_data["components"]["disk"] = disk_health
        
        # Determine overall status
        health_data["overall_status"] = self._calculate_overall_status(health_data["components"])
        
        # Store in history
        self._store_health_history(health_data)
        
        return health_data
    
    async def _check_database_health(self, db_connector: StyrDatabaseConnector) -> Dict[str, Any]:
        """Check AS400 database connectivity and performance"""
        start_time = time.time()
        
        try:
            # Test basic connection
            is_healthy = await db_connector.health_check()
            response_time = (time.time() - start_time) * 1000
            
            if is_healthy:
                # Test simple query performance
                query_start = time.time()
                await db_connector.execute_query("SELECT 1 FROM SYSIBM.SYSDUMMY1")
                query_time = (time.time() - query_start) * 1000
                
                return {
                    "status": "healthy",
                    "connection": "active",
                    "response_time_ms": round(response_time, 2),
                    "query_time_ms": round(query_time, 2),
                    "last_check": datetime.now().isoformat(),
                    "details": "Database connection and queries working normally"
                }
            else:
                return {
                    "status": "unhealthy",
                    "connection": "failed",
                    "response_time_ms": round(response_time, 2),
                    "last_check": datetime.now().isoformat(),
                    "details": "Database connection failed"
                }
                
        except Exception as e:
            return {
                "status": "unhealthy",
                "connection": "error",
                "response_time_ms": round((time.time() - start_time) * 1000, 2),
                "last_check": datetime.now().isoformat(),
                "error": str(e),
                "details": "Database health check failed with exception"
            }
    
    async def _check_service_health(self) -> Dict[str, Any]:
        """Check FastAPI service health"""
        return {
            "status": "healthy",
            "uptime_seconds": round(time.time() - self.start_time, 2),
            "uptime_readable": self._format_uptime(time.time() - self.start_time),
            "process_id": psutil.Process().pid,
            "threads": psutil.Process().num_threads(),
            "last_check": datetime.now().isoformat(),
            "details": "FastAPI service running normally"
        }
    
    def _check_circuit_breaker_health(self) -> Dict[str, Any]:
        """Check circuit breaker status"""
        status = fallback_manager.get_status()
        
        if status["circuit_breaker_open"]:
            return {
                "status": "degraded",
                "circuit_state": "open",
                "failure_count": status["failure_count"],
                "last_failure": datetime.fromtimestamp(status["last_failure"]).isoformat() if status["last_failure"] else None,
                "details": "Circuit breaker is open - database calls are being blocked"
            }
        else:
            return {
                "status": "healthy",
                "circuit_state": "closed",
                "failure_count": status["failure_count"],
                "details": "Circuit breaker is healthy"
            }
    
    def _check_memory_health(self) -> Dict[str, Any]:
        """Check memory usage"""
        process = psutil.Process()
        memory_info = process.memory_info()
        memory_percent = process.memory_percent()
        
        status = "healthy"
        if memory_percent > 80:
            status = "critical"
        elif memory_percent > 60:
            status = "warning"
        
        return {
            "status": status,
            "memory_usage_mb": round(memory_info.rss / 1024 / 1024, 2),
            "memory_percent": round(memory_percent, 2),
            "virtual_memory_mb": round(memory_info.vms / 1024 / 1024, 2),
            "details": f"Memory usage at {memory_percent:.1f}%"
        }
    
    def _check_disk_health(self) -> Dict[str, Any]:
        """Check disk space"""
        try:
            disk_usage = psutil.disk_usage('C:')  # Windows C: drive
            used_percent = (disk_usage.used / disk_usage.total) * 100
            
            status = "healthy"
            if used_percent > 90:
                status = "critical"
            elif used_percent > 80:
                status = "warning"
            
            return {
                "status": status,
                "total_gb": round(disk_usage.total / (1024**3), 2),
                "used_gb": round(disk_usage.used / (1024**3), 2),
                "free_gb": round(disk_usage.free / (1024**3), 2),
                "used_percent": round(used_percent, 2),
                "details": f"Disk usage at {used_percent:.1f}%"
            }
        except Exception as e:
            return {
                "status": "unknown",
                "error": str(e),
                "details": "Could not check disk usage"
            }
    
    async def _get_system_metrics(self) -> Dict[str, Any]:
        """Get system-level metrics"""
        try:
            cpu_percent = psutil.cpu_percent(interval=1)
            load_avg = psutil.getloadavg() if hasattr(psutil, 'getloadavg') else [0, 0, 0]
            
            return {
                "cpu_percent": cpu_percent,
                "load_average": load_avg,
                "boot_time": datetime.fromtimestamp(psutil.boot_time()).isoformat(),
                "platform": psutil.platform if hasattr(psutil, 'platform') else "Windows"
            }
        except:
            return {
                "cpu_percent": 0,
                "load_average": [0, 0, 0],
                "error": "Could not retrieve system metrics"
            }
    
    def _get_uptime(self) -> Dict[str, Any]:
        """Get service uptime information"""
        uptime_seconds = time.time() - self.start_time
        return {
            "seconds": round(uptime_seconds, 2),
            "readable": self._format_uptime(uptime_seconds),
            "started_at": datetime.fromtimestamp(self.start_time).isoformat()
        }
    
    def _format_uptime(self, seconds: float) -> str:
        """Format uptime in human-readable format"""
        days = int(seconds // 86400)
        hours = int((seconds % 86400) // 3600)
        minutes = int((seconds % 3600) // 60)
        secs = int(seconds % 60)
        
        if days > 0:
            return f"{days}d {hours}h {minutes}m {secs}s"
        elif hours > 0:
            return f"{hours}h {minutes}m {secs}s"
        elif minutes > 0:
            return f"{minutes}m {secs}s"
        else:
            return f"{secs}s"
    
    def _calculate_overall_status(self, components: Dict[str, Any]) -> str:
        """Calculate overall health status"""
        statuses = [comp.get("status", "unknown") for comp in components.values()]
        
        if "critical" in statuses:
            return "critical"
        elif "unhealthy" in statuses:
            return "unhealthy"
        elif "degraded" in statuses or "warning" in statuses:
            return "degraded"
        elif all(status == "healthy" for status in statuses):
            return "healthy"
        else:
            return "unknown"
    
    def _store_health_history(self, health_data: Dict[str, Any]):
        """Store health check in history"""
        self.health_history.append(health_data)
        
        # Keep only recent history
        if len(self.health_history) > self.max_history:
            self.health_history = self.health_history[-self.max_history:]
    
    def get_health_history(self, minutes: int = 60) -> List[Dict[str, Any]]:
        """Get health history for the last N minutes"""
        cutoff_time = datetime.now() - timedelta(minutes=minutes)
        
        return [
            entry for entry in self.health_history
            if datetime.fromisoformat(entry["timestamp"]) > cutoff_time
        ]
    
    def get_health_summary(self) -> Dict[str, Any]:
        """Get health summary statistics"""
        if not self.health_history:
            return {"error": "No health history available"}
        
        recent_entries = self.get_health_history(60)  # Last hour
        if not recent_entries:
            return {"error": "No recent health data"}
        
        status_counts = {}
        for entry in recent_entries:
            status = entry["overall_status"]
            status_counts[status] = status_counts.get(status, 0) + 1
        
        return {
            "last_hour_checks": len(recent_entries),
            "status_distribution": status_counts,
            "latest_status": self.health_history[-1]["overall_status"],
            "uptime": self._get_uptime()
        }

# Global health monitor
health_monitor = HealthMonitor()