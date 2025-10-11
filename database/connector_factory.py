# New file: database/connector_factory.py
import os
from database.styr_connector import StyrDatabaseConnector

class DatabaseConnectorFactory:
    """Factory to create database connectors for different systems"""
    
    @staticmethod
    def get_connector(system_id: str = "STYR"):
        """
        Get database connector for specified system
        
        Args:
            system_id: System identifier (STYR, JEEVES, ASTRO)
            
        Returns:
            Database connector instance
        """
        system_id = system_id.upper()
        
        if system_id == "STYR":
            return StyrDatabaseConnector(
                system=os.getenv('STYR_SYSTEM'),
                userid=os.getenv('STYR_USERID'),
                password=os.getenv('STYR_PASSWORD')
            )
        
        elif system_id == "JEEVES":
            # Will implement when you provide connection details
            return StyrDatabaseConnector(
                system=os.getenv('JEEVES_SYSTEM'),
                userid=os.getenv('JEEVES_USERID'),
                password=os.getenv('JEEVES_PASSWORD')
            )
        
        elif system_id == "ASTRO":
            # MSSQL system - will need different connector later
            # For now, raise error
            raise NotImplementedError("ASTRO connector not yet implemented")
        
        else:
            raise ValueError(f"Unknown system: {system_id}")