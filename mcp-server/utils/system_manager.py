from utils.prompt_manager import PromptManager

class SystemManager:
    def __init__(self):
        self.pm = PromptManager()
    
    def get_available_systems(self):
        """Get list of active systems"""
        # For now hardcoded, later from DB
        return ["STYR", "JEEVES", "ASTRO"]
    
    def get_system_config(self, system_id: str):
        """Get system configuration"""
        return self.pm.get_system_config(system_id)
    
    def get_gateway_url(self, system_id: str):
        """Get gateway URL for system"""
        config = self.get_system_config(system_id)
        return f"{config['gateway_base_url']}/execute-query" if config else None