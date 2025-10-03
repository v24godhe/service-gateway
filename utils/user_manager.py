from typing import Optional, Dict, List
from enum import Enum

class UserRole(Enum):
    CEO = "ceo"
    FINANCE = "finance"
    LOGISTICS = "logistics"
    CUSTOMER_SERVICE = "customer_service"
    CALL_CENTER = "call_center"
    UNIT_MANAGER = "unit_manager"

class User:
    def __init__(self, username: str, role: UserRole, department: str = None):
        self.username = username
        self.role = role
        self.department = department  # For unit managers

# Hardcoded users
USERS = {
    "harold": User("harold", UserRole.CEO),
    "lars": User("lars", UserRole.FINANCE),
    "peter": User("peter", UserRole.LOGISTICS),
    "linda": User("linda", UserRole.CUSTOMER_SERVICE),
    "pontus": User("pontus", UserRole.CALL_CENTER),
}

def get_user(username: str) -> Optional[User]:
    return USERS.get(username.lower())