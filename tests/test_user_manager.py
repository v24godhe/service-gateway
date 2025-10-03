import sys
sys.path.append('C:\\service-gateway')

from utils.user_manager import get_user, UserRole

print("Testing User Manager...\n")

# Test each user
users = ["harold", "lars", "peter", "linda", "pontus"]

for username in users:
    user = get_user(username)
    if user:
        print(f"✓ {username}: Role={user.role.value}")
    else:
        print(f"✗ {username}: Not found")

# Test invalid user
user = get_user("invalid")
print(f"\n✓ Invalid user returns None: {user is None}")