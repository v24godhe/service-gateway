import hashlib
secret = "ForlagsystemGateway2024SecretKey"
token = hashlib.sha256(secret.encode()).hexdigest()
print(f"Auth Token: {token}")

import platform
print(platform.architecture())
