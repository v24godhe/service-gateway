import requests
import time
import hashlib

# Generate token
token = hashlib.sha256('ForlagsystemGateway2024SecretKey'.encode()).hexdigest()

base_url = "http://10.200.0.2:8080"
headers = {
    "Authorization": f"Bearer {token}",
    "Content-Type": "application/json"
}

print("Testing rate limits...")

# Test tracking endpoint
for i in range(12):
    try:
        response = requests.post(
            f"{base_url}/api/tracking",
            json={"order_number": "2220477"},
            headers=headers,
            timeout=5
        )
        print(f"Request {i+1}: Status {response.status_code}")
        if response.status_code == 429:
            print(f"Rate limited! Response: {response.json()}")
            break
        time.sleep(1)
    except Exception as e:
        print(f"Request {i+1} failed: {e}")