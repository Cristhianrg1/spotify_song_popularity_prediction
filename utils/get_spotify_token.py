import os
import base64
import requests


def get_token():
    client_id = os.getenv("CLIENT_ID")
    client_secret = os.getenv("CLIENT_SECRET")
    auth_str = f"{client_id}:{client_secret}"
    b64_auth_str = base64.b64encode(auth_str.encode()).decode()

    url = "https://accounts.spotify.com/api/token"
    headers = {"Authorization": f"Basic {b64_auth_str}"}
    data = {"grant_type": "client_credentials"}

    response = requests.post(url, headers=headers, data=data)

    if response.status_code == 200:
        return response.json().get("access_token")
    else:
        print("Error getting token:", response.status_code)
        return None
