import requests
import json


payload = {
  "password": "admin",
  "provider": "db",
  "username": "admin",
}

response = requests.post("http://localhost:8088/api/v1/security/login", json=payload)

access_token = response.json()

data = {
  "active": True,
  "email": "samplematha@email.com",
  "first_name": "Matheee",
  "last_name": "Mathaaaa",
  "password": "pass",
  "roles": [
    2
  ],
  "username": "MatheMatha",
}

headersPayload = {
    'Authorization': 'Bearer ' + str(access_token['access_token']),
    'accept': 'application/json',
    'Content-Type': 'application/json'
    # 'CSRF-TOKEN': str(csrf_token['result']),
    # 'Referer': url
}

app_name = 'superset'

user_r = requests.post("http://127.0.0.1:8088/api/v1/security/users/", json=data, headers=headersPayload)