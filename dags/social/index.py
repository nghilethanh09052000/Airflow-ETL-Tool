import requests


url = 'https://www.tiktok.com/@playsipher'
post_body = {"cmd": "request.get", "url": url, "maxTimeout": 60000}

response = requests.post(
    'http://34.136.51.127:8191/v1',
    json=post_body,
    headers={'Content-Type': 'application/json'}
)
data = response.json()
if data.get('status') != 'ok':
    raise Exception(f"Failed to get response from {url}")

html_response = data.get('solution').get('response')

with open('index.html', 'w') as f:
    f.write(html_response)