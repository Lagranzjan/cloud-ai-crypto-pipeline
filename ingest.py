import requests
from google.cloud import storage
import json
import sys

# Pobieranie danych rynkowych z API
url = "https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd"
headers = {"User-Agent": "Mozilla/5.0"}
response = requests.get(url, headers=headers)
data = response.json()

# Pobieranie nazwy bucketu z linii komend i zapis do GCP
bucket_name = sys.argv[1]

client = storage.Client()
bucket = client.bucket(bucket_name)
blob = bucket.blob("raw_crypto_data.json")

blob.upload_from_string(data=json.dumps(data), content_type='application/json')
print(f"Pomyślnie wysłano dane do gs://{bucket_name}/raw_crypto_data.json")
