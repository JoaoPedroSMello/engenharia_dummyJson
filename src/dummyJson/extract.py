import requests
import json
import pandas as pd

# Extrai os produtos da API
def get_products():
    url = "https://dummyjson.com/products"
    response = requests.get(url)
    if response.ok:
        data = json.loads(response.text)

    else:
        response.raise_for_status()

    return data["products"]

# Recebe dados como paramêtro para transformar em Data Frame
def json_to_rawdf(data):
    raw_df = pd.DataFrame(data)
    raw_df.drop('stock', axis=1, inplace=True)
    raw_df.to_csv("raw_data.csv", encoding="UTF-8", index=False)
    return raw_df

