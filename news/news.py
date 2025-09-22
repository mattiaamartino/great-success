import requests
import json
from datetime import date
from dotenv import load_dotenv
import os

load_dotenv()
API_KEY = os.getenv("API_KEY")

def get_financial_news():
    today = date.today().isoformat()

    url = "https://newsapi.org/v2/top-headlines"

    params = {
        # "q": "finance OR stock market OR economy OR banking OR investment",
        # "category": "business",
        "sources": "bloomberg,the-wall-street-journal,financial-post,fortune,reuters,",
        "from": today,
        "to": today,
        "language": "en",
        "sortBy": "publishedAt",
        "apiKey": API_KEY,
        "pageSize": 50
    }

    response = requests.get(url, params=params)

    if response.status_code != 200:
        raise Exception(f"NewsAPI request failed: {response.status_code} {response.text}")

    data = response.json()
    return data

if __name__ == "__main__":
    news_data = get_financial_news()
    
    # Save to JSON file
    with open("financial_news.json", "w", encoding="utf-8") as f:
        json.dump(news_data, f, ensure_ascii=False, indent=4)