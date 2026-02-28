import pandas as pd
import requests
from sqlalchemy import create_engine
from config import DB_CONFIG

engine = create_engine(f"mysql+mysqlconnector://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}")

def fetch_fx_rates(currency="EUR"):
    print(f"Pobieranie danych dla {currency}/PLN z API NBP...")
    url = f"http://api.nbp.pl/api/exchangerates/rates/A/{currency}/2024-01-01/2026-02-28/?format=json"
    response = requests.get(url).json()
    df = pd.DataFrame(response['rates'])
    df['currency_pair'] = f"{currency}/PLN"
    df = df.rename(columns={'effectiveDate': 'date', 'mid': 'rate_mid'})[['date', 'currency_pair', 'rate_mid']]

    df.to_sql('market_data', engine, if_exists='replace', index=False)
    print(f"Sukces! Dane dla {currency} załadowane do DBeaver.")

if __name__ == "__main__":
    fetch_fx_rates("EUR")
    fetch_fx_rates("USD")
