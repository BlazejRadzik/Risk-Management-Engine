import pandas as pd
import requests
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
from config import DB_CONFIG

# Bezpieczne kodowanie hasła (rozwiązuje błąd 'Unknown MySQL server host')
safe_password = quote_plus(DB_CONFIG['password'])

# Połączenie z bazą MySQL
engine = create_engine(f"mysql+mysqlconnector://{DB_CONFIG['user']}:{safe_password}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}")

def fetch_fx_rates(currency="EUR"):
    print(f"Pobieranie danych dla {currency}/PLN z API NBP...")
    
    # Zakres dat: API NBP akceptuje max 367 dni. Pobieramy ostatni rok (2025-03-01 do 2026-02-28)
    url = f"http://api.nbp.pl/api/exchangerates/rates/A/{currency}/2025-03-01/2026-02-28/?format=json"
    
    response = requests.get(url)
    
    if response.status_code == 200:
        data = response.json()
        df = pd.DataFrame(data['rates'])
        df['currency_pair'] = f"{currency}/PLN"
        df = df.rename(columns={'effectiveDate': 'date', 'mid': 'rate_mid'})[['date', 'currency_pair', 'rate_mid']]
        
        # Zapis do SQL (append dodaje dane, replace czyści tabelę)
        df.to_sql('market_data', engine, if_exists='append', index=False)
        print(f"Sukces! Dane dla {currency} załadowane do bazy.")
    else:
        print(f"Błąd API NBP: {response.status_code} - {response.text}")

if __name__ == "__main__":
    # Najpierw czyścimy starą tabelę, żeby uniknąć duplikatów
    try:
        with engine.connect() as conn:
            conn.execute(text("DROP TABLE IF EXISTS market_data"))
            conn.commit()
        
        fetch_fx_rates("EUR")
        fetch_fx_rates("USD")
    except Exception as e:
        print(f"Błąd połączenia z bazą: {e}")