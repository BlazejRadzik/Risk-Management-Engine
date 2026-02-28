import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from urllib.parse import quote_plus
from config import DB_CONFIG

# Bezpieczne kodowanie hasła
safe_password = quote_plus(DB_CONFIG['password'])
engine = create_engine(f"mysql+mysqlconnector://{DB_CONFIG['user']}:{safe_password}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}")

def calculate_var(currency_pair='EUR/PLN'):
    print(f"\n--- Analiza ryzyka dla {currency_pair} ---")
    
    # Pobranie danych z Twojej bazy
    query = f"SELECT date, rate_mid FROM market_data WHERE currency_pair = '{currency_pair}' ORDER BY date"
    df = pd.read_sql(query, engine)
    
    if df.empty:
        print("Brak danych w bazie! Najpierw uruchom data_loader.py.")
        return

    # Obliczanie dziennych stóp zwrotu (log-returns)
    df['returns'] = np.log(df['rate_mid'] / df['rate_mid'].shift(1))
    df = df.dropna()
    
    # Parametry VaR (Metoda Wariancji-Kowariancji)
    volatility = df['returns'].std()
    confidence_level = 0.95
    z_score = 1.645 # Dla 95% pewności
    
    var_95 = z_score * volatility
    
    print(f"Liczba przeanalizowanych sesji: {len(df)}")
    print(f"Dzienny VaR (95%): {var_95:.4%}")
    print(f"Przy ekspozycji 1,000,000 PLN, ryzyko straty wynosi: {1000000 * var_95:,.2f} PLN.")

if __name__ == "__main__":
    calculate_var('EUR/PLN')
    calculate_var('USD/PLN')