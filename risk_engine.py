import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from config import DB_CONFIG

engine = create_engine(f"mysql+mysqlconnector://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}")

def calculate_risk():

    query = "SELECT rate_mid FROM market_data WHERE currency_pair = 'EUR/PLN' ORDER BY date"
    df = pd.read_sql(query, engine)

    df['returns'] = df['rate_mid'].pct_change().dropna()

    volatility = df['returns'].std()
    var_95 = 1.645 * volatility
    
    print(f"--- ANALIZA RYZYKA ---")
    print(f"Zmienność historyczna (Daily Volatility): {volatility:.4%}")
    print(f"Daily Value at Risk (95%): {var_95:.4%}")
    print(f"Przy ekspozycji 1 mln EUR, firma ryzykuje stratę {1000000 * var_95:,.2f} PLN dziennie.")

if __name__ == "__main__":
    calculate_risk()
