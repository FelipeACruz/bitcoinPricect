
import pandas as pd

def get_bitcoin_id(data):

    cryptosList = data[:]

    for crypto in cryptosList:
        print(f"ID: {crypto['id']}, Name: {crypto['name']}, Symbol: {crypto['symbol']}")

    # Filtrar Bitcoin de la lista de criptomonedas
    bitcoin = next((crypto for crypto in data if crypto["symbol"] == "btc"), None)

    # Mostrar el ID de Bitcoin
    if bitcoin:
        print("**********************************************************************")
        print(f"Bitcoin ID: {bitcoin['id']}")
        print(f"Bitcoin Symbol: {bitcoin['symbol']}")
    else:
        print("**********************************************************************")
        print("Bitcoin no encontrado")


def fetch_bitcoin_prices(data):

    # Convertir a DataFrame de pd
    df = pd.DataFrame(data["prices"], columns=["timestamp", "price"])
    df["date"] = pd.to_datetime(df["timestamp"], unit="ms").dt.date
    df.drop(columns=["timestamp"], inplace=True)

    # Guardar como CSV
    df.to_csv("C:/Users/ASUS/Documents/Desarrollos/Python/bitcoinPricect/bitcoin_prices.csv", index=False)
    print(f"Se guardo el csv bitcoin_prices.csv")

def fetch_bitcoin_by_year(data, anio):

    df = pd.DataFrame(data["prices"], columns=["timestamp", "price"])
    # Convertir timestamp a fecha legible
    df["date"] = pd.to_datetime(df["timestamp"], unit="ms").dt.date
    df.drop(columns=["timestamp"], inplace=True)

    # Asegurar que solo tenemos datos del primer trimestre 2022
    df = df[(df["date"] >= pd.to_datetime(f"{anio}-01-01").date()) &
            (df["date"] <= pd.to_datetime(f"{anio}-03-31").date())]

    # Guardar como CSV
    df.to_csv(f"C:/Users/ASUS/Documents/Desarrollos/Python/bitcoinPricect/bitcoin_prices_{anio}.csv", index=False)
    print(f"Se guardo el csv bitcoin_prices_{anio}.csv")