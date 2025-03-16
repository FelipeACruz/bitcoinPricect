

import sys
import requests

# Función para extraer datos de CoinGecko
def get_api_data(url: str, params: dict= None):

    if params == None:
        print("No hay parametros extra")
        response = requests.get(url)
        return response

    else:
        print("Contiene parametros extra")
        response = requests.get(url, params=params)
        return response






