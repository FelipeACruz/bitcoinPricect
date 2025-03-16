
import sys
import traceback
import time
from pyspark.sql.dataframe import DataFrame
from pyspark.sql import functions as F

def main():

    try:

        from utils import settings
        from service import apiConnection
        from src import bitcoinData, sparkData, visualizeData

        # Total de argumentos
        print(f"Nombre de Python script: {sys.argv[0]}")

        conf = settings.Settings
        # Iniciamos la spark session
        spark, user, fs, path = conf.get_spark_session("btc")
        spark.sparkContext.setLogLevel("ERROR")

        # Obtener lista de todas las criptomonedas con su ID, nombre y símbolo.
        # Obtener el ID de la moneda Bitcoin

        url_list = "https://api.coingecko.com/api/v3/coins/list"
        # # Llamada a la API
        response = apiConnection.get_api_data(url_list)

        if response.status_code == 200:
            data_list = response.json()

        else:
            print(f"Error {response.status_code}: {response.text}")


        print("Obtener lista de todas las criptomonedas en la API")
        bitcoinData.get_bitcoin_id(data_list)

        print("**********************************************************************")
        print("Inicia proceso de obtener el precio de Bitcoin en USD y por Q1 por año")

        # Obtener el precio de Bitcoin en USD y por Q1 por año
        # timestamps formato requerido por la API
        anio: int = 2025
        start_date = int(time.mktime((anio, 1, 1, 0, 0, 0, 0, 0, 0)))
        end_date = int(time.mktime((anio, 3, 31, 23, 59, 59, 0, 0, 0)))

        # URL de la API
        url_btc = "https://api.coingecko.com/api/v3/coins/bitcoin/market_chart/range"
        # Parámetros de la consulta
        params = {
            "vs_currency": "usd",
            "from": start_date,
            "to": end_date
        }

        # Hacer la solicitud a la API
        response = apiConnection.get_api_data(url_btc, params)
        if response.status_code == 200:
            data_btc = response.json()

        else:
            print(f"Error {response.status_code}: {response.text}")

        bitcoinData.fetch_bitcoin_by_year(data_btc, anio)

        print("**********************************************************************")
        print("Inicia proceso de obtener el precio de Bitcoin en USD ultimos 90 dias")

        url_btc = "https://api.coingecko.com/api/v3/coins/bitcoin/market_chart"
        params = {
            "vs_currency": "usd",
            "days": "90",
            "interval": "daily"}

        # Hacer la solicitud a la API
        response = apiConnection.get_api_data(url_btc, params)
        if response.status_code == 200:
            data_btc = response.json()

        else:
            print(f"Error {response.status_code}: {response.text}")

        bitcoinData.fetch_bitcoin_prices(data_btc)

        print("**********************************************************************")
        print("******************** Datos obtenidos con exito ***********************")
        print("******************** Guarda datos en Hive ****************************")

        # Lee el archivo csv con Spark
        df_dt_btc: DataFrame = sparkData.read_data_csv("C:/Users/ASUS/Documents/Desarrollos/Python/bitcoinPricect/bitcoin_prices_2025.csv", spark)

        # Guarda los datos del csv en una tabla en Hive con Spark
        sparkData.save_table(df_dt_btc, user, "cd_btc_data_2025")
        df_btc: DataFrame = spark.read.table("cd_btc_data_2025")
        # df_btc.printSchema()
        # print(df_btc.count())

        # Genera una vista temporal del DataFrame
        df_btc.createOrReplaceTempView("btc_data_2025")

        # spark.sql("""
        #     SELECT
        #       date,
        #       COUNT(*) as dias
        #     FROM btc_data_2025
        #     GROUP BY
        #       date
        #     HAVING
        #       COUNT(*) > 1
        #     ORDER BY date DESC""").show()
        #
        # spark.sql("""
        # SELECT
        #   date,
        #   avg(price)
        # FROM btc_data_2025
        # GROUP BY
        #   date
        # ORDER BY date DESC""").show()

        spark.sql("""
            SELECT
                date,
                AVG(price) OVER (ORDER BY date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS 5_day_avg
            FROM btc_data_2025 ORDER BY 1 DESC
        """).show()

        df_average:DataFrame = sparkData.calculate_moving_average(df_btc)
        df_average.orderBy(F.col("date").desc()).show()

        # Mostrar el grafico
        visualizeData.get_csv_visualize(df_average)
        spark.stop()

    except Exception as e:

        print(str(e))
        print(traceback.format_exc())
        sys.exit(1)

# -------------------------------- RUN -------------------------------

if __name__ == '__main__':
    print("**********************************************************************")
    print("******************* Inicia Proceso ***********************************")
    main()
