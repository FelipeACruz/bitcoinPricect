
# Spark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.sql.dataframe import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

def save_table(dfTabla: DataFrame, user:str, table_name:str):

    dfFinalTabla = dfTabla \
        .withColumn("fcusuariocreo", F.lit(user)) \
        .withColumn("fdfechacreacion", F.unix_timestamp(F.current_timestamp()))

    dfFinalTabla.printSchema()

    # dfFinalTabla.write.format("parquet").option("compression", "snappy").mode("overwrite").saveAsTable(table_name)
    dfFinalTabla.coalesce(1).write.format("parquet").option("compression", "snappy").insertInto(table_name, overwrite=True)

def read_data_csv(pathFile:str, spark: SparkSession) -> DataFrame:

    schema = StructType([
        StructField("price", DoubleType(), True),
        StructField("date", StringType(), True)
    ])

    dfCsv: DataFrame = spark.read.csv(pathFile, header=True, schema=schema)
    return dfCsv


def calculate_moving_average(df:DataFrame) -> DataFrame:

    df: DataFrame = df.withColumn('date', F.to_date(F.col('date'), 'yyyy-MM-dd'))
    # Crear una ventana de 5 días
    windowSpec = Window.orderBy('date').rowsBetween(-4, 0)

    # Calcular la media móvil de 5 días
    df_moving_avg = df.withColumn('media_movil_5d', F.avg('price').over(windowSpec))

    # Mostrar el resultado
    df_moving_avg = df_moving_avg.select('date', 'media_movil_5d') #'price',
    df_moving_avg.printSchema()

    return df_moving_avg

