
# Spark
from pyspark.sql import SparkSession
from pyspark import SparkConf

# System and Files
import json
import os
import sys

import logging
from logging import basicConfig

class Settings():

    def getShowString(df, n=5, truncate=True, vertical=False):
        if isinstance(truncate, bool) and truncate:
            return (df._jdf.showString(n, 5, vertical))
        else:
            return (df._jdf.showString(n, int(truncate), vertical))

    def getConfigJson(jsonFile):
        if os.path.exists(jsonFile):
            with open(jsonFile, 'r') as f:
                event:dict = json.load(f)
                flag:bool = True
                return (event, flag)

        else:
            return (None, False)

    def enable_logging(stream=None):
        """
        Permite obtener los logs
        :param stream: string que indica el nombre del archivo de logs
        """
        if stream == 'stdout':
            basicConfig(format='%(asctime)s | %(levelname)s : %(name)s %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S',
                        level=20, stream=sys.stdout)
        elif stream is not None:
            basicConfig(format='%(asctime)s | %(levelname)s : %(name)s %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S',
                        level=20, filename=stream, filemode='w+')
        else:
            basicConfig(format='%(asctime)s | %(levelname)s : %(name)s %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S',
                        level=20)

    def get_spark_session(sessionName):
        conf = SparkConf()

        warehouse_location = os.path.abspath("spark-warehouse")

        spark = SparkSession.builder.master("local[*]") \
            .appName(sessionName) \
            .config("spark.sql.warehouse.dir", warehouse_location) \
            .enableHiveSupport() \
            .getOrCreate()
            # .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkSessionCatalog") \
            # .config("spark.sql.catalog.iceberg.type", "hadoop") \


        sc = spark.sparkContext
        user: str = spark.sparkContext.sparkUser()
        fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(sc._jsc.hadoopConfiguration())
        path = sc._jvm.org.apache.hadoop.fs.Path

        spark.conf.set("spark.sql.parquet.compression.codec", "snappy")
        spark.sql("SET hive.exec.dynamic.partition = true")
        spark.sql("SET hive.exec.dynamic.partition.mode = nonstrict")

        spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
        spark.conf.set("spark.sql.parquet.output.committer.class", "org.apache.parquet.hadoop.ParquetOutputCommitter")
        spark.conf.set("spark.sql.sources.commitProtocolClass", "org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol")

        return spark, user, fs, path

    # Función para cargar los datos en Iceberg
    #def load_data_to_iceberg(spark:SparkSession):
     #   df = spark.read.csv("/tmp/bitcoin_prices.csv", header=True, inferSchema=True)
      #  df.write.format("iceberg").mode("overwrite").save("hdfs://path_to_iceberg/bitcoin_prices")