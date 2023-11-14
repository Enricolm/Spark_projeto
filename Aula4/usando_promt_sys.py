import sys
from pyspark.sql import SparkSession


if __name__ == "__main__":
    spark = SparkSession.builder.appName("Usando_sys").getOrCreate()

    dados_csv = spark.read.format("csv")\
                .option("sep",";")\
                .load(sys.argv[1])
    
    dados_csv.show()