import sys
from pyspark.sql import SparkSession


if __name__ == "__main__":
    spark = SparkSession.builder.appName("Atividade")\
        .config("spark.jars", "/home/enricolm/Documents/sqljdbc_12.4.1.0_enu/sqljdbc_12.4/enu/jars/mssql-jdbc-12.4.1.jre8.jar") \
        .getOrCreate()
    dados_vendas = spark.read.format("parquet")\
                    .load(sys.argv[1])
    
    dados_vendas.show()

    dados_vendas.write.format("jdbc")\
                        .option("url", "jdbc:sqlserver://sqlserver-rm95367.database.windows.net:1433;database=SparkDatabase;user=findtmaster@sqlserver-rm95367;password=Enrico.02122003;encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;")\
                        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")\
                        .option("dbtable", "Vendas2")\
                        .save()
    spark.stop()