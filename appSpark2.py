import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Escrevendo no terminal

if __name__ == "__main__": 
    spark = SparkSession.builder.appName("Exemplo").getOrCreate()
    arqSchema = "id INT, nome STRING, status STRING, cidade STRING, vendas INT, data STRING"
    despachantes= spark.read.csv(sys.argv[1], header=False, schema= arqSchema)
    calculo = despachantes.select("data").groupBy(year("data")).count()
    calculo.write.format("console").save()
    spark.stop()