from pyspark.sql import SparkSession


if __name__ == "__main__": 
    spark = SparkSession.builder.appName("Streaming").getOrCreate()
    jsonSchema = "nome STRING, postagem STRING, data INT"
    df= spark.readStream.json("/home/rodrigo/www/spark/download/testestreaming/", schema= jsonSchema)
    diretorio = "/home/rodrigo/temp/"

    stcal = df.writeStream.format("console").outputMode("append").trigger(processingTime="5 second").option("checkpointlocation", diretorio).start()

    stcal.awaitTermination()