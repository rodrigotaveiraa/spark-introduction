import sys, getopt
# from pyspark.sql import SparkSession


if __name__=="__main__":
    # spark = SparkSession.builder.appName("Tabelas").getOrCreate()
    opts = getopt.getopt(sys.argv[1:], "a:t:")
    arquivo, tabela = "", ""

    print(opts)
    # print(args)

    # for opt, arg in opts:
    #     if opt == "-a":
    #         arquivo = arg
    #     elif opt == "-t":
    #         tabela = arg
    
    # df = spark.read.load(arquivo)
    # df.write.format("jdbc").option("url","jdbc:postgresql://192.168.15.73:5432/tabelas").option("dbtable",tabela).option("user","postgres").option("password","1234").option("driver","org.postgresql.Driver").save()
    # spark.stop()