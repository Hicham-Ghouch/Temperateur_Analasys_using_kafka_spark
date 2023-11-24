from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("TemperatureAnalysis").getOrCreate()

# Lecture des données Kafka
df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("subscribe", "temperature_topic").load()
#  obtenir la température maximale et minimale
agg_df = df.selectExpr("CAST(value AS STRING)").selectExpr("CAST(value AS FLOAT) AS temperature")\
    .groupBy().agg({"temperature": "max", "temperature": "min"})

# Mettre les donnés dans ma base de données dans postgres
query = agg_df.writeStream \
    .outputMode("complete") \
    .foreachBatch(lambda batch, batchId: batch.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://your_postgresql_host:5432/your_database") \
        .option("dbtable", "temperature_aggregation") \
        .option("user", "your_username") \
        .option("password", "your_password") \
        .mode("overwrite") \
        .save()
    ) \
    .start()

query.awaitTermination()