from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("austin-animal-center").getOrCreate()

animal_df = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("mode", "FAILFAST")
    .option("skipRows", 0)
    .load("preprocessed/austin-animal-center-curated.csv")
)

print(animal_df.collect()[0])
