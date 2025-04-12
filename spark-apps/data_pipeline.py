from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, FloatType
from pyspark.sql.functions import col, initcap

def main():
    spark = SparkSession.builder \
        .appName("TitanicETL") \
        .master("spark://spark-master:7077") \
        .config("spark.jars.packages", "/opt/bitnami/spark/jars/postgresql.jar") \
        .config("spark.executor.extraClassPath", "/opt/bitnami/spark/jars/*") \
        .config("spark.driver.extraClassPath", "/opt/bitnami/spark/jars/*") \
        .config("spark.submit.pyFiles", "/opt/bitnami/spark/jars/postgresql.jar") \
        .getOrCreate()


    schema = StructType([
        StructField("passenger_id", IntegerType(), False),
        StructField("survived", IntegerType(), True),
        StructField("pclass", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("sex", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("sibsp", IntegerType(), True),
        StructField("parch", IntegerType(), True),
        StructField("ticket", StringType(), True),
        StructField("fare", FloatType(), True),
        StructField("cabin", StringType(), True),
        StructField("embarked", StringType(), True),

    ])

    df = spark.read.csv('/data/train.csv', header=True, schema=schema)
    df.show(5)
    print("Data reading: Success")

    df = df.withColumn("sex", initcap(col("sex")))

    df = df.fillna({
        "pclass": -1,
        "name": "Unknown",
        "sex": "Unknown",
        "sibsp": -1,
        "age": -1,
        "parch": -1,
        "ticket": "Unknown",
        "embarked": "U",
        "cabin": "Unknown",
        "fare": df.selectExpr("percentile(age, 0.5)").first()[0]
    })
    print("Simple Data Transformation: Success")

    try:
        df.write \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://postgres:5432/data_warehouse") \
            .option("dbtable", "train") \
            .option("user", "postgres") \
            .option("password", "admin") \
            .option("driver", "org.postgresql.Driver") \
            .option("numPartitions", "2") \
            .mode("overwrite") \
            .save()
    except Exception as e:
        print(e)

    spark.stop()



if __name__ == "__main__":
    main()