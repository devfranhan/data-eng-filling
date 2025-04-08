
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, sum as spark_sum


def main():

    spark = SparkSession.builder \
            .master("spark://spark-master:7077") \
            .appName("LocalSessionUsingDocker") \
            .config("spark.executor.memory", "1g") \
            .config("spark.executor.cores", "1") \
            .getOrCreate()

    df = spark.read.csv("/opt/shared-data/input/sample.csv", header=True, inferSchema=True)
    # df = df.withColumn("Order Date", to_date("Order Date", "MM/dd/yyyy")) \
    #        .withColumn("Ship Date", to_date("Ship Date", "MM/dd/yyyy"))

    # aggregated = df.groupBy("Region", "Category") \
    #         .agg(
    #             spark_sum("Sales").alias("Total_Sales"),
    #             spark_sum("Profit").alias("Total_Profit"),
    #             spark_sum("Quantity").alias("Total_Quantity")
    #         )

    # df.show(10, truncate=False)

    for column in df.columns:
        print(column)
        print("example: ", df["{}".format(column)])

    # aggregated.cache()

    # result = aggregated.orderBy(col("Total_Profit").desc())

    # result.show()

    # input("Esperando para o SparkUI não fechar.")

    spark.stop()

    return


if __name__ == "__main__":
    main()


