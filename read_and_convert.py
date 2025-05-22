from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.appName("PySparkPostgreSQLReadSmallTable")
    .config("spark.jars.packages", "org.postgresql:postgresql:42.7.5")
    .getOrCreate()
)

db_properties = {  # PostgreSQL connection properties
    "user": "postgres",
    "password": "root",
    "driver": "org.postgresql.Driver",
}
jdbc_url = "jdbc:postgresql://localhost:5432/mydb"
table_name = "details"

try:
    df_read = (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", table_name)
        .option("user", db_properties["user"])
        .option("password", db_properties["password"])
        .option("driver", db_properties["driver"])
        .load()
    )

    print("\nData read from PostgreSQL (without explicit partitioning):")
    df_read.show()
    df_read.printSchema()

    print(
        f"Number of partitions in the DataFrame after read: {df_read.rdd.getNumPartitions()}"
    )

    output_path = (
        "C:\\Users\\Radhika\\OneDrive\\Documents\\PySpark projects\\output_data_small"
    )

    print("\nSaving data as Parquet...")
    df_read.write.mode("overwrite").parquet(f"{output_path}/details_parquet")
    print("Data saved as Parquet successfully!")

except Exception as e:
    print(f"\nAn error occurred: {e}")

finally:
    spark.stop()
    print("\nSpark Session stopped successfully.")
