from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

jdbc_driver_path = "C:\\Users\\Radhika\\Downloads\\postgresql-42.7.5.jar"
spark = (
    SparkSession.builder.appName("PySparkPostgreSQLInsert")
    .config("spark.jars.packages", "org.postgresql:postgresql:42.7.5")
    .getOrCreate()
)
print("Spark session created successfully")

db_name = "mydb"
db_user = "postgres"
db_password = "root"

jdbc_url = f"jdbc:postgresql://localhost:5432/mydb"

db_properties = {
    "user": db_user,
    "password": db_password,
    "driver": "org.postgresql.Driver",
}

table_name = "details"

schema = StructType(
    [
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True),
    ]
)

data = [
    (1, "Shivam", 23),
    (2, "Aashi", 19),
    (3, "Mayank", 20),
    (4, "Yashi", 24),
    (5, "Eve", 25),
]

df_to_insert = spark.createDataFrame(data, schema)
print("DataFrame to be inserted:")
df_to_insert.show()
df_to_insert.printSchema()

try:
    # All your PySpark write operations go here
    df_to_insert.write.format("jdbc").option("url", jdbc_url).option(
        "dbtable", table_name
    ).option("user", db_properties["user"]).option(
        "password", db_properties["password"]
    ).option(
        "driver", db_properties["driver"]
    ).mode(
        "append"
    ).save()

    # THIS print statement runs ONLY if NO error occurred in the 'try' block
    print(f"\nData successfully inserted into PostgreSQL table: {table_name}")

except Exception as e:
    # THIS print statement runs ONLY if an error occurred in the 'try' block
    # 'e' is defined here and holds the error information
    print(f"\nError inserting data into PostgreSQL: {e}")
    # You might also want to re-raise the exception or handle it more gracefully
    # For now, just printing the error is fine for learning.

finally:
    # THIS code always runs, whether an error occurred or not
    spark.stop()
    print("\nSpark Session stopped successfully")
