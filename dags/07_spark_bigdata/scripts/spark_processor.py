import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, window, date_format, rank
from pyspark.sql.window import Window

DATA_DIR = '/opt/airflow/include/data'
OUTPUT_DIR = '/opt/airflow/include/data/spark_output'

def process_big_data():
    print("Initializing Spark Session...")
    spark = SparkSession.builder \
        .appName("EcommerceBigDataProcessor") \
        .master("local[*]") \
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("WARN")

    # We assume the Bronze layer has generated some orders.
    # In a real Big Data scenario, this could be a directory with thousands of Parquet files
    orders_path = f"{DATA_DIR}/orders_*.csv"
    
    # Read the dataset
    print(f"Reading data from {orders_path}")
    try:
        df = spark.read.csv(orders_path, header=True, inferSchema=True)
    except Exception as e:
        print(f"No order files found. Error: {e}")
        spark.stop()
        return

    # Data transformation: Calculate a running total per user and rank their orders
    print("Performing Distributed Window Functions...")
    window_spec = Window.partitionBy("user_id").orderBy("order_timestamp")
    
    # Calculate Total Amount per order
    df_transformed = df.withColumn("total_amount", col("quantity") * col("unit_price"))
    
    # Calculate Running Total and Rank
    df_transformed = df_transformed \
        .withColumn("running_total_spent", sum("total_amount").over(window_spec)) \
        .withColumn("order_rank", rank().over(window_spec))

    # Save results as Parquet (Partitioned by date for performance)
    df_transformed = df_transformed.withColumn("order_date", date_format(col("order_timestamp"), "yyyy-MM-dd"))
    
    output_path = os.path.join(OUTPUT_DIR, "user_lifetime_value")
    print(f"Writing results to {output_path} (partitioned by date)...")
    
    df_transformed.write \
        .mode("overwrite") \
        .partitionBy("order_date") \
        .parquet(output_path)

    print("Spark Processing Completed Successfully!")
    spark.stop()

if __name__ == "__main__":
    process_big_data()
