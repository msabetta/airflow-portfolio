import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg

def process_taxi_data(spark, input_path, output_path):
    print("Processing taxi data...")
    
    df = spark.read.parquet(input_path)
    df.createOrReplaceTempView("taxi_data")
    
    # Calculate metrics
    metrics_df = spark.sql("""
        SELECT 
            payment_type,
            COUNT(*) as total_rides,
            AVG(total_amount) as avg_amount
        FROM taxi_data
        GROUP BY payment_type
    """)
    
    metrics_df.show()
    metrics_df.write.mode("overwrite").parquet(output_path)
    print("Taxi data processed successfully.")