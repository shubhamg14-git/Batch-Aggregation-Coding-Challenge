# Import statements
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, avg, min, max, round
from pyspark.sql.types import TimestampType


def main(window_duration):

    # Create spark session with application name 
    spark = SparkSession.builder \
    .appName("DataAggregator") \
    .config("spark.sql.session.timeZone", "UTC") \
    .getOrCreate()

    # Read input file & cast timestamp column from String to TimestampType
    weather_input_df = spark.read.csv("input_data/weather_data.csv", header=True, inferSchema=True)
    weather_input_df = weather_input_df.withColumn("Timestamp", col("Timestamp").cast(TimestampType()))

    # Aggregate data based on Timestamp, Metric Type & Calculate diff metrics (Average, Min & Max Values)
    aggregated_df = weather_input_df.groupBy(
        window(col("Timestamp"), window_duration),
        col("Metric")
    ).agg(
        round(avg("Value"),2).alias("average_value"),
        round(min("Value"),2).alias("min_value"),
        round(max("Value"),2).alias("max_value")
    ).select(
        col("window.start").alias("start_time"),
        col("window.end").alias("end_time"),
        col("Metric"),
        col("average_value"),
        col("min_value"),
        col("max_value")
    ).orderBy("start_time", "Metric")

    # Print results in console
    aggregated_df.show(truncate=False)

    # Converting to a Pandas dataframe (hadoop native drivers not supporting direct write in windows) 
    aggregated_df = aggregated_df.toPandas()

    # Writing output to a csv file
    aggregated_df.to_csv("output_data/aggregated_weather_metrics.csv", index=False)

    # Stop spark session to release all resources
    spark.stop()

if __name__ == "__main__":

    # Check if window duration was provided as a command-line argument, otherwise pass default as 24 hours
    if len(sys.argv) > 1:
        window_duration = sys.argv[1] 
    else:
        window_duration = "24 hour" 

    # Call main method and calculate aggregations
    main(window_duration)