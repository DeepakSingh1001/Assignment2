import zipfile
import io
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from datetime import timedelta

def main():

    spark = SparkSession.builder \
    .appName("ExternalApp") \
    .master("spark://spark-master:7077") \
    .config("spark.executor.memory", "2g") \
    .config("spark.executor.cores", "2") \
    .config("spark.cores.max", "4") \
    .getOrCreate()

    
    zip_files = [
        "data/Divvy_Trips_2019_Q4.zip",
        "data/Divvy_Trips_2020_Q1.zip"
    ]
    
    dfs = {}   
    
    for zip_file in zip_files:
        with zipfile.ZipFile(zip_file, "r") as z:
            for file in z.namelist():
                # Skip macOS or non-CSV files
                if file.startswith("__MACOSX") or not file.endswith(".csv"):
                    continue
    
                print(f"Reading file: {file} from {zip_file}")
    
                # Read file safely (handle encoding issues)
                data = z.read(file).decode("utf-8", errors="ignore")
    
                # Convert the text to Spark RDD
                rows = [row for row in data.split("\n") if row.strip() != ""]
                rdd = spark.sparkContext.parallelize(rows)
    
                # Load as DataFrame
                df = spark.read.csv(rdd, header=True, inferSchema=True)
    
                # Save this dataframe with a name
                dfs[zip_file] = df
    
    # Access DataFrames separately:
    df1 = dfs.get("data/Divvy_Trips_2019_Q4.zip")
    df_2020_Q1 = dfs.get("data/Divvy_Trips_2020_Q1.zip")
    
    print("First ZIP DataFrame:")
    
    print("Second ZIP DataFrame:")
    
    # Q1. What is the average trip duration per day?
    def avg_trip_duration_per_day(df):
        df = df.withColumn("Trip_Duration", regexp_replace(col("tripduration"), ",", ""))
        result = df.withColumn("date", to_date(col("start_time"))) \
                   .groupBy("date") \
                   .agg(round(avg("Trip_Duration"),1).alias("avg_trip_duration"))
        
        result.write.csv(f"reports/Avg_trip_Duration_PerDay", header=True, mode="overwrite")
        return "Result File for Question 1 Downloaded Successfully!!"
    
    
    # Q2. How many trips were taken each day?
    def total_trips(df):
        df = df.withColumn("Trip_Duration", regexp_replace(col("tripduration"), ",", ""))
        df = df.withColumn("date_only", to_date(col("start_time")))
        result = df.groupBy("date_only").agg(count("trip_id").alias("total_trips"))
    
        result.write.csv(f"reports/TotalTrips_rips_PerDay", header=True, mode="overwrite")
        return "Result File for Question 2 Downloaded Successfully!!"
    
        
    # Q3. What was the most popular starting trip station for each month?
    def Most_Popular_starting_station_each_month(df):
        df = df.withColumn("Trip_Duration", regexp_replace(col("tripduration"), ",", ""))
        df1 = df.withColumn("Month", month(col("start_time")))
        df2 = df1.groupBy("Month", "from_station_name").count()
        df3 = df2.withColumn("Rank", row_number().over(Window.partitionBy("Month").orderBy(col("count").desc())))
        result = df3.filter(col("Rank")==1).select("Month", "from_station_name", "count")
    
        result.write.csv(f"reports/Popular_Station_PerMonth", header=True, mode="overwrite")
        return "Result File for Question 3 Downloaded Successfully!!"
    
    # Q4. What were the top 3 trip stations each day for the last two weeks?
    def top3_trip_stations_each_day_for_last_2_weeks(df):
        df = df.withColumn("Trip_Duration", regexp_replace(col("tripduration"), ",", ""))
        df = df.withColumn("date", to_date("start_time"))
        
        max_date = df.select(F.max('date').alias('max_date')).first()[0]
        two_weeks_before = max_date-timedelta(weeks=2)
    
        filter_data = df.filter((col("date") > two_weeks_before) & (col("date") <= max_date)) \
                    .groupBy("date", "from_station_name") \
                    .count()
    
        window = Window.partitionBy("Date").orderBy(col("count").desc())
    
    
        result = filter_data.withColumn("rank", row_number().over(window)) \
                        .filter(col("rank") <= 3) \
                        .orderBy("date", "rank")
    
        result.write.csv(f"reports/top3_trip_stations_last_2_week", header=True, mode="overwrite")
        return "Result File for Question 4 Downloaded Successfully!!"
    
    
    # Q5. Do Males or Females take longer trips on average?
    
    def trips_avg_of_male_female(df):
        updated_df = df.withColumn("Trip_Duration", regexp_replace(col("tripduration"), ",", ""))
        result = updated_df.groupBy("gender").agg(round(avg("Trip_Duration"),1).alias("Average"))
    
        result.write.csv(f"reports/trips_avg_of_male_female", header=True, mode="overwrite")
        return "Result File for Question 5 Downloaded Successfully!!"
    
    # Q6. What is the top 10 ages of those that take the longest trips, and shortest?
    
    def top_ten_ages_with_shortest_and_longest_trips(df):
        df = df.withColumn("Trip_Duration", regexp_replace(col("tripduration"), ",", ""))
        df = df.withColumn("Year_of_Birth", to_date("birthyear"))
        df = df.withColumn("Age", floor(date_diff(F.current_date(), F.col("Year_of_Birth"))/365.25))
    
        # Store the results in variables without showing them yet
        longest_trips  = df.filter(F.col("Age").isNotNull()).orderBy(col("Trip_Duration").desc()).select("Age", "Trip_Duration").limit(10)
        shortest_trips = df.filter(F.col("Age").isNotNull()).orderBy(col("Trip_Duration").asc()).select("Age", "Trip_Duration").limit(10)
    
        longest_trips.write.csv(f"reports/Top_Ten_Ages/Longest_trip", header=True, mode="overwrite")
    
        shortest_trips.write.csv(f"reports/Top_Ten_Ages/Shortest_trip", header=True, mode="overwrite")
        
        return "Result File for Question 6 Downloaded Successfully!!"

    a = avg_trip_duration_per_day(df1)
    b = total_trips(df1)
    c = Most_Popular_starting_station_each_month(df1)
    d = top3_trip_stations_each_day_for_last_2_weeks(df1)
    e = trips_avg_of_male_female(df1)
    f = top_ten_ages_with_shortest_and_longest_trips(df1)
    
    print(a)
    print(b)
    print(c)
    print(d)
    print(e)
    print(f)

if __name__ == "__main__":
    main()
