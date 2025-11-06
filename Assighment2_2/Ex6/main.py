import zipfile
import io
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from datetime import timedelta



def read_csv_from_zip_to_df(spark, zip_path):

    df_list = []
    with zipfile.ZipFile(zip_path, "r") as z:
        for member in z.namelist():
            if member.startswith("__MACOSX") or not member.lower().endswith(".csv"):
                continue
            print(f"Reading file: {member} from {zip_path}")
            raw = z.read(member)                         
            text = raw.decode("utf-8", errors="ignore") 
            
            lines = [line for line in text.splitlines() if line.strip() != ""]
            rdd = spark.sparkContext.parallelize(lines)
            df = spark.read.csv(rdd, header=True, inferSchema=True)
            df_list.append(df)

    if not df_list:
        return None
  
    out = df_list[0]
    for other in df_list[1:]:
        out = out.unionByName(other, allowMissingColumns=True)
    return out

def main():

    spark = SparkSession.builder \
    .appName("ExternalApp") \
    .master("local[*]") \
    .config("spark.executor.memory", "2g") \
    .config("spark.executor.cores", "2") \
    .config("spark.cores.max", "4") \
    .getOrCreate()
    
    
    zip_files = [
    "data/Divvy_Trips_2019_Q4.zip",
    "data/Divvy_Trips_2020_Q1.zip"
    ]

    dfs = {}
    for z in zip_files:
        df = read_csv_from_zip_to_df(spark, z)
        if df is None:
            print(f"No CSV files found in {z}")
        else:
            dfs[z] = df

    df1 = dfs.get("data/Divvy_Trips_2019_Q4.zip")
    df2 = dfs.get("data/Divvy_Trips_2020_Q1.zip")

    if df1 is None:
        print("df1 not found — exiting")
        spark.stop()
        return

    # show a quick preview
    df1.show(5, truncate=False)
    print("Files Downloaded Successfully!11")

    # Helper: cast tripduration to double after removing commas
    def prepare_trip_duration(df):
        return df.withColumn("Trip_Duration", regexp_replace(col("tripduration"), ",", "").cast("double"))

    # Q1: average trip duration per day
    def avg_trip_duration_per_day(df):
        df = prepare_trip_duration(df)
        result = df.withColumn("date", to_date(col("start_time"))) \
                   .groupBy("date") \
                   .agg(round(avg("Trip_Duration"), 1).alias("avg_trip_duration"))
        # result.write.mode("overwrite").option("header", True).csv("reports/Avg_trip_Duration_PerDay")
        result.coalesce(1).write.csv("reports/Avg_trip_Duration_PerDay", header=True, mode="overwrite")
        return result

    # Q2: daily trip counts
    def total_trips(df):
        df = df.withColumn("date", to_date(col("start_time")))
        result = df.groupBy("date").agg(count("trip_id").alias("total_trips"))
        # result.write.mode("overwrite").option("header", True).csv("reports/TotalTrips_PerDay")
        result.coalesce(1).write.csv("reports/TotalTrips_PerDay", header=True, mode="overwrite")
        return result

    # Q3: most popular starting station each month
    def most_popular_starting_station_each_month(df):
        df = df.withColumn("Month", month(col("start_time")))
        df2 = df.groupBy("Month", "from_station_name").count()
        win = Window.partitionBy("Month").orderBy(col("count").desc())
        result = df2.withColumn("Rank", row_number().over(win)).filter(col("Rank") == 1) \
                    .select("Month", "from_station_name", "count")
        # result.write.mode("overwrite").option("header", True).csv("reports/Popular_Station_PerMonth")
        result.coalesce(1).write.csv("reports/Popular_Station_PerMonth", header=True, mode="overwrite")
        return result

    # Q4: top 3 stations each day for the last 2 weeks (based on available max date)
    def top3_trip_stations_each_day_for_last_2_weeks(df):
        df = df.withColumn("date", to_date(col("start_time")))
        max_date_row = df.select(F.max("date").alias("max_date")).first()
        if max_date_row is None or max_date_row["max_date"] is None:
            print("No dates found in data for Q4")
            return None
        max_date = max_date_row["max_date"]
        # date_sub expects a Column, so we use a literal date and then filter using string comparison
        two_weeks_before = (max_date - F.expr("interval 14 days")) if False else None
        # Simpler: compute string dates using python datelike arithmetic (safe here)
        from datetime import timedelta as _td
        two_weeks_before_py = max_date - _td(days=14)

        filter_data = df.filter((col("date") > F.lit(two_weeks_before_py)) & (col("date") <= F.lit(max_date))) \
                        .groupBy("date", "from_station_name").count()

        win = Window.partitionBy("date").orderBy(col("count").desc())
        result = filter_data.withColumn("rank", row_number().over(win)) \
                            .filter(col("rank") <= 3) \
                            .orderBy("date", "rank")
        # result.write.mode("overwrite").option("header", True).csv("reports/top3_trip_stations_last_2_week")
        result.coalesce(1).write.csv("reports/top3_trip_stations_last_2_week", header=True, mode="overwrite")
        
        return result

    # Q5: average trip duration by gender
    def trips_avg_of_male_female(df):
        df = prepare_trip_duration(df)
        result = df.groupBy("gender").agg(round(avg("Trip_Duration"), 1).alias("Average"))
        result.write.mode("overwrite").option("header", True).csv("reports/trips_avg_of_male_female")
        result.coalesce(1).write.csv("reports/trips_avg_of_male_female", header=True, mode="overwrite")
        
        return result

    # Q6: top 10 ages with longest and shortest trips
    def top_ten_ages_with_shortest_and_longest_trips(df):
        df = prepare_trip_duration(df)
        # birthyear is likely an int; compute age as (year_of_data - birthyear)
        # If a dataset spans multiple years you'd want to use the trip date year; here we use trip start year.
        df = df.withColumn("start_year", year(to_date(col("start_time")))) \
               .withColumn("birthyear_int", col("birthyear").cast("int")) \
               .withColumn("Age", (col("start_year") - col("birthyear_int")).cast("int"))

        longest = df.filter(col("Age").isNotNull()).orderBy(col("Trip_Duration").desc()).select("Age", "Trip_Duration").limit(10)
        shortest = df.filter(col("Age").isNotNull()).orderBy(col("Trip_Duration").asc()).select("Age", "Trip_Duration").limit(10)

        # longest.write.mode("overwrite").option("header", True).csv("reports/Top_Ten_Ages/Longest_trip")
        longest.coalesce(1).write.csv("reports/Top_Ten_Ages/Longest_trip", header=True, mode="overwrite")
        
        # shortest.write.mode("overwrite").option("header", True).csv("reports/Top_Ten_Ages/Shortest_trip")
        shortest.coalesce(1).write.csv("reports/Top_Ten_Ages/Shortest_trip", header=True, mode="overwrite")
        return longest, shortest

    # Run questions on df1 (you can change to union of df1 and df2 if desired)
    q1 = avg_trip_duration_per_day(df1)
    q2 = total_trips(df1)
    q3 = most_popular_starting_station_each_month(df1)
    q4 = top3_trip_stations_each_day_for_last_2_weeks(df1)
    q5 = trips_avg_of_male_female(df1)
    q6 = top_ten_ages_with_shortest_and_longest_trips(df1)

    # Optionally show small previews
    if q1 is not None: q1.show(5)
    if q2 is not None: q2.show(5)
    if q3 is not None: q3.show(5)
    if q4 is not None: q4.show(10)
    if q5 is not None: q5.show(5)
    if q6 is not None:
        q6[0].show(5)
        q6[1].show(5)

    spark.stop()

if __name__ == "__main__":
    main()
