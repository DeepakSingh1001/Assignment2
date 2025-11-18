import polars as pl


def main():
    # Q1. Convert all data types to the correct ones.
    def dtypes_checking_and_updating():
        schema_overrides = {
            "ride_id": pl.Categorical,
            "rideable_type": pl.String,
            "started_at": pl.Datetime,
            "ended_at": pl.Datetime,
            "start_station_name": pl.String,
            "start_station_id": pl.String,
            "end_station_name": pl.String,
            "end_station_id": pl.String,
            "start_lat": pl.Float64,
            "start_lng": pl.Float64,
            "end_lat": pl.Float64,
            "end_lng": pl.Float64,
            "member_casual": pl.String,
        }
        return pl.scan_csv("data/202306-divvy-tripdata.csv", schema=schema_overrides)

    # Q2. Count the number of bike rides per day.
    def count_number_of_bike_rides_per_day(lazy_df):
        df = lazy_df.with_columns(pl.col("started_at").dt.date().alias("Started_date"))
        result = (df.group_by("Started_date").agg(pl.len().alias("count")).collect())
        result.write_csv("Results/Total_Rides_Per_day.csv", include_header=True)
        return result

    # Q3. Calculate average, max, and min number of rides per week.
    def avg_max_min_number_of_rides_per_week(lazy_df):
        df = lazy_df.with_columns(pl.col("started_at").dt.date().alias("Started_date"))
        df = df.with_columns(pl.col("Started_date").dt.week().alias("week"))
        res = (
            df.group_by("week")
            .agg(pl.len().alias("Ride_Count"))
            .select([
                pl.mean("Ride_Count").alias("Average_Rides"),
                pl.max("Ride_Count").alias("Max_rides"),
                pl.min("Ride_Count").alias("Min_rides")
            ])
            .collect()
        )
        res.write_csv("Results/average_max_minimum_number_of_rides_per_week.csv", include_header=True)
        return res

    # Q4. For each day, calculate how many rides above/below same day last week.
    def diff_b_w_days_of_ride(lazy_df):
        df = lazy_df.with_columns(pl.col("started_at").dt.date().alias("Started_date"))
        daily_counts = df.group_by("Started_date").agg(pl.len().alias("ride_count"))

        daily_counts = daily_counts.with_columns([
            pl.col("Started_date").dt.weekday().alias("day_of_week"),
            (pl.col("Started_date") - pl.duration(days=7)).alias("last_week_date")
        ])

        result = daily_counts.join(
            daily_counts.select([
                pl.col("Started_date").alias("last_week_date"),
                pl.col("ride_count").alias("last_week_ride_count")
            ]),
            on="last_week_date",
            how="left"
        ).with_columns([
            (pl.col("ride_count").cast(pl.Int32) - pl.col("last_week_ride_count").cast(pl.Int32)).alias("diff_vs_last_week")
        ]).select([
            "Started_date", "day_of_week", "ride_count", "last_week_ride_count", "diff_vs_last_week"
        ]).collect()

        result.write_csv("Results/Diff_b_w_current_and_last_weeks_day.csv", include_header=True)
        return result

    # Execute pipeline
    lazy_df = dtypes_checking_and_updating()
    print("Original_Dataframe:- ",lazy_df.collect())
    print("Count of bike rides per Day:- ",count_number_of_bike_rides_per_day(lazy_df))
    print("Avg_Max_Min_number_of_Rides:- ", avg_max_min_number_of_rides_per_week(lazy_df))
    print("diff_b_w_days_of_ride:- ", diff_b_w_days_of_ride(lazy_df))


if __name__ == "__main__":
    main()   