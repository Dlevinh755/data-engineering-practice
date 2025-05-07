import os
import glob
import zipfile
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, count, col, desc, to_date, year, month, expr, current_date
from pyspark.sql.window import Window


def unzip_data(zip_folder, extract_to):
    """
    Giải nén tất cả các file .zip trong thư mục zip_folder
    vào thư mục extract_to.
    """
    os.makedirs(extract_to, exist_ok=True)
    zip_files = glob.glob(os.path.join(zip_folder, "*.zip"))
    for zip_path in zip_files:
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(extract_to)


def read_data(spark, data_path):
    return spark.read.csv(data_path, header=True, inferSchema=True)


def average_trip_duration_per_day(df):
    result = df.withColumn("date", to_date("start_time")) \
               .groupBy("date") \
               .agg(avg("tripduration").alias("average_trip_duration"))
    result.write.csv("reports/average_trip_duration_per_day", header=True)


def trips_per_day(df):
    result = df.withColumn("date", to_date("start_time")) \
               .groupBy("date") \
               .agg(count("*").alias("trips_count"))
    result.write.csv("reports/trips_per_day", header=True)


def most_popular_start_station_per_month(df):
    window_spec = Window.partitionBy("year", "month").orderBy(desc("trips_count"))
    agg_df = df.withColumn("month", month("start_time")) \
               .withColumn("year", year("start_time")) \
               .groupBy("year", "month", "from_station_name") \
               .agg(count("*").alias("trips_count"))
    result = agg_df.withColumn("rank", expr("row_number() over (partition by year, month order by trips_count desc)")) \
                   .filter(col("rank") == 1) \
                   .drop("rank")
    result.write.csv("reports/most_popular_start_station_per_month", header=True)


def top_3_trip_stations_last_two_weeks(df):
    recent_dates = df.select(to_date("start_time").alias("date")) \
                     .distinct() \
                     .orderBy(desc("date")) \
                     .limit(14)
    last_two_weeks = df.withColumn("date", to_date("start_time")) \
                       .join(recent_dates, on="date", how="inner")
    window_spec = Window.partitionBy("date").orderBy(desc("trips_count"))
    result = last_two_weeks.groupBy("date", "from_station_name") \
                           .agg(count("*").alias("trips_count")) \
                           .withColumn("rank", expr("row_number() over (partition by date order by trips_count desc)")) \
                           .filter(col("rank") <= 3) \
                           .drop("rank")
    result.write.csv("reports/top_3_trip_stations_last_two_weeks", header=True)


def gender_trip_duration_comparison(df):
    result = df.groupBy("gender") \
               .agg(avg("tripduration").alias("average_trip_duration"))
    result.write.csv("reports/gender_trip_duration_comparison", header=True)


def top_10_ages_longest_shortest_trips(df):
    df = df.withColumn("age", year(current_date()) - col("birthyear"))
    longest_trips = df.orderBy(desc("tripduration")) \
                      .select("age") \
                      .distinct() \
                      .limit(10)
    shortest_trips = df.orderBy("tripduration") \
                       .select("age") \
                       .distinct() \
                       .limit(10)
    longest_trips.write.csv("reports/top_10_ages_longest_trips", header=True)
    shortest_trips.write.csv("reports/top_10_ages_shortest_trips", header=True)


def main():
    # 1. Giải nén file .zip
    unzip_data(zip_folder="/var/tmp/app/Exercise_6/data", extract_to="/var/tmp/app/downloads/ex6_data")

    # 2. Tạo SparkSession (sử dụng cấu hình mặc định trên Windows/Linux)
    spark = (
        SparkSession.builder
            .appName("Exercise6")
            .getOrCreate()
    )

    # 3. Đọc CSV từ thư mục giải nén (relative path)
    #    Trên Windows, Spark sẽ tìm file tương đối tại thư mục làm việc hiện tại.
    data_dir = "/var/tmp/app/downloads/ex6_data"
    data_path = os.path.join(data_dir, "*.csv")
    df = read_data(spark, data_path)

    # 4. Chạy các phân tích
    average_trip_duration_per_day(df)
    trips_per_day(df)
    most_popular_start_station_per_month(df)
    top_3_trip_stations_last_two_weeks(df)
    gender_trip_duration_comparison(df)
    top_10_ages_longest_shortest_trips(df)

    spark.stop()

if __name__ == "__main__":
    main()
