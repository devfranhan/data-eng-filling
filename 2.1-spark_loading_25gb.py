
# input data: "discogs_20250201_releases.csv" 24.8 GB from Kaggle
# https://www.kaggle.com/datasets/ofurkancoban/discogs-datasets-january-2025?select=discogs_20250201_releases.csv

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType

# for job observability, check Spark UI:
# http://localhost:4050/jobs/
spark = SparkSession.builder \
    .appName("Load 25gb") \
    .config("spark.driver.bindAddress", "localhost") \
    .config("spark.ui.port", "4050") \
    .getOrCreate()

# the wrong way (java.lang.OutOfMemoryError: Java heap space):
def wrong_way():
    log(red("wrong way starting"))
    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv("/Users/franhan/alfred/data/*.csv")
    df = df.limit(10)

    log(yellow("writing data..."))

    df.coalesce(25) \
        .write \
        .mode("overwrite") \
        .parquet("/Users/franhan/alfred/data-eng-filling/output")

    return


def right_way():
    log(green("right way started"))

    schema = StructType([
        StructField("artists_artist_anv", StringType(), True),
        StructField("artists_artist_id", StringType(), True),
        StructField("artists_artist_join", StringType(), True),
        StructField("artists_artist_name", StringType(), True),
        StructField("companies_company_catno", StringType(), True),
        StructField("companies_company_entity_type", StringType(), True),
        StructField("companies_company_entity_type_name", StringType(), True),
        StructField("companies_company_id", StringType(), True),
        StructField("companies_company_name", StringType(), True),
        StructField("companies_company_resource_url", StringType(), True),
        StructField("extraartists_artist_anv", StringType(), True),
        StructField("extraartists_artist_id", StringType(), True),
        StructField("extraartists_artist_name", StringType(), True),
        StructField("extraartists_artist_role", StringType(), True),
        StructField("extraartists_artist_tracks", StringType(), True),
        StructField("format_descriptions_description", StringType(), True),
        StructField("release_formats_format_name", StringType(), True),
        StructField("release_formats_format_qty", StringType(), True),
        StructField("release_formats_format_text", StringType(), True),
        StructField("release_genres_genre", StringType(), True),
        StructField("release_identifiers_identifier_description", StringType(), True),
        StructField("release_identifiers_identifier_type", StringType(), True),
        StructField("release_identifiers_identifier_value", StringType(), True),
        StructField("release_labels_label_catno", StringType(), True),
        StructField("release_labels_label_id", StringType(), True),
        StructField("release_labels_label_name", StringType(), True),
        StructField("release_series_series_catno", StringType(), True),
        StructField("release_series_series_id", StringType(), True),
        StructField("release_series_series_name", StringType(), True),
        StructField("release_styles_style", StringType(), True),
        StructField("release_videos_video_duration", StringType(), True),
        StructField("release_videos_video_embed", StringType(), True),
        StructField("release_videos_video_src", StringType(), True),
        StructField("releases_release_country", StringType(), True),
        StructField("releases_release_data_quality", StringType(), True),
        StructField("releases_release_id", StringType(), True),
        StructField("releases_release_master_id", StringType(), True),
        StructField("releases_release_master_id_is_main_release", StringType(), True),
        StructField("releases_release_notes", StringType(), True),
        StructField("releases_release_released", StringType(), True),
        StructField("releases_release_status", StringType(), True),
        StructField("releases_release_title", StringType(), True),
        StructField("sub_tracks_track_duration", StringType(), True),
        StructField("sub_tracks_track_position", StringType(), True),
        StructField("sub_tracks_track_title", StringType(), True),
        StructField("tracklist_track_duration", StringType(), True),
        StructField("tracklist_track_position", StringType(), True),
        StructField("tracklist_track_title", StringType(), True),
        StructField("videos_video_description", StringType(), True),
        StructField("videos_video_title", StringType(), True)
    ])

    df = spark.readStream \
        .format("csv") \
        .schema(schema) \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .option("maxFilesPerTrigger", "1") \
        .option("maxBytesPerTrigger", "250MB") \
        .load("/Users/franhan/alfred/data/")

    df = df.limit(100000)
    df = df.repartition(5)

    log(yellow("writing data..."))

    df.writeStream \
        .format("parquet") \
        .option("path", "/Users/franhan/alfred/data-eng-filling/output") \
        .option("checkpointLocation", "/Users/franhan/alfred/data-eng-filling/checkpoint") \
        .outputMode("append") \
        .start() \
        .awaitTermination()

    return

def main():
    #wrong_way()
    right_way()
    log(green('success! :)'))
    return

# some color for better learning
def green(s):
    return '\033[1;32m%s\033[m' % s


def yellow(s):
    return '\033[1;33m%s\033[m' % s


def red(s):
    return '\033[1;31m%s\033[m' % s


def log(*m):
    print(" ".join(map(str, m)))


if __name__ == "__main__":
    main()

# the wrong way

# dfr = data for research
# dfr = df.filter(F.col("videos_video_title") != "NULL").limit(500)
# dfr.cache()
# print(dfr.columns)

# for column in dfr.columns:
#     dfr.select(column).filter(F.col(column) != "NULL").show(1, truncate=False)

# dfr.select("videos_video_title", "tracklist_track_duration").show(50, truncate=False)

# df.write.mode("overwrite").parquet("/output")


