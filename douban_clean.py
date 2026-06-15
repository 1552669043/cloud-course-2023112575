from pyspark.sql import SparkSession
from pyspark.sql import functions as F

PATH = "file:///opt/spark/work/douban_movies.csv"

spark = SparkSession.builder.appName("DoubanDataCleaning").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

df_raw = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("multiLine", "true")
    .option("quote", '"')
    .option("escape", '"')
    .csv(PATH)
)

print("=== Schema ===")
df_raw.printSchema()

print("=== First 5 Rows ===")
df_raw.select(
    "movie_id", "title", "year", "rating_score", "rating_count",
    "genres", "countries", "directors"
).show(5, truncate=False)

total = df_raw.count()
print(f"RAW_ROW_COUNT={total}")

print("=== Missing Ratio ===")
for c in df_raw.columns:
    n = df_raw.filter(F.col(c).isNull() | (F.trim(F.col(c).cast("string")) == "")).count()
    print(f"{c:20s} missing={n:6d} ratio={n / total * 100:.2f}%")

df = (
    df_raw
    .withColumn("year", F.col("year").cast("int"))
    .withColumn("rating_score", F.col("rating_score").cast("double"))
    .withColumn("rating_count", F.col("rating_count").cast("long"))
    .withColumn("collect_count", F.col("collect_count").cast("long"))
)

df1 = df.dropna(subset=["rating_score"])
after_drop = df1.count()
print("=== Cleaning Strategy 1: dropna(rating_score) ===")
print(f"before={total}, after={after_drop}, dropped={total - after_drop}")

median_year = int(df1.approxQuantile("year", [0.5], 0.01)[0])
df_clean = df1.fillna({
    "year": median_year,
    "genres": "Unknown",
    "countries": "Unknown",
    "directors": "Unknown",
    "summary": "No summary"
})

final_count = df_clean.count()
print("=== Cleaning Strategy 2: fillna(year/genres/countries/directors/summary) ===")
print(f"median_year={median_year}")
print(f"CLEAN_ROW_COUNT={final_count}")
print(f"DROPPED_TOTAL={total - final_count}")
print(f"DROPPED_RATIO={(total - final_count) / total * 100:.2f}%")

print("=== Basic Statistics ===")
df_clean.select("year", "rating_score", "rating_count", "collect_count").describe().show(truncate=False)

print("=== Genre Distribution After Cleaning ===")
df_clean.groupBy("genres").count().orderBy(F.desc("count")).show(10, truncate=False)

spark.stop()
