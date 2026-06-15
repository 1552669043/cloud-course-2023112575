import time
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

LOCAL_PATH = "/opt/spark/work/douban_movies.csv"
SPARK_PATH = "file:///opt/spark/work/douban_movies.csv"

spark = SparkSession.builder.appName("DoubanBenchmark").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

executor_instances = spark.conf.get("spark.executor.instances", "unknown")

df_raw = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("multiLine", "true")
    .option("quote", '"')
    .option("escape", '"')
    .csv(SPARK_PATH)
)

df = (
    df_raw
    .withColumn("year", F.col("year").cast("int"))
    .withColumn("rating_score", F.col("rating_score").cast("double"))
    .dropna(subset=["rating_score", "year"])
    .withColumn("decade", F.floor(F.col("year") / 10) * 10)
    .cache()
)
total = df.count()
df.createOrReplaceTempView("movies")

print(f"BENCHMARK_ROWS={total}")
print(f"EXECUTOR_INSTANCES={executor_instances}")

pdf = pd.read_csv(LOCAL_PATH)
pdf["year"] = pd.to_numeric(pdf["year"], errors="coerce")
pdf["rating_score"] = pd.to_numeric(pdf["rating_score"], errors="coerce")
pdf = pdf.dropna(subset=["year", "rating_score"])
pdf["decade"] = (pdf["year"].astype(int) // 10) * 10

t0 = time.time()
pd_result = (
    pdf.groupby("decade")
    .agg(movie_count=("movie_id", "count"), avg_rating=("rating_score", "mean"))
    .reset_index()
    .sort_values("decade")
)
pandas_time = time.time() - t0

print("=== Pandas Result ===")
print(pd_result.round(2).to_string(index=False))
print(f"PANDAS_SECONDS={pandas_time:.4f}")

sql = """
SELECT decade,
       COUNT(*) AS movie_count,
       ROUND(AVG(rating_score), 2) AS avg_rating
FROM movies
GROUP BY decade
ORDER BY decade
"""

t0 = time.time()
spark_result = spark.sql(sql).collect()
spark_time = time.time() - t0

print("=== PySpark Result ===")
spark.sql(sql).show(50, truncate=False)
print(f"PYSPARK_SECONDS={spark_time:.4f}")
print(f"PERF_RESULT executor_instances={executor_instances} pandas_seconds={pandas_time:.4f} pyspark_seconds={spark_time:.4f} speedup={pandas_time / spark_time if spark_time > 0 else 0:.4f}")

spark.stop()
