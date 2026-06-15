from pyspark.sql import SparkSession
from pyspark.sql import functions as F

PATH = "file:///opt/spark/work/douban_movies.csv"

spark = SparkSession.builder.appName("DoubanSQLAnalysis").getOrCreate()
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

df = (
    df_raw
    .withColumn("year", F.col("year").cast("int"))
    .withColumn("rating_score", F.col("rating_score").cast("double"))
    .withColumn("rating_count", F.col("rating_count").cast("long"))
    .withColumn("collect_count", F.col("collect_count").cast("long"))
    .dropna(subset=["rating_score"])
)

median_year = int(df.approxQuantile("year", [0.5], 0.01)[0])
df = df.fillna({
    "year": median_year,
    "genres": "Unknown",
    "countries": "Unknown",
    "directors": "Unknown",
    "summary": "No summary"
})

movies = df.select(
    "movie_id", "title", "year", "rating_score", "rating_count",
    "genres", "countries", "directors", "collect_count"
)
movies.createOrReplaceTempView("movies")

genre_movies = (
    movies
    .select("movie_id", F.explode(F.split(F.col("genres"), "/")).alias("genre"))
    .where(F.col("genre") != "Unknown")
)
genre_movies.createOrReplaceTempView("genre_movies")

print(f"ANALYSIS_ROW_COUNT={movies.count()}")

print("=== Q1 GROUP BY: genre rating statistics ===")
q1 = spark.sql("""
SELECT gm.genre,
       COUNT(*) AS movie_count,
       ROUND(AVG(m.rating_score), 2) AS avg_rating,
       ROUND(AVG(m.rating_count), 0) AS avg_rating_count
FROM genre_movies gm
JOIN movies m ON gm.movie_id = m.movie_id
GROUP BY gm.genre
ORDER BY movie_count DESC
LIMIT 15
""")
q1.show(15, truncate=False)
print("Analysis Q1: The result shows which genres have the largest sample size and compares average rating across genres. A genre with many movies is not always the genre with the highest average score, so both count and rating should be considered.")

print("=== Q2 ORDER BY Top-N: highest rated movies ===")
q2 = spark.sql("""
SELECT title, year, genres, countries, rating_score, rating_count
FROM movies
WHERE rating_count >= 10000
ORDER BY rating_score DESC, rating_count DESC
LIMIT 10
""")
q2.show(10, truncate=False)
print("Analysis Q2: Top-N ranking uses rating score first and rating count second. Filtering by rating_count avoids unstable rankings caused by movies with very few raters.")

print("=== Q3 Time Trend: movies by decade ===")
q3 = spark.sql("""
SELECT FLOOR(year / 10) * 10 AS decade,
       COUNT(*) AS movie_count,
       ROUND(AVG(rating_score), 2) AS avg_rating,
       ROUND(AVG(rating_count), 0) AS avg_rating_count
FROM movies
WHERE year IS NOT NULL
GROUP BY FLOOR(year / 10) * 10
ORDER BY decade
""")
q3.show(30, truncate=False)
print("Analysis Q3: Grouping by decade shows the time trend of movie quantity and rating. This is a time dimension analysis and can reveal which periods have denser movie records or higher average scores.")

print("=== Q4 JOIN + WINDOW: top 3 movies per genre ===")
q4 = spark.sql("""
WITH genre_stats AS (
    SELECT gm.genre,
           COUNT(*) AS genre_movie_count,
           ROUND(AVG(m.rating_score), 2) AS genre_avg_rating
    FROM genre_movies gm
    JOIN movies m ON gm.movie_id = m.movie_id
    GROUP BY gm.genre
),
ranked AS (
    SELECT gm.genre,
           m.title,
           m.year,
           m.rating_score,
           m.rating_count,
           ROW_NUMBER() OVER (
             PARTITION BY gm.genre
             ORDER BY m.rating_score DESC, m.rating_count DESC
           ) AS rn
    FROM genre_movies gm
    JOIN movies m ON gm.movie_id = m.movie_id
    WHERE m.rating_count >= 10000
)
SELECT r.genre, s.genre_movie_count, s.genre_avg_rating,
       r.rn, r.title, r.year, r.rating_score, r.rating_count
FROM ranked r
JOIN genre_stats s ON r.genre = s.genre
WHERE r.rn <= 3
ORDER BY r.genre, r.rn
""")
q4.show(80, truncate=False)
print("Analysis Q4: This query combines JOIN and window ranking. It first expands multi-genre movies, then ranks movies inside each genre, which is useful for comparing representative high-score movies across categories.")

spark.stop()
