WITH cleaned_movies AS (
    SELECT TRY_CAST("year" AS INTEGER) AS year_num, "MID"
    FROM "DB_IMDB"."DB_IMDB"."MOVIE"
    WHERE year_num IS NOT NULL
),
start_years AS (
    SELECT DISTINCT year_num AS start_year
    FROM cleaned_movies
),
period_counts AS (
    SELECT s.start_year, COUNT(c."MID") AS total_count
    FROM start_years s
    JOIN cleaned_movies c
        ON c.year_num BETWEEN s.start_year AND s.start_year + 9
    GROUP BY s.start_year
)
SELECT start_year, total_count
FROM period_counts
ORDER BY total_count DESC, start_year ASC
LIMIT 1