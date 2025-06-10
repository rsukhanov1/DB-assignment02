EXPLAIN ANALYZE
WITH avg_ratings AS (
    SELECT
        LOWER(Title) AS lower_title,
        AVG(CAST(Rating AS DECIMAL(3,2))) AS avg_popular_rating
    FROM Amazon_popular_books_dataset
    GROUP BY LOWER(Title)
)

SELECT
    ab.Title,
    ab.Author,
    ab.`Main Genre`,
    ab.`Sub Genre`,
    gr.genre AS goodreads_genre,
    COUNT(DISTINCT gr.reviews) AS review_count,
    ar.avg_popular_rating
FROM Amazon_books_dataset ab
LEFT JOIN GoodReads_100k_books gr
    ON LOWER(gr.title) = LOWER(ab.Title)
LEFT JOIN avg_ratings ar
    ON ar.lower_title = LOWER(ab.Title)
WHERE
    LENGTH(ab.Title) > 3
  AND CAST(ab.Rating AS DECIMAL(3,2)) > 3.5
  AND ab.Price NOT IN ('Free', 'N/A')
GROUP BY
    ab.Title, ab.Author, ab.`Main Genre`, ab.`Sub Genre`, gr.genre, ar.avg_popular_rating
ORDER BY ar.avg_popular_rating DESC
LIMIT 100;