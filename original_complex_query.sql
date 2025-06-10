EXPLAIN ANALYZE
SELECT
    ab.Title,
    ab.Author,
    ab.`Main Genre`,
    ab.`Sub Genre`,
    gr.genre AS goodreads_genre,
    COUNT(DISTINCT gr.reviews) AS review_count,
    (
        SELECT AVG(CAST(pb.Rating AS DECIMAL(3,2)))
        FROM Amazon_popular_books_dataset pb
        WHERE LOWER(pb.Title) = LOWER(ab.Title)
    ) AS avg_popular_rating
FROM Amazon_books_dataset ab
LEFT JOIN GoodReads_100k_books gr
    ON LOWER(gr.title) = LOWER(ab.Title)
WHERE
    LENGTH(ab.Title) > 3
  AND CAST(ab.Rating AS DECIMAL(3,2)) > 3.5
  AND ab.Price NOT IN ('Free', 'N/A')
GROUP BY
    ab.Title, ab.Author, ab.`Main Genre`, ab.`Sub Genre`, gr.genre
ORDER BY avg_popular_rating DESC
LIMIT 100;