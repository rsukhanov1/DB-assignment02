ALTER TABLE GoodReads_100k_books
    ADD COLUMN lower_title VARCHAR(512) GENERATED ALWAYS AS (LOWER(title)) STORED;

CREATE INDEX idx_gr_lower_title ON GoodReads_100k_books (lower_title);


ALTER TABLE Amazon_books_dataset
    ADD COLUMN lower_title VARCHAR(512) GENERATED ALWAYS AS (LOWER(Title)) STORED;

CREATE INDEX idx_ab_lower_title ON Amazon_books_dataset (lower_title);


ALTER TABLE Amazon_popular_books_dataset
    ADD COLUMN lower_title VARCHAR(512) GENERATED ALWAYS AS (LOWER(Title)) STORED;


CREATE INDEX idx_ap_lower_title ON Amazon_popular_books_dataset (lower_title);


EXPLAIN ANALYZE
WITH avg_ratings AS (
    SELECT
        lower_title,
        AVG(CAST(Rating AS DECIMAL(3,2))) AS avg_popular_rating
    FROM Amazon_popular_books_dataset
    GROUP BY lower_title
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
                   ON gr.lower_title = ab.lower_title
         LEFT JOIN avg_ratings ar
                   ON ar.lower_title = ab.lower_title
WHERE
    LENGTH(ab.Title) > 3
  AND CAST(ab.Rating AS DECIMAL(3,2)) > 3.5
  AND ab.Price NOT IN ('Free', 'N/A')
GROUP BY
    ab.Title, ab.Author, ab.`Main Genre`, ab.`Sub Genre`, gr.genre, ar.avg_popular_rating
ORDER BY ar.avg_popular_rating DESC
    LIMIT 100;