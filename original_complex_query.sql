SELECT
    ab.Title,
    ab.Author,
    ab.`Main Genre`,
    ab.`Sub Genre`,
    gr.genre AS goodreads_genre,
    COUNT(gr.reviews) AS review_count,
    (
        SELECT AVG(CAST(pb.Rating AS DECIMAL(3,2)))
        FROM Amazon_popular_books_dataset pb
        WHERE LOWER(pb.Title) = LOWER(ab.Title)
          AND LOWER(pb.Author) = LOWER(ab.Author)
    ) AS avg_popular_rating,
    (
        SELECT COUNT(*)
        FROM GoodReads_100k_books gr2
        WHERE LOWER(gr2.title) = LOWER(ab.Title)
    ) AS matching_titles_goodreads,
    (
        SELECT MAX(CAST(pb.Total_Ratings AS UNSIGNED))
        FROM Amazon_popular_books_dataset pb
        WHERE pb.Title = ab.Title
    ) AS max_total_ratings
FROM Amazon_books_dataset ab
         LEFT JOIN GoodReads_100k_books gr
                   ON LOWER(gr.title) = LOWER(ab.Title)
                       AND LOWER(gr.author) = LOWER(ab.Author)
         LEFT JOIN Amazon_popular_books_dataset apb
                   ON apb.Title = ab.Title
WHERE
    LENGTH(ab.Title) > 3
  AND CAST(ab.Rating AS DECIMAL(3,2)) > 3.5
  AND ab.Price NOT IN ('Free', 'N/A')
  AND (
          SELECT COUNT(*)
          FROM GoodReads_100k_books gr3
          WHERE gr3.genre = ab.`Main Genre`
      ) > 50
GROUP BY
    ab.Title, ab.Author, ab.`Main Genre`, ab.`Sub Genre`, gr.genre
ORDER BY avg_popular_rating DESC
LIMIT 100;
