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

->

Explain analize for the first non optimized query!
38 sec

-> Limit: 100 row(s)  (actual time=36875..36876 rows=100 loops=1)
-> Sort: avg_popular_rating DESC, limit input to 100 row(s) per chunk  (actual time=36875..36876 rows=100 loops=1)
-> Stream results  (actual time=36850..36873 rows=7360 loops=1)
-> Group aggregate: count(distinct goodreads_100k_books.reviews)  (actual time=36850..36868 rows=7360 loops=1)
-> Sort: ab.Title, ab.Author, ab.`Main Genre`, ab.`Sub Genre`, gr.genre  (actual time=36850..36854 rows=7478 loops=1)
-> Stream results  (cost=54.9e+6 rows=548e+6) (actual time=189..36823 rows=7478 loops=1)
-> Left hash join (<hash>(lower(gr.title))=<hash>(lower(ab.Title))), extra conditions: (lower(gr.title) = lower(ab.Title))  (cost=54.9e+6 rows=548e+6) (actual time=185..396 rows=7478 loops=1)
-> Filter: ((length(ab.Title) > 3) and (cast(ab.Rating as decimal(3,2)) > 3.5) and (ab.Price not in ('Free','N/A')))  (cost=810 rows=6031) (actual time=0.0324..6.08 rows=7439 loops=1)
-> Table scan on ab  (cost=810 rows=7539) (actual time=0.0281..4.26 rows=7928 loops=1)
-> Hash
-> Table scan on gr  (cost=20 rows=90892) (actual time=0.0147..97.1 rows=100000 loops=1)

This is non optimized query because of using small subqueries ' as () ' while not using CTE; 
Using 'LOWER' in 'JOIN' and 'WHERE' blocks using indexes!
'CAST' in 'WHERE' makes it more harder to calculate and avoids using indexes;
'DISTINCT' in 'COUNT' is bad practice due to disitnct complexity when we need only to count lines;
Using 'LIMIT' without indexes forces sql to do query for all lines and ony then limit it by LIMIT;
