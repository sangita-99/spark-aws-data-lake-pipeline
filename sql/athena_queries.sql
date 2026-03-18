SELECT genre, COUNT(*) AS total_movies
FROM movies
GROUP BY genre
ORDER BY total_movies DESC;