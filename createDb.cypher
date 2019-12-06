CREATE CONSTRAINT ON (user:User) ASSERT user.movieLensId IS UNIQUE;
CREATE CONSTRAINT ON (movie:Movie) ASSERT movie.movieLensId IS UNIQUE;

USING PERIODIC COMMIT
LOAD CSV WITH HEADERS FROM "file:///movielens-100k/processedUsers.csv" AS row
CREATE (u:User {
	movieLensId: toInteger(row.id)
});

USING PERIODIC COMMIT
LOAD CSV WITH HEADERS FROM "file:///movielens-100k/processedMovies.csv" AS row
CREATE (n:Movie {
	movieLensId: toInteger(row.movieId),
    title: row.title,
    genres: row.genres,
    year: toInteger(row.year)
});

USING PERIODIC COMMIT
LOAD CSV WITH HEADERS FROM "file:///movielens-100k/ratings.csv" AS row
MERGE (m:Movie {movieLensId: toInteger(row.movieId)})
MERGE (u:User {movieLensId: toInteger(row.userId)})
CREATE (u)-[:RATES {rating: toFloat(row.rating), timestamp: toInteger(row.timestamp)}]->(m);

MATCH (u:User)-[r:RATES]->() 
WITH u AS u, avg(r.rating) AS average, count(r) as count
SET u.avgRating = average, u.ratingsCount = count