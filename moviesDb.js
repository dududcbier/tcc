const dbConnector = require('./neo4jConnector.js')

const get = () => {
    const session = dbConnector.getSession()
    return session.run(`MATCH (m:Movie) RETURN m`)
    .then(res => res.records.map(record => record.get('m').properties))
}

const getNeighbors = (movieId, distinct = true) => {
    const session = dbConnector.getSession()
    return session.run(
        `MATCH (m:Movie {movieLensId: ${movieId}})<-[:RATES]-(:User)-[:RATES]->(n:Movie)
        RETURN ${distinct ? 'DISTINCT' : ''} n`
    )
    .then(res => res.records.map(record => record.get('n').properties))
}

const getHitSet = (userId, recSuffix) => {
    const session = dbConnector.getSession()
    return session.run(`
        MATCH (m:Movie)<-[r:DISABLED_RATES]-(u:User {movieLensId: ${userId}})
        MATCH (m)<-[pred:PROBABLY_LIKES_${recSuffix}]-(u)
        RETURN m.movieLensId AS movieId, r.rating as rating, pred.score as predictedScore
    `).then(res => res.records.map(record => ({movieId: record.get('movieId'), rating: record.get('rating'), predictedScore: record.get('predictedScore')})))
}

const calculateAllCossineSimilarities = (movieLensId, similarityThreshold) => {
    const session = dbConnector.getSession()
    return session.run(`
        MATCH (m1:Movie {movieLensId: ${movieLensId}})<-[rates1:RATES]-(u:User)
        MATCH (m2:Movie)<-[rates2:RATES]-(u:User) WHERE id(m1) < id(m2)
        WITH  m1 as m1, m2 as m2, collect(rates1.rating - u.avgRating) as rates1,  collect(rates2.rating - u.avgRating) as rates2, count(rates2) as inCommon
        UNWIND [inCommon / 61.0, 1] as weight
        WITH m1 AS m1,
            m2 AS m2,
            algo.similarity.cosine(rates1, rates2) AS similarity,
            min(weight) as weight
        WHERE abs(similarity * weight) > ${similarityThreshold}
        MERGE (m1)-[s:COS_SIM {similarity: similarity * weight}]->(m2)
    `)
}

const calculateAllPearsonSimilarities = (movieLensId, similarityThreshold) => {
    const session = dbConnector.getSession()
    return session.run(`
        MATCH (m1:Movie {movieLensId: ${movieLensId}})<-[rates1:RATES]-(user:User)
        WITH m1, algo.similarity.asVector(user, rates1.rating) AS vector1
        MATCH (m2:Movie)<-[rates2:RATES]-(user:User) WHERE id(m1) < id(m2)
        WITH m1, m2, vector1, algo.similarity.asVector(user, rates2.rating) AS vector2
        WITH m1, m2, algo.similarity.pearson(vector1, vector2, {vectorType: "maps"}) AS similarity
        MATCH (m1:Movie)<-[:RATES]-(u:User)-[:RATES]->(m2:Movie)
        WITH m1, m2, similarity, count(u) as inCommon
        UNWIND [inCommon / 61.0, 1] as weight
        WITH m1, m2, similarity, min(weight) as weight
        WHERE abs(weight * similarity) > ${similarityThreshold}
        MERGE (m1)-[:PEARS_SIM {similarity: similarity * weight}]->(m2)
    `)
}

const predictRatingUB = (userId, movieId, userRelationship, k) => {
    const session = dbConnector.getSession()
    return session.run(`
        MATCH (u:User {movieLensId: ${userId}})-[sim:${userRelationship}]-(v:User)-[r:RATES]->(m:Movie {movieLensId: ${movieId}})
        WITH u, v, sim, r
        ORDER BY sim.similarity DESC
        LIMIT ${k}
        WITH u, collect({avgRating: v.avgRating, rating: r.rating, similarity: sim.similarity}) as ratings
        WITH u, reduce(num = 0, rating in ratings | num + rating.similarity * (rating.rating - rating.avgRating)) as num, reduce(den = 0, rating in ratings | den + rating.similarity) as den
        RETURN u.avgRating + num / den as score
    `)
    .then(res => res.records[0] ? res.records[0].get('score') : 0)
}

const predictRatingIB = (userId, movieId, movieRelationship, k) => {
    const session = dbConnector.getSession()
    return session.run(`
        MATCH (u:User {movieLensId: ${userId}})-[r:RATES]->(m:Movie)-[sim:${movieRelationship}]-(n:Movie {movieLensId: ${movieId}})
        WITH r, sim
        ORDER BY sim.similarity DESC
        LIMIT ${k}
        WITH collect({rating: r.rating, similarity: sim.similarity}) as ratings
        WITH reduce(num = 0, rating in ratings | num + rating.similarity * rating.rating) as num, reduce(den = 0, rating in ratings | den + rating.similarity) as den
        RETURN num / den as score
    `)
    .then(res => res.records[0].get('score'))
    .catch(e => console.log({userId, movieId, movieRelationship, e}))
}

const calculateAvgRatings = () => {
    const session = dbConnector.getSession()
    return session.run(`
        MATCH (:User)-[r:RATES]->(m:Movie) 
        WITH m, avg(r.rating) AS average, count(r) as count
        SET m.avgRating = average, m.ratingsCount = count
    `)
}

module.exports = {
    get,
    calculateAllPearsonSimilarities,
    calculateAllCossineSimilarities,
    getNeighbors,
    getHitSet,
    predictRatingIB,
    predictRatingUB,
    calculateAvgRatings
}