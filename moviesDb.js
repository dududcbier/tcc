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

const getTestSet = (userId, recSuffix) => {
    const session = dbConnector.getSession()
    return session.run(`
        MATCH (m:Movie)<-[r:DISABLED_RATES]-(u:User {movieLensId: ${userId}})
        MATCH (m:Movie)<-[pred:PROBABLY_LIKES_${recSuffix}]-(u:User)
        RETURN m.movieLensId AS movieId, r.rating as rating, pred.score as predictedScore
    `).then(res => res.records.map(record => ({movieId: record.get('movieId'), rating: record.get('rating'), predictedScore: record.get('predictedScore') || 0})))
}

const calculateAllCossineSimilarities = (movieLensId) => {
    const session = dbConnector.getSession()
    return session.run(`
        MATCH (m1:Movie {movieLensId: ${movieLensId}})<-[rates1:RATES]-(u:User)
        MATCH (m2:Movie)<-[rates2:RATES]-(u:User) WHERE id(m1) < id(m2)
        WITH  m1 as m1, m2 as m2, collect(rates1.rating) as rates1,  collect(rates2.rating) as rates2, count(rates2) as inCommon
        UNWIND [inCommon / 50.0, 1] as weight
        WITH m1 AS m1,
            m2 AS m2,
            algo.similarity.cosine(rates1, rates2) AS similarity,
            min(weight) as weight
        WHERE weight * similarity > 0.5
        MERGE (m1)-[s:COS_SIM {similarity: similarity * weight}]->(m2)
    `)
}

const calculateAllPearsonSimilarities = (movieLensId) => {
    const session = dbConnector.getSession()
    return session.run(`
        MATCH (m1:Movie {movieLensId: ${movieLensId}})<-[rates:RATES]-(user:User)
        WITH m1, algo.similarity.asVector(user, rates.rating) AS vector1
        MATCH (m2:Movie)<-[rates:RATES]-(user:User) WHERE id(m1) < id(m2)
        WITH m1, m2, vector1, algo.similarity.asVector(user, rates.rating) AS vector2
        WITH m1 AS m1,
               m2 AS m2,
               algo.similarity.pearson(vector1, vector2, {vectorType: "maps"}) AS similarity
        MATCH (m1:Movie)<-[:RATES]-(u:User)-[:RATES]->(m2:Movie)
        WITH m1, m2, similarity, count(u) as inCommon
        UNWIND [inCommon / 50.0, 1] as weight
        WITH m1, m2, similarity, min(weight) as weight
        WHERE similarity * weight > 0.25
        MERGE (m1)-[:PEARS_SIM {similarity: similarity * weight}]->(m2)
    `)
}

const predictRatingUB = (userId, movieId, userRelationship) => {
    const session = dbConnector.getSession()
    return session.run(`
        MATCH (u:User {movieLensId: ${userId}})-[sim:${userRelationship}]-(v:User)-[r:RATES]->(m:Movie {movieLensId: ${movieId}})
        WITH u, collect({avgRating: v.avgRating, rating: r.rating, similarity: sim.similarity}) as ratings
        WITH u, reduce(num = 0, rating in ratings | num + rating.similarity * (rating.rating - rating.avgRating)) as num, reduce(den = 0, rating in ratings | den + rating.similarity) as den
        RETURN u.avgRating + num / den as score
    `)
}

const predictRatingIB = (userId, movieId, movieRelationship) => {
    const session = dbConnector.getSession()
    return session.run(`
        MATCH (u:User {movieLensId: ${userId}})-[r:RATES]->(m:Movie)-[sim:${movieRelationship}]-(n:Movie {movieLensId: ${movieId}})
        WITH collect({rating: r.rating, similarity: sim.similarity}) as ratings
        WITH reduce(num = 0, rating in ratings | num + rating.similarity * rating.rating) as num, reduce(den = 0, rating in ratings | den + rating.similarity) as den
        RETURN num / den as score
    `)
}
module.exports = {
    get,
    calculateAllPearsonSimilarities,
    calculateAllCossineSimilarities,
    getNeighbors,
    getTestSet,
    predictRatingIB,
    predictRatingUB
}