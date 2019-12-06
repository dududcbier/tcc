const dbConnector = require('./neo4jConnector.js')

const get = () => {
    const session = dbConnector.getSession()
    return session.run(`MATCH (u:User) RETURN u`)
    .then(res => res.records.map(record => record.get('u').properties))
}

const getNeighbors = (userId, DISTINCT = true) => {
    const session = dbConnector.getSession()
    return session.run(`MATCH (u:User {movieLensId: ${userId}})-[r:RATES]->(m:Movie)<-[t:RATES]-(v:User) RETURN ${DISTINCT ? 'DISTINCT' : ''} v`)
    .then(res => res.records.map(record => record.get('v').properties))
}

const calculateUsersAvgRating = () => {
    const session = dbConnector.getSession()
    return session.run(`
        MATCH (u:User)-[r:RATES]->() 
        WITH u, avg(r.rating) AS average, count(r) as count
        SET u.avgRating = average, u.ratingsCount = count
    `)
}

const getUserBasedRecommendations = (userId, n, userRelationship) => {
    const session = dbConnector.getSession() 
    return session.run(`MATCH (u:User {movieLensId: ${userId}})-[sim:${userRelationship}]-(v:User)-[r:RATES]->(m:Movie) WHERE NOT (u)-[:RATES]->(m)
    WITH u, m, collect({normalizedRating: r.rating - v.avgRating, similarity: sim.similarity}) as ratings
    WITH u, m, u.avgRating + reduce(num = 0, rating in ratings | num + rating.similarity * rating.normalizedRating) / reduce(den = 0, rating in ratings | den + rating.similarity) as score
    ORDER BY score DESC
    LIMIT ${n}
    MERGE (u)-[:PROBABLY_LIKES_UB {score: score}]->(m)`)    
}

const getItemBasedRecommendations = (userId, n, movieRelationship) => {
    const session = dbConnector.getSession() 
    return session.run(`MATCH (u:User {movieLensId: ${userId}})-[r:RATES]->(m:Movie)-[sim:${movieRelationship}]-(similarMovies:Movie) WHERE NOT (u)-[:RATES]->(similarMovies)
    WITH u, similarMovies, collect({rating: r.rating, similarity: sim.similarity}) as ratings
    WITH u, similarMovies, reduce(num = 0, rating in ratings | num + rating.similarity * rating.rating) as num, reduce(den = 0, rating in ratings | den + rating.similarity) as den
    WITH u, similarMovies, num / den as score
    ORDER BY score DESC
    LIMIT ${n}
    MERGE (u)-[:PROBABLY_LIKES_IB {score: score}]->(similarMovies)`)    
}

const markAsTestUser = userId => {
    const session = dbConnector.getSession()
    return session.run(`MATCH (u:User {movieLensId: ${userId}}) SET u.testUser = true`)
}

const getTestUsers = () => {
    const session = dbConnector.getSession()
    return session.run(`MATCH (u:User) WHERE u.testUser = true RETURN u`)
    .then(res => res.records.map(record => record.get('u').properties))
}

const getF1Info = async (userId, suffix) => {
    const session = dbConnector.getSession()
    const [hits, recset, testset] = await Promise.all([
        session.run(`
            MATCH (:User {movieLensId: ${userId}})-[:DISABLED_RATES]->(m:Movie)
            MATCH (:User {movieLensId: ${userId}})-[:PROBABLY_LIKES_${suffix}]->(m:Movie)
            RETURN count(m) AS hits
        `).then(res => res.records[0].get('hits')),
        session.run(`
            MATCH (:User {movieLensId: ${userId}})-[:PROBABLY_LIKES_${suffix}]->(m:Movie)
            RETURN count(m) AS recset
        `).then(res => res.records[0].get('recset')),
        session.run(`
            MATCH (:User {movieLensId: ${userId}})-[:DISABLED_RATES]->(m:Movie)
            RETURN count(m) AS testset
        `).then(res => res.records[0].get('testset'))
    ])
    return {hits, recset, testset}
}


const calculateAllCossineSimilarities = (movieLensId, similarityThreshold = 0.5) => {
    const session = dbConnector.getSession()
    return session.run(`
        MATCH (u:User {movieLensId: ${movieLensId}})-[uRates:RATES]->(m:Movie)
        MATCH (v:User)-[vRates:RATES]->(m:Movie) WHERE id(u) < id(v)
        WITH  u as u,  v as v, collect(uRates.rating) as uRates,  collect(vRates.rating) as vRates, count(vRates) as inCommon
        UNWIND [inCommon / 50.0, 1] as weight
        WITH u AS u,
            v AS v,
            algo.similarity.cosine(uRates, vRates) AS similarity,
            min(weight) as weight
        WHERE weight * similarity > ${similarityThreshold}
        MERGE (u)-[s:COS_SIM {similarity: similarity * weight}]->(v)
    `)
}

const calculateAllPearsonSimilarities = (movieLensId, similarityThreshold = 0.5) => {
    const session = dbConnector.getSession()
    return session.run(`
        MATCH (u:User {movieLensId: ${movieLensId}})-[rates:RATES]->(movie:Movie)
        WITH u, algo.similarity.asVector(movie, rates.rating) AS uVector
        MATCH (v:User)-[vrates:RATES]->(movie:Movie) WHERE id(u) < id(v)
        WITH u, v, uVector, algo.similarity.asVector(movie, vrates.rating) AS vVector
        WITH u AS u,
            v AS v,
            algo.similarity.pearson(uVector, vVector, {vectorType: "maps"}) AS similarity
        MATCH (u:User)-[:RATES]->(m:Movie)<-[:RATES]-(v:User)
        WITH u, v, similarity, count(m) as inCommon
        UNWIND [inCommon / 50.0, 1] as weight
        WITH u, v, similarity, min(weight) as weight
        WHERE similarity * weight > ${similarityThreshold}
        MERGE (u)-[:PEARS_SIM {similarity: similarity * weight}]->(v)
    `)
}

module.exports = {
    get,
    getNeighbors,
    getUserBasedRecommendations,
    getItemBasedRecommendations,
    markAsTestUser,
    getTestUsers,
    getF1Info,
    calculateAllPearsonSimilarities,
    calculateAllCossineSimilarities,
    calculateUsersAvgRating
}