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

const getUserBasedRecommendations = async (userIds, n, userRelationship, k) => {
    const session = dbConnector.getSession()     
    return session.run(`
    MATCH (u:User)-[sim:${userRelationship}]-(v:User)-[r:RATES]->(m:Movie)
    WHERE NOT (u)-[:RATES]->(m) AND u.movieLensId IN [${userIds}]
    WITH u, v, m, sim, r
    ORDER BY sim.similarity DESC
    WITH u, m, collect({normalizedRating: r.rating - v.avgRating, similarity: sim.similarity})[..${k}] as similarUsers
    WITH u, m, u.avgRating + reduce(num = 0, v in similarUsers | num + v.similarity * v.normalizedRating) / reduce(den = 0, v in similarUsers | den + abs(v.similarity)) as score
    ORDER BY score DESC
    WITH u, collect({m: m.movieLensId, score: score})[..${n}] as recommendations
    UNWIND recommendations as rec
    MATCH (m:Movie {movieLensId: rec.m})
    MERGE (u)-[r:PROBABLY_LIKES_UB {score: rec.score}]->(m)
    RETURN count(r) AS count`)
    .then(res => res.records[0].get('count'))
}

const getItemBasedRecommendations = (userIds, n, movieRelationship, k) => {
    const session = dbConnector.getSession() 
    return session.run(`
    MATCH (u:User)-[r:RATES]->(m:Movie)-[sim:${movieRelationship}]-(similarMovies:Movie) 
    WHERE NOT (u)-[:RATES]->(similarMovies) AND u.movieLensId IN [${userIds}]
    WITH u, similarMovies, r, sim
    ORDER BY sim.similarity DESC
    WITH u, similarMovies, collect({rating: r.rating, similarity: sim.similarity})[..${k}] as ratings
    WITH u, similarMovies, reduce(num = 0, rating in ratings | num + rating.similarity * rating.rating) as num, reduce(den = 0, rating in ratings | den + abs(rating.similarity)) as den
    WITH u, similarMovies, num / den as score
    ORDER BY score DESC
    WITH u, collect({m: similarMovies.movieLensId, score: score})[..${n}] as recommendations
    UNWIND recommendations as rec
    MATCH (m:Movie {movieLensId: rec.m})
    MERGE (u)-[r:PROBABLY_LIKES_IB {score: rec.score}]->(m)
    RETURN count(r) AS count`)
    .then(res => res.records[0].get('count'))
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
    const [tp, fp, fn] = await Promise.all([
        session.run(`
            MATCH (u:User {movieLensId: ${userId}})-[r:DISABLED_RATES]->(m:Movie)
            WHERE 
                r.rating >= 3.5 AND
                (u)-[:PROBABLY_LIKES_${suffix}]->(m)
            RETURN count(m) AS tp
        `).then(res => res.records[0].get('tp')),
        session.run(`
            MATCH (:User {movieLensId: ${userId}})-[:PROBABLY_LIKES_${suffix}]->(m:Movie)
            OPTIONAL MATCH (:User {movieLensId: ${userId}})-[r:DISABLED_RATES]->(m:Movie)
            WITH r, m
            WHERE r IS NULL OR r.rating < 3.5
            RETURN count(m) AS fp
        `).then(res => res.records[0].get('fp')),
        session.run(`
            MATCH (u:User {movieLensId: ${userId}})-[r:DISABLED_RATES]->(m:Movie)
            WHERE 
                r.rating >= 3.5 AND 
                NOT (u)-[:PROBABLY_LIKES_${suffix}]->(m)
            RETURN count(m) AS fn
        `).then(res => res.records[0].get('fn'))
    ])
    return {tp, fp, fn}
}


const calculateAllCossineSimilarities = (movieLensId, similarityThreshold = -1) => {
    const session = dbConnector.getSession()
    return session.run(`
        MATCH (u:User {movieLensId: ${movieLensId}})-[uRates:RATES]->(m:Movie)
        MATCH (v:User)-[vRates:RATES]->(m:Movie) WHERE id(u) < id(v)
        WITH  u as u,  
            v as v, 
            collect(uRates.rating - m.avgRating) as uRates,  
            collect(vRates.rating - m.avgRating) as vRates, 
            count(m) as inCommon
        UNWIND [inCommon / 30.0, 1] as weight
        WITH u AS u,
            v AS v,
            algo.similarity.cosine(uRates, vRates) AS similarity,
            min(weight) as weight
        WHERE abs(similarity * weight) > ${similarityThreshold}
        MERGE (u)-[s:COS_SIM {similarity: similarity * weight}]->(v)
    `)
}
const calculateAllPearsonSimilarities = (movieLensId, similarityThreshold = -1) => {
    const session = dbConnector.getSession()
    return session.run(`
        MATCH (u:User {movieLensId: ${movieLensId}})-[urates:RATES]->(movie:Movie)
        WITH u, algo.similarity.asVector(movie, urates.rating) AS uVector
        MATCH (v:User)-[vrates:RATES]->(movie:Movie) WHERE id(u) < id(v)
        WITH u, v, uVector, algo.similarity.asVector(movie, vrates.rating) AS vVector
        WITH u, v, algo.similarity.pearson(uVector, vVector, {vectorType: "maps"}) AS similarity
        MATCH (u:User)-[:RATES]->(m:Movie)<-[:RATES]-(v:User)
        WITH u, v, similarity, count(m) as inCommon
        WITH u, v, similarity, CASE WHEN inCommon >= 30 THEN 1 ELSE inCommon / 30.0 END AS weight
        WHERE abs(similarity * weight) > ${similarityThreshold}
        MERGE (u)-[:PEARS_SIM {similarity: similarity * weight}]->(v)
    `)
}

const clearSimilarities = () => {
    const session = dbConnector.getSession()
    return session.run(`call apoc.periodic.iterate(
        "MATCH ()-[r:COS_SIM|:PEARS_SIM]->() RETURN r", "DELETE r",  {batchSize:10000}
    )
    yield batches, total return batches, total`)
}

const eraseBottomRecommendations = (targetN, recommendationTypes) => {
    const session = dbConnector.getSession()
    return Promise.all(recommendationTypes.map(recType => 
        session.run(`
            MATCH (u:User)-[r:PROBABLY_LIKES_${recType}]->(:Movie)
            WITH u, count(r) as n
            WHERE n > ${targetN}
            RETURN u, n`)
        .then(res => Promise.all(res.records.map(record => {
            const userId = record.get('u').properties.movieLensId
            const n = record.get('n')
            return session.run(`
                MATCH (:User {movieLensId: ${userId}})-[r:PROBABLY_LIKES_${recType}]->(:Movie)
                WITH r
                ORDER BY r.score
                LIMIT ${n - targetN}
                DELETE r
            `)
        })))
    ))
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
    calculateUsersAvgRating,
    clearSimilarities,
    eraseBottomRecommendations
}