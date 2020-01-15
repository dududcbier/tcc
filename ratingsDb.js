const dbConnector = require('./neo4jConnector.js')

const getUserRatings = userId => {
    const session = dbConnector.getSession()
    return session.run(
        `MATCH (:User {movieLensId: ${userId}})-[r:RATES]->(:Movie) 
        RETURN id(r) AS r`
    ).then(res => res.records.map(record => record.get('r')))
}

const enableRating = ratingId => {
    const session = dbConnector.getSession()
    return session.run(`MATCH (n:User)-[r:DISABLED_RATES]->(m:Movie) WHERE id(r) = ${ratingId}
    MERGE (n)-[new:RATES]->(m)
    SET new = r
    WITH r
    DELETE r`)
}

const disableRating = ratingId => {
    const session = dbConnector.getSession()
    return session.run(`MATCH (n:User)-[r:RATES]->(m:Movie) WHERE id(r) = ${ratingId}
    MERGE (n)-[new:DISABLED_RATES]->(m)
    SET new = r
    WITH r
    DELETE r`)
}

const getMovieRatings = () => {
    const session = dbConnector.getSession()
    return session.run(`
        MATCH (m:Movie)<-[r]-(u:User)
        RETURN m.movieLensId as movieId, ID(u) as userId, r.rating as rating
    `)
    .then(({records}) => records.reduce((ratings, record) => {
        const userId = record.get('userId')
        const movieId = record.get('movieId')
        const rating = record.get('rating')
        if (!ratings[movieId]) ratings[movieId] = {}
        ratings[movieId][userId] = rating
        return ratings
    }, {}))
}

module.exports = {
    getUserRatings,
    getMovieRatings,
    enableRating,
    disableRating
}