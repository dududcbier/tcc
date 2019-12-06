const parser = require('argv-parser')
const _cliProgress = require('cli-progress');
const dbConnector = require('./neo4jConnector.js')
const moviesDb = require('./moviesDb.js')
const usersDb = require('./usersDb.js')
const ratingsDb = require('./ratingsDb.js')
const fs = require('fs')

const progressBar = new _cliProgress.SingleBar({}, _cliProgress.Presets.shades_classic);

const rules = {
    walks: {
        type: Number,
        value: 100
	},
	k: {
		type: Number,
		value: 10
	},
    skipRecommendations: {
        type: Boolean,
        value: false
	},
	userRelationship: {
		type: String
	},
	movieRelationship: {
		type: String
	},
	output: {
		type: String,
		value: `output_${new Date().valueOf()}`.txt
	}
}

const scenarios = {
	ratingsPercentage: [25],
    n: [10, 25, 50],
    steps: [3, 5]
}

const options = parser.parse(process.argv, { rules })
const k = options.parsed.k

const userRelationship = options.parsed.userRelationship
const movieRelationship = options.parsed.movieRelationship

const run = async () => {
    dbConnector.connect()
    await clearTestScenario()
	for (const ratingsPercentage of scenarios.ratingsPercentage) {
		console.log('Setting up test scenario...')
		await setupTest(k, ratingsPercentage)
		for (const n of scenarios.n) {
            for (const steps of scenarios.steps) {
                console.log(`Running test scenario - ratingsPerc ${ratingsPercentage}, n ${n}, steps ${steps}`)
                await runScenario(n, steps)
                await clearRecommendations()
                console.log('--------------------------------------------------------------------------------')
            }
		}
		await clearTestScenario()
	}
    dbConnector.disconnect()
}

let previousN = 0

const runScenario = async (n, steps) => {
  	const users = await usersDb.getTestUsers().then(items => items.map(i => i.movieLensId))
	console.log('Getting recommendations...')
	progressBar.start(users.length * 4, 0)
	for (const user of users) {
        if (previousN !== n) {
            await usersDb.getUserBasedRecommendations(user, n, userRelationship)
            progressBar.increment()
            await usersDb.getItemBasedRecommendations(user, n, movieRelationship)
            progressBar.increment()
        } else {
            progressBar.increment(2)
        }
		await getRandomWalkRecommendations(user, options.parsed.walks, steps, n)
		progressBar.increment()
		await getRandomWalkRecommendations(user, options.parsed.walks, steps, n, true)
        progressBar.increment()
    }
    previousN = n
	progressBar.stop()
	const recommendationTypes = ['UB', 'IB', 'RW', 'BRW']
	for (const type of recommendationTypes) {
		console.log(`${type}\t\t\t`)
		console.log(`MAE\tP\tR\tF1`)
		await calculateMAE(users, type)
		await calculateF1(users, type)
		console.log()
	}
}
 
const calculateMAE = async (testUsers, recSuffix) => {
    if (recSuffix === 'RW' || recSuffix === 'BRW') return
    const predictRating = recSuffix === 'UB' ? moviesDb.predictRatingUB :  moviesDb.predictRatingIB
    const relationshipType = recSuffix === 'UB' ? userRelationship :  movieRelationship
    let numerator = 0
    let denominator = 0
    for (const user of testUsers) {
        const testSet = await moviesDb.getTestSet(user, recSuffix)
        for (const item of testSet) {
            if (!item.predictedScore) item.predictedScore = await predictRating(user, item, relationshipType)
            numerator += Math.abs(Math.min(item.predictedScore, 5) - item.rating)
        }
        denominator += testSet.length
	}
	process.stdout.write(`${numerator/denominator}\t`);
}

const calculateF1 = async (testUsers, recSuffix) => {
    const results = []
    for (const user of testUsers) {
        const info = await usersDb.getF1Info(user, recSuffix)
        const precision = info.recset ? info.hits / info.recset : 0
        const recall = info.hits / info.testset
        const f1 = recall && precision ? 2 * precision * recall / (recall + precision) : 0
        results.push({precision, recall, f1})
    }
    const resultsSum = results.reduce((sum, res) => {
        sum.precision += res.precision
        sum.recall += res.recall
        sum.f1 += res.f1
        return sum
	}, {precision: 0, recall: 0, f1: 0})
	if (recSuffix === 'RW' || recSuffix === 'BRW') process.stdout.write(`\t`)
	process.stdout.write(`${resultsSum.precision/results.length}\t${resultsSum.recall/results.length}\t${resultsSum.f1/results.length}\n`);
}

const getRandomWalkRecommendations = async (userId, walks, steps, n, biased) => {
    const recommendationCount = await Promise.all(
        [...Array(walks).keys()]
        .map(() => walk(userId, steps, biased, userId))
    ).then(recommendations => recommendations
        .filter(rec => !!rec)
        .map(rec => rec.movieLensId)
        .reduce((recommendations, movieId) => {
            if (!(movieId in recommendations)) recommendations[movieId] = 0
            recommendations[movieId]++
            return recommendations
        }, {})
    )
    const candidates = Object.keys(recommendationCount)
    candidates.sort((a, b) => {
        if (a !== b) return recommendationCount[b] - recommendationCount[a]
        return 0.5 - Math.random()
    })
    const session = dbConnector.getSession()
    return session.run(
        `MATCH (m:Movie) WHERE m.movieLensId IN [${candidates.slice(0, n)}]
         MATCH (u:User) WHERE u.movieLensId = ${userId}
         MERGE (u)-[:PROBABLY_LIKES_${biased ? 'B' : ''}RW]->(m)
    `)
}

const walk = async (start, steps, biased, userId) => {
    let currentNode = {movieLensId: start}
    for (let currentStep = 0; currentStep < steps && currentNode; currentStep++) {
        const isLastStep = currentStep === steps-1
        currentNode = await step(currentNode, biased, isLastStep ? userId : undefined)
    }
    return currentNode
}

const step = async (start, biased, userId) => {
    const startNodeType = isMovie(start) ? ':Movie' : ':User'
    const targetNodeType = userId ? ':Movie' : ''
    const neighbors = await getNeighbors(start.movieLensId, startNodeType, targetNodeType, userId)
    return pickRandomNeighbor(neighbors, biased)
}

const pickRandomNeighbor = (neighbors, biased) => {
    if (neighbors.length === 0) return null
    const maxRand = biased ? neighbors.reduce((sum, neighbor) => sum + neighbor.score, 0) : neighbors.length
    const rand = Math.random() * maxRand
    if (!biased) return neighbors[Math.floor(rand)].destination
    let sum = 0
    for (const neighbor of neighbors) {
        if (sum > rand) return neighbor.destination
        sum += neighbor.score
    }
}

const isMovie = node => !!node.title

const getNeighbors = (start, startNodeType, targetNodeType = '', userId) => {
    const session = dbConnector.getSession()
    const extraRestriction = userId ? `AND NOT (start)-[:RATES]->(destination)` : ''
    return session.run(
        `MATCH (start${startNodeType})-[path:RATES|:PEARS_SIM|:COS_SIM]-(destination${targetNodeType}) 
        WHERE start.movieLensId = ${start} ${extraRestriction}
        RETURN path, destination
    `)
    .then(res => res.records.map(record => ({score: record.get('path').properties , destination: record.get('destination').properties})))
    .then(neighbors => neighbors.map(neighbor => ({score: neighbor.score.similarity || neighbor.score.rating, destination: neighbor.destination })))
}

const setupTest = async (populationPercentage, ratingsPercentage) => {
    const users = await usersDb.get().then(items => items.map(i => i.movieLensId))
    const n = parseInt(users.length * populationPercentage / 100)
    shuffle(users)
    const testUsers = users.slice(0, n)
    progressBar.start(testUsers.length, 0)
    for (const user of testUsers) {
        const ratings = await ratingsDb.getUserRatings(user)
        shuffle(ratings) 
        const disabledRatings = ratings.slice(0, parseInt(ratings.length * ratingsPercentage / 100))
        await Promise.all(disabledRatings.map(ratingsDb.disableRating))
        await usersDb.markAsTestUser(user)
        progressBar.increment()
    }
    progressBar.stop()
    await usersDb.calculateUsersAvgRating()
}

const shuffle = array => {
    for (let i = array.length - 1; i > 0; i--) {
        const j = Math.floor(Math.random() * (i + 1));
        [array[i], array[j]] = [array[j], array[i]]
    }
}

const clearTestScenario = () => {
    const session = dbConnector.getSession()
    return Promise.all([
		session.run(`
			MATCH (n)-[r:DISABLED_RATES]->(m)
			MERGE (n)-[new:RATES]->(m)
			SET new = r
			WITH r
			DELETE r			
		`),
		clearRecommendations(),
		session.run(`MATCH (n:User {testUser: true}) SET n.testUser = false`)
	])
}

const clearRecommendations = () => {
	const session = dbConnector.getSession()
    return Promise.all([
		session.run(`MATCH (n)-[r:PROBABLY_LIKES_IB]->(m) DELETE r`),
		session.run(`MATCH (n)-[r:PROBABLY_LIKES_UB]->(m) DELETE r`),
		session.run(`MATCH (n)-[r:PROBABLY_LIKES_RW]->(m) DELETE r`),
		session.run(`MATCH (n)-[r:PROBABLY_LIKES_BRW]->(m) DELETE r`)
	])
}

if (!userRelationship || !movieRelationship) throw 'Missing relationship names!!!'
if (userRelationship.startsWith(':') || movieRelationship.startsWith(':')) throw 'Invalid relationship names!!!'
run()
