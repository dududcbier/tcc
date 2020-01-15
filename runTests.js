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
	populationPercentage: {
		type: Number,
		value: 20
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
    },
    threshold: {
        type: Number,
        value: 0.5
    },
    k: {
        type: Number,
        value: 50
    },
    skipResetScenario: {
        type: Boolean,
        value: false
    },
    folds: {
        type: Number,
        value: 0
    }
}

const scenarios = {
	ratingsPercentage: [20],
    n: [25, 50, 100],
    steps: [3, 5]
}

const options = parser.parse(process.argv, { rules })
const populationPercentage = options.parsed.populationPercentage
const k = options.parsed.k
const userRelationship = options.parsed.userRelationship
const movieRelationship = options.parsed.movieRelationship
const shouldResetScenario = !options.parsed.skipResetScenario

const run = async () => {
    dbConnector.connect()
    if (shouldResetScenario) await clearTestScenario()
    else await clearRecommendations()
	for (const ratingsPercentage of scenarios.ratingsPercentage) {
		console.log('Setting up test scenario...')
        if (shouldResetScenario) await setupTest(populationPercentage, ratingsPercentage)
        else console.log('SKIPPING RESET TEST SCENARIO')
        const users = await getRecommendations(Math.max(...scenarios.n))
        await evaluate(users, ratingsPercentage)
        if (shouldResetScenario) await clearRecommendations()
		if (shouldResetScenario) await clearTestScenario()
	}
    dbConnector.disconnect()
}

const getRecommendations = async n => {
  	const users = await usersDb.getTestUsers().then(items => items.map(i => i.movieLensId))
	console.log('Getting recommendations...')
    progressBar.start(users.length * (2 + 2 * scenarios.steps.length), 0)
    const count = {ub: 0, ib: 0}
	for (const user of users) {
        const ub = await usersDb.getUserBasedRecommendations(user, n, userRelationship, k)
        progressBar.increment()
        const ib = await usersDb.getItemBasedRecommendations(user, n, movieRelationship, k)
        progressBar.increment()
        for (const steps of scenarios.steps) {
            await getRandomWalkRecommendations(user, options.parsed.walks, steps, n)
		    progressBar.increment()
		    await getRandomWalkRecommendations(user, options.parsed.walks, steps, n, true)
            progressBar.increment()
        }
        count.ub += ub
        count.ib += ib
    }
    console.log(count)
    progressBar.stop()
    return users
}

const evaluate = async (users,ratingsPercentage) => {
    let lastN = Math.max(scenarios.n)
    const recommendationTypes = ['UB', 'IB']
    for (const steps of scenarios.steps) {
        recommendationTypes.push(`RW_${steps}`)
        recommendationTypes.push(`BRW_${steps}`)
    }
    for (const n of scenarios.n.sort((a, b) => b - a)) {
        console.log(`Evalutating test scenario - ratingsPerc ${ratingsPercentage}, n ${n}`)
        if (lastN > n) await usersDb.eraseBottomRecommendations(n, recommendationTypes)
        lastN = n
        for (const type of recommendationTypes) {
            console.log(`${type}\t\t\t`)
            console.log(`MAE\tP\tR\tF1`)
            await calculateMAE(users, type)
            await calculateF1(users, type)
            console.log()
        }
        console.log('--------------------------------------------------------------------------------')
    }
}
 
const calculateMAE = async (testUsers, recSuffix) => {
    if (recSuffix.startsWith('RW') || recSuffix.startsWith('BRW')) return
    let numerator = 0
    let denominator = 0
    for (const user of testUsers) {
        const hitSet = await moviesDb.getHitSet(user, recSuffix)
        for (const item of hitSet) {
            numerator += Math.abs(Math.max(Math.min(item.predictedScore, 5), 0) - item.rating)
        }
        denominator += hitSet.length
    }
    console.log({denominator})
	process.stdout.write(`${Number.parseFloat(numerator/denominator).toFixed(4)}\t`);
}

const calculateF1 = async (testUsers, recSuffix) => {
    const results = []
    let count = 0
    for (const user of testUsers) {
        const {tp, fp, fn} = await usersDb.getF1Info(user, recSuffix)
        const precision = tp || fp ? tp / (tp + fp) : 0
        const recall = tp || fn ? tp / (tp + fn) : 0
        const f1 = recall && precision ? 2 * precision * recall / (recall + precision) : 0
        if ((!tp && !fn) || (!tp && !fp)) count += 1
        results.push({precision, recall, f1})
    }
    const resultsSum = results.reduce((sum, res) => {
        sum.precision += res.precision
        sum.recall += res.recall
        sum.f1 += res.f1
        return sum
	}, {precision: 0, recall: 0, f1: 0, recset: 0, hits: 0, testset: 0})
    if (recSuffix.startsWith('RW') || recSuffix.startsWith('BRW')) process.stdout.write(`\t`)
    const length = testUsers.length - count
	process.stdout.write(`${toPercentage(resultsSum.precision/length)}%\t${toPercentage(resultsSum.recall/length)}%\t${toPercentage(resultsSum.f1/length)}%\n`)
}

const toPercentage = n => Number.parseFloat(n * 100).toFixed(2)

const getRandomWalkRecommendations = async (userId, walks, steps, n, biased) => {
    const recommendationCount = await Promise.all(
        [...Array(walks).keys()]
        .map(() => walk(userId, steps, biased, userId))
    )
    .then(recommendations => recommendations
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
         MERGE (u)-[:PROBABLY_LIKES_${biased ? 'B' : ''}RW_${steps}]->(m)
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
    const similarityType = isMovie(start) ? movieRelationship : userRelationship
    const targetNodeType = userId ? ':Movie' : ''
    const neighbors = await getNeighbors(start.movieLensId, startNodeType, targetNodeType, userId, similarityType)
    return !neighbors.length && isMovie(start) ? start : pickRandomNeighbor(neighbors, biased)
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

const getNeighbors = (start, startNodeType, targetNodeType = '', userId, similarityType) => {
    const session = dbConnector.getSession()
    const extraRestriction = userId ? `AND NOT (:User {movieLensId: ${userId}})-[:RATES]->(destination)` : ''
    return session.run(
        `MATCH (start${startNodeType})-[path:RATES|${similarityType}]-(destination${targetNodeType}) 
        WHERE start.movieLensId = ${start} ${extraRestriction}
        RETURN 
            CASE
                WHEN path.similarity IS NOT NULL THEN path.similarity
                ELSE path.rating / 5.0
            END AS score, destination
    `)
    .then(res => res.records.map(record => ({score: record.get('score') , destination: record.get('destination').properties})))
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
    const movies = await getMovies()
    await usersDb.clearSimilarities()
    await calculateSimilarities(users, userRelationship === 'COS_SIM' ? usersDb.calculateAllCossineSimilarities : usersDb.calculateAllPearsonSimilarities)
    await calculateSimilarities(movies, movieRelationship === 'COS_SIM' ? moviesDb.calculateAllCossineSimilarities : moviesDb.calculateAllPearsonSimilarities)
    await usersDb.calculateUsersAvgRating()
    await moviesDb.calculateAvgRatings()
}

const getMovies = () => moviesDb.get().then(movies => movies.map(m => m.movieLensId))

const calculateSimilarities = async (items, similarityFunction) => {
    console.log('Calculating similarities...')
    progressBar.start(items.length, 0)
    await Promise.all(items.map(async itemId => {
      await similarityFunction(itemId, options.parsed.threshold)
      progressBar.increment()
    }))
    progressBar.stop()
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
        ...scenarios.steps.map(steps => session.run(`MATCH (n)-[r:PROBABLY_LIKES_RW_${steps}]->(m) DELETE r`)),
        ...scenarios.steps.map(steps => session.run(`MATCH (n)-[r:PROBABLY_LIKES_BRW_${steps}]->(m) DELETE r`))
	])
}

if (!userRelationship || !movieRelationship) throw 'Missing relationship names!!!'
if (userRelationship.startsWith(':') || movieRelationship.startsWith(':')) throw 'Invalid relationship names!!!'
run()

