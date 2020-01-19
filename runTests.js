const parser = require('argv-parser')
const _cliProgress = require('cli-progress');
const dbConnector = require('./neo4jConnector.js')
const moviesDb = require('./moviesDb.js')
const usersDb = require('./usersDb.js')
const ratingsDb = require('./ratingsDb.js')

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
    steps: [3, 5],
    similarities: ['COS_SIM', 'PEARS_SIM']
}

const options = parser.parse(process.argv, { rules })
const populationPercentage = options.parsed.folds ? 1 / options.parsed.folds : options.parsed.populationPercentage
const k = options.parsed.k
const userRelationships = options.parsed.userRelationship ? [options.parsed.userRelationship] : scenarios.similarities 
const movieRelationships = options.parsed.movieRelationship ? [options.parsed.movieRelationship] : scenarios.similarities
const shouldResetScenario = !options.parsed.skipResetScenario

const recommendationTypes = ['UB', 'IB']
for (const steps of scenarios.steps) {
    recommendationTypes.push(`RW_${steps}`)
    recommendationTypes.push(`BRW_${steps}`)
}

let userRelationship = null
let movieRelationship = null
let f = 0

const results = {}
for (const recType of recommendationTypes) {
    results[recType] = {}
}

const run = async () => {
    dbConnector.connect()
    if (shouldResetScenario) await clearTestScenario()
    else await clearRecommendations()
	for (const ratingsPercentage of scenarios.ratingsPercentage) {
        console.log('Setting up test scenario...')
        const folds = await prepareFolds(populationPercentage)
        f = 0
        for (const testUsers of folds) {
            if (shouldResetScenario) await setupTest(testUsers, ratingsPercentage)
            else console.log('SKIPPING RESET TEST SCENARIO')
            let skipUB = false
            let skipIB = false
            for (userRelationship of userRelationships) {
                for (movieRelationship of movieRelationships) {
                    await getRecommendations(testUsers, Math.max(...scenarios.n), skipUB, skipIB)
                    await evaluate(testUsers, ratingsPercentage, skipUB, skipIB)
                    if (shouldResetScenario) await clearRecommendations(skipUB, skipIB)
                    skipUB = true
                }
                skipIB = true
                skipUB = false
            }
            if (shouldResetScenario) await clearTestScenario()
            f += 1
        }
        console.log('FINAL RESULTS')
        for (const recType of recommendationTypes) {
            console.log(`\n${recType}`)
            for (const scenario of Object.keys(results[recType])) {
                console.log(scenario)
                console.log(`MAE\tP\tR\tF1`)
                results[recType][scenario] = results[recType][scenario].reduce((sum, res) => {
                    sum.mae += res.mae
                    sum.recall += res.recall
                    sum.precision += res.precision
                    sum.f1 += res.f1
                    return sum
                }, {mae: 0, recall: 0, precision: 0, f1: 0})
                if (results[recType][scenario].mae || results[recType][scenario].mae === 0) process.stdout.write(`${Number.parseFloat(results[recType][scenario].mae / f).toFixed(4)}\t`);
                process.stdout.write(`${toPercentage(results[recType][scenario].precision / f)}%\t${toPercentage(results[recType][scenario].recall / f)}%\t${toPercentage(results[recType][scenario].f1 / f)}%\n`)
            }

        }
    }
    dbConnector.disconnect()
}

const getRecommendations = async (users, n, skipUB, skipIB) => {
	console.log('Getting recommendations...')
    progressBar.start(users.length * (2 + 2 * scenarios.steps.length), 0)
	for (const user of users) {
        if (!skipUB) await usersDb.getUserBasedRecommendations(user, n, userRelationship, k)
        progressBar.increment()
        if (!skipIB) usersDb.getItemBasedRecommendations(user, n, movieRelationship, k)
        progressBar.increment()
        for (const steps of scenarios.steps) {
            await getRandomWalkRecommendations(user, options.parsed.walks, steps, n)
		    progressBar.increment()
		    await getRandomWalkRecommendations(user, options.parsed.walks, steps, n, true)
            progressBar.increment()
        }
    }
    progressBar.stop()
}

const evaluate = async (users, ratingsPercentage, skipUB, skipIB) => {
    let lastN = Math.max(scenarios.n)
    const filteredRecommendationTypes = recommendationTypes.filter(recType => (recType !== 'IB' || !skipIB) && (recType !== 'UB' || !skipUB))
    for (const n of scenarios.n.sort((a, b) => b - a)) {
        console.log(`Evaluating test scenario - ratingsPerc ${ratingsPercentage}, n ${n}, user similarity = ${userRelationship}, movie similarity = ${movieRelationship}`)
        if (lastN > n) await usersDb.eraseBottomRecommendations(n, filteredRecommendationTypes)
        lastN = n
        for (const type of filteredRecommendationTypes) {
            console.log(`\n${type}\t\t\t`)
            console.log(`MAE\tP\tR\tF1`)
            const mae = await calculateMAE(users, type)
            const {precision, recall, f1} = await calculateF1(users, type)
            const scenarioKey = getScenarioKey(type, userRelationship, movieRelationship, n)
            if (!(scenarioKey in results[type])) results[type][scenarioKey] = []
            results[type][scenarioKey].push({mae, precision, recall, f1})
            if (mae || mae === 0) process.stdout.write(`${Number.parseFloat(mae).toFixed(4)}\t`);
	        process.stdout.write(`${toPercentage(precision)}%\t${toPercentage(recall)}%\t${toPercentage(f1)}%\n`)
        }
        console.log('--------------------------------------------------------------------------------')
    }
}

const getScenarioKey = (type, userRelationship, movieRelationship, n) => {
    switch(type) {
        case 'UB': return `similarity> ${userRelationship} n> ${n}`
        case 'IB': return `similarity> ${movieRelationship} n> ${n}`
        default: return `user> ${userRelationship} movie> ${movieRelationship} n> ${n}`
    }
}
 
const calculateMAE = async (testUsers, recSuffix) => {
    if (recSuffix.startsWith('RW') || recSuffix.startsWith('BRW')) return null
    let numerator = 0
    let denominator = 0
    for (const user of testUsers) {
        const hitSet = await moviesDb.getHitSet(user, recSuffix)
        for (const item of hitSet) {
            numerator += Math.abs(Math.max(Math.min(item.predictedScore, 5), 0) - item.rating)
        }
        denominator += hitSet.length
    }
    return numerator/denominator
}

const calculateF1 = async (testUsers, recSuffix) => {
    let count = 0
    const f1Info = await Promise.all(testUsers.map(async user => {
        const {tp, fp, fn} = await usersDb.getF1Info(user, recSuffix)
        const precision = tp || fp ? tp / (tp + fp) : 0
        const recall = tp || fn ? tp / (tp + fn) : 0
        const f1 = recall && precision ? 2 * precision * recall / (recall + precision) : 0
        if ((!tp && !fn) || (!tp && !fp)) count += 1
        return {precision, recall, f1}
    }))
    const resultsSum = f1Info.reduce((sum, res) => {
        sum.precision += res.precision
        sum.recall += res.recall
        sum.f1 += res.f1
        return sum
    }, {precision: 0, recall: 0, f1: 0})
    if (recSuffix.startsWith('RW') || recSuffix.startsWith('BRW')) process.stdout.write(`\t`)
    const length = testUsers.length - count
    return {precision: resultsSum.precision/length, recall: resultsSum.recall/length, f1: resultsSum.f1/length}
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

const prepareFolds = async (populationPercentage) => {
    const users = await getUsers()
    const n = parseInt(users.length * populationPercentage)
    shuffle(users)
    const testUsers = [users.slice(0, n)]
    for (let i = 1; i < options.parsed.folds; i++) {
        testUsers.push(users.slice(i * n, (i + 1) * n))
    }
    return testUsers
}

const setupTest = async (testUsers, ratingsPercentage) => {
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
    const users = await getUsers()
    for (userRelationship of userRelationships) {
        await calculateSimilarities(users, userRelationship === 'COS_SIM' ? usersDb.calculateAllCossineSimilarities : usersDb.calculateAllPearsonSimilarities)
    }
    for (movieRelationship of movieRelationships) {
        await calculateSimilarities(movies, movieRelationship === 'COS_SIM' ? moviesDb.calculateAllCossineSimilarities : moviesDb.calculateAllPearsonSimilarities)
    }
    await usersDb.calculateUsersAvgRating()
    await moviesDb.calculateAvgRatings()
}

const getMovies = () => moviesDb.get().then(movies => movies.map(m => m.movieLensId))

const getUsers = () => usersDb.get().then(users => users.map(i => i.movieLensId))

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

run()

