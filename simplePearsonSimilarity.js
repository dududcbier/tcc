/* eslint-disable require-atomic-updates */
const parser = require('argv-parser')
const _cliProgress = require('cli-progress');
const dbConnector = require('./neo4jConnector.js')
const moviesDb = require('./moviesDb.js')
const usersDb = require('./usersDb.js')

const progressBar = new _cliProgress.SingleBar({}, _cliProgress.Presets.shades_classic);

const rules = {
    movies: {
        type: Boolean,
        value: false
    },
    users: {
        type: Boolean,
        value: false
    },
    threshold: {
      type: Number,
      value: 0.5
    }
}

const options = parser.parse(process.argv, { rules })

const run = async () => {
  dbConnector.connect()

  if (options.parsed.users) { 
    console.log('Getting users...')
    const users = await getItems(usersDb)
    await calculateSimilarities(users, usersDb)
  }

  if (options.parsed.movies) { 
    console.log('Getting movies...')
    const movies = await getItems(moviesDb)
    await calculateSimilarities(movies, moviesDb)
  }

  dbConnector.disconnect()
}

const getItems = db => db.get().then(items => items.map(i => i.movieLensId))

const calculateSimilarities = async (items, db) => {
    console.log('Calculating similarities...')
    progressBar.start(items.length, 0)
    await Promise.all(items.map(async itemId => {
      await db.calculateAllPearsonSimilarities(itemId, options.parsed.threshold)
      progressBar.increment()
    }))
    progressBar.stop()
}

run()
