const parser = require('argv-parser')
const _cliProgress = require('cli-progress');
const dbConnector = require('./neo4jConnector.js')
const moviesDb = require('./moviesDb.js')
const usersDb = require('./usersDb.js')
const ratingsDb = require('./ratingsDb.js')

const progressBar = new _cliProgress.SingleBar({}, _cliProgress.Presets.shades_classic);

const rules = {
    pop: {
      type: Number,
      value: 25
    },
    ratings: {
      type: Number,
      value: 75
    }
}

const options = parser.parse(process.argv, { rules })

const run = async () => {
  dbConnector.connect()
  const users = await usersDb.get().then(items => items.map(i => i.movieLensId))
  const n = parseInt(users.length * options.parsed.pop / 100)
  shuffle(users)
  const testUsers = users.slice(0, n)
  progressBar.start(testUsers.length, 0)
  for (const user of testUsers) {
    const ratings = await ratingsDb.getUserRatings(user)
    shuffle(ratings)
    const disabledRatings = ratings.slice(0, parseInt(ratings.length * options.parsed.ratings / 100))
    await Promise.all(disabledRatings.map(ratingsDb.disableRating))
    await usersDb.markAsTestUser(user)
    progressBar.increment()
  }
  progressBar.stop()
  dbConnector.disconnect()
}

const shuffle = array => {
  for (let i = array.length - 1; i > 0; i--) {
      const j = Math.floor(Math.random() * (i + 1));
      [array[i], array[j]] = [array[j], array[i]]
  }
}

run()
