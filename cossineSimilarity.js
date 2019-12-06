/* eslint-disable require-atomic-updates */
const parser = require('argv-parser')
const json = require('big-json');
const fs = require('fs')
const _cliProgress = require('cli-progress');
const dbConnector = require('./neo4jConnector.js')
const ratingsDb = require('./ratingsDb.js')
const moviesDb = require('./moviesDb.js')
const usersDb = require('./usersDb.js')

const progressBar = new _cliProgress.SingleBar({}, _cliProgress.Presets.shades_classic);

let movieRatings
let avgRating
const similarity = {}

const rules = {
  weighted: {
    type: Boolean,
    value: false
  },
  clear: {
    type: Boolean,
    value: true
  },
  safe: {
    type: Boolean,
    value: true
  },
  save_similarities: {
    type: Boolean,
    value: false 
  }
}

const options = parser.parse(process.argv, { rules })

const baseWeight = options.parsed.weighted ? 25 : 1

const shouldSaveSimilarities = options.parsed.save_similarities

const delay = ms => new Promise(done => setTimeout(done, ms))

const run = async () => {
  dbConnector.connect()
  if (options.parsed.clear) {
    if (options.parsed.safe) {
      console.log('Clearing similarities in 3 seconds')
      await delay(3000)
    }
    await moviesDb.clearSimilarities()
    console.log('Similarities cleared!')
  }

  console.log('Getting movies...')
  const movies = await moviesDb.getMovies()
  movies.forEach(({ movieLensId }) => similarity[movieLensId] = {})

  console.log('Getting ratings...')
  movieRatings = await ratingsDb.getMovieRatings()
  avgRating = await usersDb.getAvgRatings()

  console.log('Calculating similarities...')
  progressBar.start(movies.length, 0)
  for (const movie of movies.map(m => m.movieLensId)) {
    const neighbors = await moviesDb.getNeighbors(movie)
    for (const neighbor of neighbors) {
      await calculateCossineSimilarity(movie, neighbor.movieLensId)
    }
    progressBar.increment()
  }
  progressBar.stop()
  for (const movie in similarity) {
    const rows = []
    const similarMovies = []
    for (const similarMovie in similarity[movie]) {
      if (Number(movie) > Number(similarMovie)) continue
      const rowItem = {
        id: similarMovie, 
        similarity: similarity[movie][similarMovie].value, 
        baseWeight: similarity[movie][similarMovie].baseWeight,
        usersInCommon: similarity[movie][similarMovie].usersInCommon
      }
      if (similarity[movie][similarMovie].value >= 0.5) similarMovies.push(rowItem)
    }
    console.log(`Linking ${similarMovies.length} movies similiar to ${movie}`)
    if (similarMovies.length > 0) await moviesDb.linkMovies(movie, similarMovies)
  }
  dbConnector.disconnect()
}

run()

const calculateCossineSimilarity = async (m, n) => {
  if (similarity[m][n]) return similarity[m][n]
  if (similarity[n][m]) return similarity[n][m]
  
  let sum = 0
  let squaredSumU = 0
  let squaredSumV = 0
  
  const ratings = {}
  ratings[m] = await getRatings(m)
  ratings[n] = await getRatings(n)
  
  const users = getUsersInCommon(ratings[m], ratings[n])
  for (const user of users) {
    const dif_u = ratings[m][user] - avgRating[user]
    const dif_v = ratings[n][user] - avgRating[user]
    sum += dif_u * dif_v
    squaredSumU += dif_u * dif_u
    squaredSumV += dif_v * dif_v
  }
  let r_uv = sum / (Math.sqrt(squaredSumU) * Math.sqrt(squaredSumV))
  if (Number.isNaN(r_uv)) r_uv = 0
  const weight = users.length > baseWeight ? 1 : users.length / baseWeight 
  r_uv *= weight
  similarity[m][n] = { value: r_uv, usersInCommon: users.length, baseWeight }
  return r_uv
}

const getUsersInCommon = (ratings, otherRatings) => Object.keys(ratings).filter(userNeoId => userNeoId in otherRatings)

const getRatings = async movieId => movieRatings[movieId]