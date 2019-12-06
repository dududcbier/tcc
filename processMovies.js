// Create Movie nodes:
// LOAD CSV WITH HEADERS FROM "file:///movielens-100k/processedMovies.csv" AS row
// CREATE (n:Movie {
// 	id: toInteger(row.id),
//     title: row.title,
//     genres: row.genres,
//     year: row.year
// })

const csv = require('csvtojson')
const fastcsv = require('fast-csv')
const fs = require('fs')

const inputFile = process.argv[2]
const outputFile = process.argv[3] || 'output.csv'

const yearRegex = /^(.*) \((\d+)\)?$/ 

const ws = fs.createWriteStream(outputFile)
const csvStream = fastcsv.format({ headers: true, quoteColumns: {title: true} })
csvStream.pipe(ws)

const exitWithError = err => {
    console.log(err)
    process.exit()
}

if (!inputFile) exitWithError('No file specified')

const processMovie = movie => {
    const re = movie.title.match(yearRegex)
    if (re) {
        movie.title = re[1]
        movie.year = parseInt(re[2])
    }
    if (movie.genres === '(no genres listed)') delete movie.genres
    csvStream.write(movie)
}

csv().fromFile(inputFile)
.subscribe(processMovie)
.on('done', error => {
    csvStream.end()
    if (error) exitWithError(error)
    console.log('Complete!')
})