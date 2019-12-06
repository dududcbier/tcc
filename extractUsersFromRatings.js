// LOAD CSV WITH HEADERS FROM "file:///movielens-100k/processedUsers.csv" AS row
// CREATE (u:User {
// 	id: toInteger(row.userId)
// })

const csv = require('csvtojson')
const fastcsv = require('fast-csv')
const fs = require('fs')

const inputFile = process.argv[2]
const outputFile = process.argv[3] || 'output.csv'

const ws = fs.createWriteStream(outputFile)
const csvStream = fastcsv.format({ headers: true })
csvStream.pipe(ws)

const exitWithError = err => {
    console.log(err)
    process.exit()
}

if (!inputFile) exitWithError('No file specified')

const users = {}

const processRating = ({userId}) => {
    if (users[userId]) return
    users[userId] = true
    user = {id: userId}
    csvStream.write(user)
}

csv().fromFile(inputFile)
.subscribe(processRating)
.on('done', error => {
    csvStream.end()
    if (error) exitWithError(error)
    console.log('Complete!')
})