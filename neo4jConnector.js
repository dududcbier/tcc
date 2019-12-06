const neo4j = require('neo4j-driver').v1
const errors = require('./errors.js')

const uri = 'bolt://localhost'
const user = 'neo4j'
const password = '290891'

let session
let driver

const connect = () => {
    driver = neo4j.driver(
        uri, 
        neo4j.auth.basic(user, password),
        { disableLosslessIntegers: true }
    )
}

const disconnect = () => {
    driver.close()
    driver = undefined
}

const getSession = () => {
    if (!driver) throw errors.noDatabaseConnection
    if (!session) session = driver.session()
    return session
}

const closeSession = () => {
    if (session) session.close()
    session = undefined
}

module.exports = {
    getSession,
    closeSession,
    connect,
    disconnect
}