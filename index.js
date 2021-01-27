const sql = require('mysql');

const host = 'localhost'
const user = 'root'
const password = 'dBpVi@247'
const database = 'people'



exports.connector = (conf, cb) => {
    const connection = sql.createConnection(conf)
    connection.connect(cb);
    return connection
}

exports.connectToDb = () => {
    return this.connector({
        host, user, password, database
    }, (err) => {
        if (err) throw new Error(err)
    })
}

exports.sql = this.connectToDb()

exports.retrieve = (cb) => {
    const connection = this.connectToDb()
    const sql = 'SELECT * FROM trial'
    connection.query(sql, cb)
}





