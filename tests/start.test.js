const sql = require('mysql');

const host = 'localhost'
const user = 'root'
const password = 'dBpVi@247'
const database = 'people'

describe('starting tests', () => {
    let connection;

    beforeAll((done) => {
        const con = sql.createConnection({
            host,
            user,
            password,
            database
        })
        const cb = (err) => {
            if (err) throw new Error(err)
            done()
        }
        con.connect(cb);
        connection = con;
    }
    );
    it('can create tables', (done) => {
        const sql = `CREATE TABLE department(
         dept_name VARCHAR(20),
         building VARCHAR(20),
         budget  NUMERIC(12,2),
         PRIMARY KEY (dept_name)
        )`

        const cb = (err, result) => {
            if (err) throw new Error(err)
            console.log(result)
            done()
        }
        connection.query(sql, cb)
    });
});

function connectToDb(database) {
    const conf = {
        host,
        user,
        password,
        database
    }
    const cb = (err) => {
        if (err) throw new Error(err)
    }
    return sqlImp.connector(conf, cb)

}


function connectToServer() {
    const conf = {
        host,
        user,
        password,
    }
    const cb = (err) => {
        if (err) throw new Error(err)
    }
    return sqlImp.connector(conf, cb)
}
const deleteDatabase = (name) => {
    sqlImp.sql.query(`DROP DATABASE ${name} `)
}
const deleteTable = (name, dbName) => {
    sqlImp.sql.query(`USE ${dbName} DROP TABLE ${name} `)
}