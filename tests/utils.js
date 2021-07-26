const sql = require('mysql');

const { credentials, dbConfig } = require('../config')

exports.createConnectionWithDone = (done) => {
    const cb = err => {
        if (err) throw new Error(err);
        done();
    };
    const con = sql.createConnection({
        host: dbConfig.host,
        user: credentials.user,
        password: credentials.password,

        //A different table can be created using  the "CREATE DATABASE dbName"
        database: dbConfig.database,
    });

    //first connect and then return the instance.
    con.connect(cb);
    return con;
}

exports.globalCb = err => {
    if (err) throw new Error(err);
};

exports.cbWithDone = (done) => {
    const cb = err => {
        if (err) throw new Error(err);
        done();
    };
    return cb;
}

exports.createTable = (connection, config, cb) => {
    const { name, schema } = config;
    const create = `CREATE TABLE ${name}(${schema})`;
    connection.query(create, cb);
};
exports.dropTable = (connection, name, cb) => {
    connection.query(`DROP TABLE ${name}`, cb);
};
exports.createTableWithDone = (connection, table, done) => {
    const cb = err => {
        if (err) throw new Error(err);
        done();
    };

    this.createTable(connection, table, cb);
}
exports.deleteTableWithDone = (connection, name, done) => {
    const cb = err => {
        if (err) throw new Error(err);
        done();
    };
    this.dropTable(connection, name, cb);
}