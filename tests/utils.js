const sql = require("mysql");

const university = require("./schemas/universityDb");
const payload = require("./data");

//ENSURE THAT MYSQL IS RUNNING BEFORE
//STARTING OUT THESE TESTS.

const { credentials, dbConfig, tableName } = require("../config");
const { ensureEqual } = require("./testUtils");

const MAX_TESTING_TIME = 100000;

function createConnectionToDb(cb) {
  const con = sql.createConnection({
    host: dbConfig.host,
    user: credentials.user,
    password: credentials.password,
    insecureAuth: true,
  });
  con.connect(cb);
  return con;
}

exports.includeSetUpAndTeardown = (connection) => {
  beforeAll(async () => {
    await this.createTablesAndLoadThemWithData(connection);
  }, MAX_TESTING_TIME);
  afterAll(async () => {
    await this.clearDbAsync(connection);
  });
};

exports.clearDbAsync = async (connection) => {
  await this.runNonParametricQueryAsync(
    connection,
    `DROP DATABASE ${dbConfig.database}`
  );
};

exports.createTablesAndLoadThemWithData = async (
  connection,
  tablesToCreate = []
) => {
  //the database is delted by dropping it hence there is need to create a
  //new database when there is need to write data into tables.
  await this.createDbAndUseIt(connection);
  const schemas = Object.values(university);
  //if second param is not provided create and fill all tables present.
  if (tablesToCreate.length < 1) {
    //create the tables separately so that generation of some(those after the first insertion)  are not affected by
    //errors generated during insertions.
    for (const schema of schemas) {
      await this.createTableAsync(connection, schema);
    }
    for (const schema of schemas) {
      await this.loadDataIntoTable(connection, schema.name);
    }
  } else {
    //same case as before, create the tables at first.
    for (const table of tablesToCreate) {
      await this.createTableAsync(connection, university[table]);
    }

    for (const table of tablesToCreate) {
      const schema = university[table];
      await this.loadDataIntoTable(connection, schema.name);
    }
  }
};

exports.createDbAndUseIt = async (con) => {
  const db = dbConfig.database;
  return new Promise((resolve, reject) => {
    function cb(err, result) {
      if (err) return reject(err);
      resolve(result);
    }
    con.query(`CREATE DATABASE ${db}`, (err, result) => {
      if (err) return reject(err);
      con.query(`USE ${db}`, cb);
    });
  });
};

exports.loadDataIntoTable = async (connection, tableName) => {
  let data = payload[tableName];

  const insert = `INSERT INTO ${tableName} SET ?`;
  const attributes = await this.returnAttributesAsync(connection, tableName);
  for (const record of data) {
    const recordAsObject = {};
    let field;
    attributes.forEach((attr, index) => {
      field = attr.Field;
      recordAsObject[field] = record[index];
    });
    await this.insertAsync(connection, insert, recordAsObject);
  }
  const noOfRecords = data.length;
  const created = await this.runNonParametricQueryAsync(
    connection,
    `SELECT * from ${tableName}`
  );
  //to verify that all the records have been inserted.
  ensureEqual(created.length, noOfRecords);
};
exports.insertAsync = async (connection, insert, data) => {
  return new Promise((resolve, reject) => {
    const cb = (err, result) => {
      if (err) reject(err);
      resolve(result);
    };
    connection.query(insert, data, cb);
  });
};

//run fetch question that do not require
//adding of data to the database.
exports.runNonParametricQueryAsync = async (connection, query) => {
  return new Promise((resolve, reject) => {
    const cb = (err, result) => {
      if (err) return reject(err);
      resolve(result);
    };
    connection.query(query, cb);
  });
};

exports.createConnectionAsync = async () => {
  return new Promise((resolve, reject) => {
    const cb = (err) => {
      if (err) return reject(err);
      done();
    };
    const con = createConnectionToDb(cb);
    function done() {
      resolve(con);
    }
  });
};

exports.createConnectionWithDone = (done) => {
  const cb = (err) => {
    if (err) throw new Error(err);
    done();
  };
  return createConnectionToDb(cb);
};

exports.globalCb = (err) => {
  if (err) throw new Error(err);
};

exports.cbWithDone = (done) => {
  const cb = (err) => {
    if (err) throw new Error(err);
    done();
  };
  return cb;
};

exports.createTableAsync = async (connection, config) => {
  const { name, schema } = config;
  const create = `CREATE TABLE ${name}(${schema})`;
  return new Promise((resolve, reject) => {
    const cb = (err, result) => {
      if (err) {
        return reject(err);
      }
      resolve(result);
    };
    connection.query(create, cb);
  });
};

exports.returnAttributesAsync = async function (connection, tableName) {
  return new Promise((resolve, reject) => {
    connection.query(`SHOW COLUMNS FROM  ${tableName} `, (err, result) => {
      if (err) return reject(err);
      resolve(result);
    });
  });
};

exports.deleteTableAsync = async (connection, name) => {
  return new Promise((resolve, reject) => {
    const cb = (err) => {
      if (err) return reject(err);
      resolve(true);
    };
    connection.query(`DROP TABLE ${name}`, cb);
  });
};

exports.createTable = (connection, config, cb) => {
  const { name, schema } = config;
  const create = `CREATE TABLE ${name}(${schema})`;
  connection.query(create, cb);
};
exports.dropTable = (connection, name, cb) => {
  connection.query(`DROP TABLE ${name}`, cb);
};
exports.createTableWithDone = (connection, table, done) => {
  const cb = (err) => {
    if (err) throw new Error(err);
    done();
  };

  this.createTable(connection, table, cb);
};
exports.deleteTableWithDone = (connection, name, done) => {
  const cb = (err) => {
    if (err) throw new Error(err);
    done();
  };
  this.dropTable(connection, name, cb);
};
