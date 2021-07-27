const utils = require('./utils');

const { university } = require('./schemas');
const payload = require('./data');
const { ensureEqual } = require('./testUtils');
const { dbConfig } = require('../config');
const { createConnectionAsync, createTableAsync } = utils;

const MAX_TESTING_TIME = 20000;
describe('Operation on populated relations', () => {
	let connection;
	beforeAll(async () => {
		connection = await createConnectionAsync();
		const schemas = Object.values(university);

		//create the tables separately so that they are not affected by
		//errors generated during insertions.
		for (const schema of schemas) {
			await createTableAsync(connection, schema);
		}
		let tableName, data;

		for (const schema of schemas) {
			tableName = schema.name;
			data = payload[tableName];
			const insert = `INSERT INTO ${tableName} SET ?`;
			const attributes = await returnAttributesAsync(tableName);
			for (const record of data) {
				const recordAsObject = {};
				let field;
				attributes.forEach((attr, index) => {
					field = attr.Field;
					recordAsObject[field] = record[index];
				});
				await insertAsync(insert, recordAsObject);
			}
			const noOfRecords = data.length;
			const created = await runNonParametricQueryAsync(
				`SELECT * from ${tableName}`
			);
			//to verify that all the records have been inserted.
			ensureEqual(created.length, noOfRecords);
		}
	}, MAX_TESTING_TIME);
	afterAll(async () => {
		await runNonParametricQueryAsync(`DROP DATABASE ${dbConfig.database}`);
		connection.end();
	});
	it('should just pass', () => {
		expect(1).toBe(1);
	});

	async function returnAttributesAsync(tableName) {
		return new Promise((resolve, reject) => {
			connection.query(`SHOW COLUMNS FROM  ${tableName} `, (err, result) => {
				if (err) return reject(err);
				resolve(result);
			});
		});
	}

	async function insertAsync(insert, data) {
		return new Promise((resolve, reject) => {
			const cb = (err, result) => {
				if (err) reject(err);
				resolve(result);
			};
			connection.query(insert, data, cb);
		});
	}
	//run query that does not require data from the user.
	async function runNonParametricQueryAsync(query) {
		return new Promise((resolve, reject) => {
			const cb = (err, result) => {
				if (err) return reject(err);
				resolve(result);
			};
			connection.query(query, cb);
		});
	}
	function createTableWithDone(table, done) {
		utils.createTableWithDone(connection, table, done);
	}
	function deleteTableWithDone(name, done) {
		utils.deleteTableWithDone(connection, name, done);
	}

	function createTable(conf, cb) {
		utils.createTable(connection, conf, cb);
	}
	function dropTable(tableName, cb) {
		utils.dropTable(connection, tableName, cb);
	}
});
