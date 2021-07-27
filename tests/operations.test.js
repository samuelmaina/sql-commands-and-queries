const utils = require('./utils');

const { university } = require('./schemas');
const payload = require('./data');
const { ensureEqual, ensureDeeplyEqual } = require('./testUtils');
const { dbConfig } = require('../config');
const { createConnectionAsync, createTableAsync } = utils;

const MAX_TESTING_TIME = 20000;
describe('Operation on populated relations', () => {
	let connection;
	beforeAll(async () => {
		connection = await createConnectionAsync();
	});
	afterAll(() => {
		connection.end();
	});
	describe('Select', () => {
		beforeAll(async () => {
			const schemas = Object.values(university);
			//create the tables separately so that some  are not affected by
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
		});
		it('simple select', async () => {
			const select = `SELECT name FROM instructor`;
			const result = await runNonParametricQueryAsync(select);

			const resultAsArray = result.map(elem => {
				return elem.name;
			});
			const expected = [
				'Srinivasan',
				'Wu',
				'Mozart',
				'Einstein',
				'El Said',
				'Gold',
				'Katz',
				'Califieri',
				'Singh',
				'Crick',
				'Brandt',
				'Kim'
			];
			ensureDeeplyEqual(expected, resultAsArray);
		});
		describe('Select with or without duplicates', () => {
			it('with duplicates', async () => {
				//the "SELECT A"  works the same as "SELECT ALL A"
				const select = `SELECT ALL dept_name
									   FROM instructor`;
				const result = await runNonParametricQueryAsync(select);

				const resultAsArray = result.map(elem => {
					return elem.dept_name;
				});

				//mySQL will sort the data in ascending order by difault for foreign
				//field when querying. This may or may not be the case for other sql dbs.
				const expected = [
					'Biology',
					'Comp. Sci.',
					'Comp. Sci.',
					'Comp. Sci.',
					'Elec. Eng.',
					'Finance',
					'Finance',
					'History',
					'History',
					'Music',
					'Physics',
					'Physics'
				];
				ensureDeeplyEqual(expected, resultAsArray);
			});
			it('without duplicates', async () => {
				//the same query as above but duplication is removed from the results.
				const select = `SELECT DISTINCT dept_name
									   FROM instructor`;
				const result = await runNonParametricQueryAsync(select);

				const resultAsArray = result.map(elem => {
					return elem.dept_name;
				});

				const expected = [
					'Biology',
					'Comp. Sci.',
					'Elec. Eng.',
					'Finance',
					'History',
					'Music',
					'Physics'
				];
				ensureDeeplyEqual(expected, resultAsArray);
			});
		});
		describe('Aggregate and arithemtic operations on the select clause', () => {});
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
