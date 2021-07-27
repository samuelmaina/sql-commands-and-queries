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
		describe('Aggregate and arithemtic operations on the select clause', () => {
			it('operations like multiplication', async () => {
				const incrementedSelect = `SELECT ID,name, dept_name,salary *1.1 as salary
									   	   FROM instructor`;
				const incremented = await runNonParametricQueryAsync(incrementedSelect);

				const normalSelect = `SELECT *
								FROM instructor`;
				const previous = await runNonParametricQueryAsync(normalSelect);

				previous.forEach((record, index) => {
					ensureEqual(
						Number((record.salary * 1.1).toFixed(0)),
						incremented[index].salary
					);
				});
			});
		});
		describe('select combining tables and filtering return results', () => {
			it('the behaviour of the from clause', async () => {
				//the from clause creates
				const select = `SELECT *
								FROM instructor, department;`;
				const result = await runNonParametricQueryAsync(select);
				const { instructor, department } = payload;
				//creates a cartesian product for the tuples(records) in each relation.
				//according to math the cardinarity of a cartesian product is the product of the cardinarity of  each relation(set).
				ensureEqual(result.length, instructor.length * department.length);
			});
			it('from  together with where  for one table', async () => {
				//sql allows connectives NOT, OR and AND  and the other logical operators in the
				//WHERE clause(<,<=, >, >=, <> and =)
				//from combines two or more table that will be used for querying of data.
				const select = `SELECT name
									FROM instructor
									WHERE dept_name = 'Comp. Sci.' AND salary > 70000;`;
				const result = await runNonParametricQueryAsync(select);
				const resultAsArray = result.map(elem => {
					return elem.name;
				});
				const expected = ['Katz', 'Brandt'];
				ensureDeeplyEqual(resultAsArray, expected);
			});
			it('from  together with where  for multiple tables', async () => {
				//the from clause creates
				const select = `SELECT name, instructor.dept_name, building
									FROM instructor, department
									WHERE instructor.dept_name= department.dept_name;`;
				const result = await runNonParametricQueryAsync(select);
				const resultAsArray = result.map(elem => {
					const { name, dept_name, building } = elem;
					return [name, dept_name, building];
				});
				//mySQL will sort the data in ascending order by difault for foreign(dept_name)
				//field when querying. This may or may not be the case for other sql dbs.
				const expected = [
					['Crick', 'Biology', 'Watson'],
					['Srinivasan', 'Comp. Sci.', 'Taylor'],
					['Katz', 'Comp. Sci.', 'Taylor'],
					['Brandt', 'Comp. Sci.', 'Taylor'],
					['Kim', 'Elec. Eng.', 'Taylor'],
					['Wu', 'Finance', 'Painter'],
					['Singh', 'Finance', 'Painter'],
					['El Said', 'History', 'Painter'],
					['Califieri', 'History', 'Painter'],
					['Mozart', 'Music', 'Packard'],
					['Einstein', 'Physics', 'Watson'],
					['Gold', 'Physics', 'Watson']
				];
				ensureDeeplyEqual(resultAsArray, expected);
			});
		});

		describe('renaming', () => {
			it('using AS ', async () => {
				//AS used in the from clause is used to reduce names of long relations.
				const select = `SELECT name, D.dept_name AS dept_name, building
									FROM instructor AS I , department as D
									WHERE I.dept_name= D.dept_name;`;
				const result = await runNonParametricQueryAsync(select);
				const resultAsArray = result.map(elem => {
					const { name, dept_name, building } = elem;
					return [name, dept_name, building];
				});
				const expected = [
					['Crick', 'Biology', 'Watson'],
					['Srinivasan', 'Comp. Sci.', 'Taylor'],
					['Katz', 'Comp. Sci.', 'Taylor'],
					['Brandt', 'Comp. Sci.', 'Taylor'],
					['Kim', 'Elec. Eng.', 'Taylor'],
					['Wu', 'Finance', 'Painter'],
					['Singh', 'Finance', 'Painter'],
					['El Said', 'History', 'Painter'],
					['Califieri', 'History', 'Painter'],
					['Mozart', 'Music', 'Packard'],
					['Einstein', 'Physics', 'Watson'],
					['Gold', 'Physics', 'Watson']
				];
				ensureDeeplyEqual(resultAsArray, expected);
			});
		});

		it('ordering the results', async () => {
			//first order with salary in descending order
			//if there are records with the same salaries,
			//order them with the name in ASC order.
			//if DESC is left out the default is ASC,
			const select = `SELECT *
								FROM instructor
								ORDER BY salary DESC, name ASC;`;
			const result = await runNonParametricQueryAsync(select);
			const resultAsArray = result.map(elem => {
				const { ID, name, dept_name, salary } = elem;
				return [ID, name, dept_name, salary];
			});
			const expected = [
				['22222', 'Einstein', 'Physics', 95000],
				['83821', 'Brandt', 'Comp. Sci.', 92000],
				['12121', 'Wu', 'Finance', 90000],
				['33456', 'Gold', 'Physics', 87000],
				['98345', 'Kim', 'Elec. Eng.', 80000],
				['76543', 'Singh', 'Finance', 80000],
				['45565', 'Katz', 'Comp. Sci.', 75000],
				['76766', 'Crick', 'Biology', 72000],
				['10101', 'Srinivasan', 'Comp. Sci.', 65000],
				['58583', 'Califieri', 'History', 60000],
				['32343', 'El Said', 'History', 60000],
				['15151', 'Mozart', 'Music', 40000]
			];
			ensureDeeplyEqual(resultAsArray, expected);
		});
	});
	//TODO : for string matching in the WHERE clause(page 112/1273)
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
