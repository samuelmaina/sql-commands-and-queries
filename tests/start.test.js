const sql = require('mysql');

const MAX_TESTING_TIME = 10000;

/**
 * put your database creditials in the different fields.Also make sure the database is also running.
 */

const host = 'localhost';
const user = 'root';
const password = '';

const database = 'people';

describe('starting tests', () => {
	let connection;
	beforeAll(done => {
		const cbWithDone = err => {
			if (err) throw new Error(err);
			done();
		};
		const con = sql.createConnection({
			host,
			user,
			password,
			database,
		});

		con.connect(cbWithDone);
		connection = con;
	});
	it(
		'create table',
		done => {
			const create = `CREATE TABLE trial(
            dept_name VARCHAR(20) NOT NULL,
            faculty VARCHAR(20) NOT NULL,
            budget NUMERIC(12,2),
            type_serial_no CHAR(10) NOT NULL,
            num_of_employees INT(20),
            PRIMARY KEY (dept_name,faculty)
        )`;
			let cb = err => {
				if (err) throw new Error(err);
			};
			connection.query(create, cb);
			cb = err => {
				if (err) throw new Error(err);
				done();
			};
			dropTable('trial', cb);
		},
		MAX_TESTING_TIME
	);
	it(
		'delete table',
		done => {
			const table = 'department';
			const schema = ` dept_name VARCHAR(20) NOT NULL,
            faculty VARCHAR(20) NOT NULL,
            budget NUMERIC(12,2),
            type_serial_no CHAR(10) NOT NULL,
            num_of_employees INT(20),
            PRIMARY KEY (dept_name,faculty)`;
			createTable(table, schema, () => undefined);
			cb = err => {
				if (err) throw new Error(err);
				done();
			};
			connection.query(`DROP TABLE ${table}`, cb);
		},
		MAX_TESTING_TIME
	);
	describe('alter table schema', () => {
		const table = 'department';
		beforeEach(done => {
			const cbWithDone = err => {
				if (err) throw new Error(err);
				done();
			};
			const schema = ` dept_name VARCHAR(20) NOT NULL,
            faculty VARCHAR(20) NOT NULL,
            budget NUMERIC(12,2),
            type_serial_no CHAR(10) NOT NULL,
            num_of_employees INT(20),
            PRIMARY KEY (dept_name,faculty)`;
			createTable(table, schema, cbWithDone);
		});
		afterEach(done => {
			const cbWithDone = err => {
				if (err) throw new Error(err);
				done();
			};
			dropTable(table, cbWithDone);
		});
		it('adding attributes', done => {
			cb = err => {
				if (err) throw new Error(err);
				done();
			};
			connection.query(`ALTER TABLE ${table} ADD vision VARCHAR(50)`, cb);
		});
		it('removing attributes', done => {
			const field = 'num_of_employees';
			cb = err => {
				if (err) throw new Error(err);
				done();
			};
			connection.query(`ALTER TABLE ${table} DROP ${field}  `, cb);
		});
	});

	describe('insert into tables', () => {
		const tableName = 'department';

		beforeEach(done => {
			const schema = `dept_name VARCHAR(20),
                        building VARCHAR(20),
                        budget NUMERIC(12,2),
                        PRIMARY KEY(dept_name)`;
			createTable(tableName, schema, done);
		});
		afterEach(done => {
			const cb = err => {
				if (err) throw new Error(err);
				done();
			};
			dropTable(tableName, cb);
		});
		it(
			'inserting data in bulk',
			done => {
				const department1 = {
					dept_name: 'Math',
					building: 'Prof Ken',
					budget: 1345.68,
				};
				const insert = `INSERT INTO ${tableName} SET ?`;
				cb = err => {
					if (err) throw new Error(err);
				};
				connection.query(insert, department1, cb);
				const fetchAll = `SELECT * FROM ${tableName}`;
				cb = (err, results) => {
					if (err) throw new Error(err);
					for (const result of results) {
						expect(result.dept_name).toBe(department1.dept_name);
						expect(result.building).toBe(department1.building);
						expect(result.budget).toBe(department1.budget);
					}
					done();
				};
				connection.query(fetchAll, cb);
			},
			MAX_TESTING_TIME
		);
	});

	describe.skip('delete data from table', () => {
		const tableName = 'department';
		const TRIALS = 20;
		beforeEach(done => {
			const schema = `dept_name VARCHAR(20),
                        building VARCHAR(20),
                        budget NUMERIC(12,2),
                        PRIMARY KEY(dept_name)`;
			createTable(tableName, schema, () => undefined);

			let cb = err => {
				if (err) throw new Error(err);
			};
			for (let i = 0; i < TRIALS; i++) {
				const department1 = {
					dept_name: `Math ${i}`,
					building: `Prof Ken ${i}`,
					budget: (Math.random() * TRIALS).toFixed(2),
				};
				const insert = `INSERT INTO ${tableName} SET ?`;
				connection.query(insert, department1, cb);
			}
			done();
		});
		afterEach(() => {
			dropTable(tableName);
		});
		it(
			'',
			done => {
				let cb = err => {
					if (err) {
						throw new Error(err);
					}
				};

				for (let i = 0; i < TRIALS; i++) {
					const del = `DELETE FROM ${tableName} WHERE dept_name= 'Math ${i}'`;
					connection.query(del, cb);
				}

				const fetchAll = `SELECT * FROM ${tableName}`;
				cb = (err, results) => {
					if (err) throw new Error(err);
					expect(results.length).toBe(0);
					done();
				};
				connection.query(fetchAll, cb);
			},
			MAX_TESTING_TIME
		);
	});

	const createTable = (name, schema, cb) => {
		const create = `CREATE TABLE ${name}(${schema})`;
		connection.query(create, cb);
	};
	const dropTable = (name, cb) => {
		connection.query(`DROP TABLE ${name}`, cb);
	};
});
