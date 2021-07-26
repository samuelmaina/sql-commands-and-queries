const sql = require('mysql');

const MAX_TESTING_TIME = 10000;

/**
 * put your database creditials in the different fields.Also make sure the database is also running.
 */

const host = 'localhost';
const user = 'root';
const password = '';

const database = 'people';

const globalCb = err => {
	if (err) throw new Error(err);
};

describe('starting tests', () => {
	let connection;
	beforeAll(done => {
		const cb = err => {
			if (err) throw new Error(err);
			done();
		};
		const con = sql.createConnection({
			host,
			user,
			password,
			database,
		});

		con.connect(cb);
		connection = con;
	});
	afterAll(done => {
		connection.end();
		done();
	});
	it(
		'create table',
		done => {
			const name = 'trial';

			const create = `CREATE TABLE ${name}
			(
            dept_name VARCHAR(20) NOT NULL,
            faculty VARCHAR(20) NOT NULL,
            budget NUMERIC(12,2),
            type_serial_no CHAR(10) NOT NULL,
            num_of_employees INT(20),
            PRIMARY KEY (dept_name,faculty)
        )`;
			connection.query(create, globalCb);
			dropTable(name, globalCb);
			done();
		},
		MAX_TESTING_TIME
	);
	it(
		'delete table',
		done => {
			const table = {
				name: 'department',
				schema: `dept_name VARCHAR(20) NOT NULL,
                         faculty VARCHAR(20) NOT NULL,
                         budget NUMERIC(12,2),
                         type_serial_no CHAR(10) NOT NULL,
                         num_of_employees INT(20),
                         PRIMARY KEY (dept_name,faculty)`,
			};

			createTable(table, globalCb);

			cb = err => {
				if (err) throw new Error(err);
				done();
			};
			connection.query(`DROP TABLE ${table.name}`, cb);
		},
		MAX_TESTING_TIME
	);
	describe('alter table schema', () => {
		const table = {
			name: 'departments',
			schema: ` dept_name VARCHAR(20) NOT NULL,
            faculty VARCHAR(20) NOT NULL,
            budget NUMERIC(12,2),
            type_serial_no CHAR(10) NOT NULL,
            num_of_employees INT(20),
            PRIMARY KEY (dept_name,faculty)`,
		};
		beforeEach(done => {
			const cb = err => {
				if (err) throw new Error(err);
				done();
			};

			createTable(table, cb);
		});
		afterEach(done => {
			const cb = err => {
				if (err) throw new Error(err);
				done();
			};
			dropTable(table.name, cb);
		});
		it('adding attributes', done => {
			const cb = err => {
				if (err) throw new Error(err);
				done();
			};
			connection.query(`ALTER TABLE ${table.name} ADD vision VARCHAR(50)`, cb);
		});
		it('removing attributes', done => {
			const field = 'num_of_employees';
			const cb = err => {
				if (err) throw new Error(err);
				done();
			};
			connection.query(`ALTER TABLE ${table.name} DROP ${field}  `, cb);
		});
	});

	describe('insert into tables', () => {
		const table = {
			name: 'department',
			schema: `dept_name VARCHAR(20),
                        building VARCHAR(20),
                        budget NUMERIC(12,2),
                        PRIMARY KEY(dept_name)`,
		};
		beforeEach(done => {
			const cb = err => {
				if (err) throw new Error(err);
				done();
			};
			createTable(table, cb);
		});
		afterEach(done => {
			const cb = err => {
				if (err) throw new Error(err);
				done();
			};
			dropTable(table.name, cb);
		});
		it(
			'inserting data in bulk',
			done => {
				const department1 = {
					dept_name: 'Math',
					building: 'Prof Ken',
					budget: 1345.68,
				};
				const { name } = table;
				const insert = `INSERT INTO ${name} SET ?`;

				connection.query(insert, department1, globalCb);
				const fetchAll = `SELECT * FROM ${name}`;
				const cb = (err, results) => {
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

	describe('delete data from table', () => {
		const table = {
			name: 'department',
			schema: `dept_name VARCHAR(20),
                     building VARCHAR(20),
                     budget NUMERIC(12,2),
                     PRIMARY KEY(dept_name)
                    `,
		};
		const TRIALS = 20;
		beforeEach(done => {
			createTable(table, globalCb);

			for (let i = 0; i < TRIALS; i++) {
				const department1 = {
					dept_name: `Math ${i}`,
					building: `Prof Ken ${i}`,
					budget: (Math.random() * TRIALS).toFixed(2),
				};
				const insert = `INSERT INTO ${table.name} SET ?`;
				connection.query(insert, department1, globalCb);
			}
			done();
		});
		afterEach(done => {
			const cb = err => {
				if (err) throw new Error(err);
				done();
			};
			dropTable(table.name, cb);
		});
		it(
			'delete data for one field',
			done => {
				const { name } = table;
				for (let i = 0; i < TRIALS; i++) {
					const del = `DELETE FROM ${name} WHERE dept_name= 'Math ${i}'`;
					connection.query(del, globalCb);
				}

				const fetchAll = `SELECT * FROM ${name}`;
				cb = (err, results) => {
					if (err) throw new Error(err);

					//we have deleted every thing so we expect to have nothing in the database.
					expect(results.length).toBe(0);
					done();
				};
				connection.query(fetchAll, cb);
			},
			MAX_TESTING_TIME
		);
	});

	const createTable = (config, cb) => {
		const { name, schema } = config;
		const create = `CREATE TABLE ${name}(${schema})`;
		connection.query(create, cb);
	};
	const dropTable = (name, cb) => {
		connection.query(`DROP TABLE ${name}`, cb);
	};
});
