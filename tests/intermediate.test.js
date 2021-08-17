const utils = require('./utils');

const { ensureEqual, ensureDeeplyEqual } = require('./testUtils');

const { createConnectionAsync, createTableAsync } = utils;

const MAX_SETUP_TIME_IN_MS = 30000;
const MAX_TESTING_TIME_IN_MS = 20000;

describe('Operation on populated relations', () => {
	let connection;
	beforeAll(async () => {
		connection = await createConnectionAsync();
		await utils.createTablesAndLoadThemWithData(connection);
	}, MAX_SETUP_TIME_IN_MS);
	afterAll(async () => {
		await utils.clearDbAsync(connection);
		connection.end();
	}, MAX_SETUP_TIME_IN_MS);

	describe('Select', () => {
		describe('sub queries', () => {
			describe('IN clause for membership chekc', () => {
				it('IN clause usage', async () => {
					//“Find all the courses taught in the both the
					// Fall 2017 and Spring 2018 semesters.”;

					//this is the same query as the one done with the intersection.
					//distinct must be used since  INTERSECTION removes duplicate by
					//default.
					const select = `SELECT distinct  course_id
                                FROM section
                               WHERE semester = 'Fall' AND year= 2017 AND
                                course_id IN (SELECT course_id
                                FROM section
                               WHERE semester = 'Spring' AND year= 2018);`;
					const result = await utils.runNonParametricQueryAsync(
						connection,
						select
					);
					const resultAsArray = result.map(elem => {
						return elem.course_id;
					});
					const expected = ['CS-101'];
					ensureDeeplyEqual(resultAsArray, expected);
				});
				it('NOT IN clause usage', async () => {
					//“Find all name of other people except Mozart and Einstein.”;

					//IN can be used to check in enumerated sets such ('Mozart', 'Einstein')
					const select = `select distinct name
									from instructor
									where name not in ('Mozart', 'Einstein');`;
					const result = await utils.runNonParametricQueryAsync(
						connection,
						select
					);
					const resultAsArray = result.map(elem => {
						return elem.name;
					});
					const expected = [
						'Srinivasan',
						'Wu',
						'El Said',
						'Gold',
						'Katz',
						'Califieri',
						'Singh',
						'Crick',
						'Brandt',
						'Kim',
					];
					ensureDeeplyEqual(resultAsArray, expected);
				});
			});
		});

		it('SOME clause', async () => {
			//it is used to check if at least one of records in the right relation
			//meets some criteria.
			//here > SOME is used
			//SQL also allows < SOME, <= SOME, >= SOME, = SOME, and <> SOME comparisons.
			//  = SOME is identical to IN, whereas <> SOME is not the same
			// as NOT IN.
			const select = `
                          select name
                            from instructor
                            where salary > SOME (select salary
                            from instructor
                            where dept_name = 'Biology');`;
			const result = await utils.runNonParametricQueryAsync(connection, select);
			const resultAsArray = result.map(elem => {
				return elem.name;
			});
			const expected = [
				'Wu',
				'Einstein',
				'Gold',
				'Katz',
				'Singh',
				'Brandt',
				'Kim',
			];
			ensureDeeplyEqual(resultAsArray, expected);
		});
		describe('ALL clause', () => {
			it('ALL usage', async () => {
				//It is used to check that all the records in the right
				//relation meet some criteria.
				//here > ALL is used
				//SQL also allows < ALL, <= ALL, >= ALL, = ALL, and <> ALL comparisons.
				//  <> ALL is identical to  NOT IN, whereas <> ALL is not the same
				// as IN.
				const select = `
                          select name
                            from instructor
                            where salary > ALL (select salary
                            from instructor
                            where dept_name = 'Comp. Sci.');`;
				const result = await utils.runNonParametricQueryAsync(
					connection,
					select
				);
				const resultAsArray = result.map(elem => {
					return elem.name;
				});
				//The greatest salary in the Comp. Sci. department is 92,000.
				//The highest paid person in the whole relation is Einstein with 95,000
				//and the computer science highest earner is second.
				const expected = ['Einstein'];
				ensureDeeplyEqual(resultAsArray, expected);
			});
			it('ALL (complex) ', async () => {
				// “Find the departments
				// that have the highest average salary.”
				const select = `
                          select dept_name
                            from instructor
                            group by dept_name
                            having avg(salary) >= ALL (select AVG(salary) as avg_salary
                            from instructor
                            group by dept_name)`;
				const result = await utils.runNonParametricQueryAsync(
					connection,
					select
				);
				const resultAsArray = result.map(elem => {
					return elem.dept_name;
				});
				const expected = ['Physics'];
				ensureDeeplyEqual(resultAsArray, expected);
			});
		});

		it(
			'EXISTS clause',
			async () => {
				//return true if the subquery in question return non empty results.
				//“Find all courses taught in both the Fall 2017
				//semester and in the Spring 2018 semester”
				const select = `
                    select course_id
                    from section as S
                    where semester = 'Fall' and year= 2017 and
                        exists (select *
                                from section as T
                                where semester = 'Spring' and year= 2018 and
                                S.course_id= T.course_id);`;
				const result = await utils.runNonParametricQueryAsync(
					connection,
					select
				);
				const resultAsArray = result.map(elem => {
					return elem.course_id;
				});
				const expected = ['CS-101'];
				ensureDeeplyEqual(resultAsArray, expected);
			},
			MAX_TESTING_TIME_IN_MS
		);
	});
});
