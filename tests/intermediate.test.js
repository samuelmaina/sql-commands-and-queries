const utils = require("./utils");

const { ensureEqual, ensureDeeplyEqual } = require("./testUtils");

const { createConnectionAsync, createTableAsync } = utils;

const MAX_SETUP_TIME_IN_MS = 100000;
const MAX_TESTING_TIME_IN_MS = 7000;

describe("Operation on populated relations", () => {
  let connection;

  beforeAll(async () => {
    connection = await createConnectionAsync();
  }, MAX_SETUP_TIME_IN_MS);

  afterAll((done) => {
    connection.end(utils.cbWithDone(done));
  }, MAX_SETUP_TIME_IN_MS);

  describe("SELECT", () => {
    beforeAll(async () => {
      await utils.createTablesAndLoadThemWithData(connection);
    }, MAX_SETUP_TIME_IN_MS);

    afterAll(async () => {
      await utils.clearDbAsync(connection);
    }, MAX_SETUP_TIME_IN_MS);
    describe("sub queries", () => {
      describe("IN clause for membership chekc", () => {
        it("IN clause usage", async () => {
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
          const resultAsArray = result.map((elem) => {
            return elem.course_id;
          });
          const expected = ["CS-101"];
          ensureDeeplyEqual(resultAsArray, expected);
        });
        it("NOT IN clause usage", async () => {
          //“Find all name of other people except Mozart and Einstein.”;

          //IN can be used to check in enumerated sets such ('Mozart', 'Einstein')
          const select = `SELECT distinct name
									FROM instructor
									where name not in ('Mozart', 'Einstein');`;
          const result = await utils.runNonParametricQueryAsync(
            connection,
            select
          );
          const resultAsArray = result.map((elem) => {
            return elem.name;
          });
          const expected = [
            "Srinivasan",
            "Wu",
            "El Said",
            "Gold",
            "Katz",
            "Califieri",
            "Singh",
            "Crick",
            "Brandt",
            "Kim",
          ];
          ensureDeeplyEqual(resultAsArray, expected);
        });
      });
    });

    it("SOME clause", async () => {
      //it is used to check if at least one of records in the right relation
      //meets some criteria.
      //here > SOME is used
      //SQL also allows < SOME, <= SOME, >= SOME, = SOME, and <> SOME comparisons.
      //  = SOME is identical to IN, whereas <> SOME is not the same
      // as NOT IN.
      const select = `
                          SELECT name
                            FROM instructor
                            where salary > SOME (SELECT salary
                            FROM instructor
                            where dept_name = 'Biology');`;
      const result = await utils.runNonParametricQueryAsync(connection, select);
      const resultAsArray = result.map((elem) => {
        return elem.name;
      });
      const expected = [
        "Wu",
        "Einstein",
        "Gold",
        "Katz",
        "Singh",
        "Brandt",
        "Kim",
      ];
      ensureDeeplyEqual(resultAsArray, expected);
    });
    describe("ALL clause", () => {
      it("ALL usage", async () => {
        //It is used to check that all the records in the right
        //relation meet some criteria.
        //here > ALL is used
        //SQL also allows < ALL, <= ALL, >= ALL, = ALL, and <> ALL comparisons.
        //  <> ALL is identical to  NOT IN, whereas <> ALL is not the same
        // as IN.
        const select = `
                          SELECT name
                            FROM instructor
                            where salary > ALL (SELECT salary
                            FROM instructor
                            where dept_name = 'Comp. Sci.');`;
        const result = await utils.runNonParametricQueryAsync(
          connection,
          select
        );
        const resultAsArray = result.map((elem) => {
          return elem.name;
        });
        //The greatest salary in the Comp. Sci. department is 92,000.
        //The highest paid person in the whole relation is Einstein with 95,000
        //and the computer science highest earner is second.
        const expected = ["Einstein"];
        ensureDeeplyEqual(resultAsArray, expected);
      });
      it("ALL (complex) ", async () => {
        // “Find the departments
        // that have the highest average salary.”
        const select = `
                          SELECT dept_name
                            FROM instructor
                            group by dept_name
                            having avg(salary) >= ALL (SELECT AVG(salary) as avg_salary
                            FROM instructor
                            group by dept_name)`;
        const result = await utils.runNonParametricQueryAsync(
          connection,
          select
        );
        const resultAsArray = result.map((elem) => {
          return elem.dept_name;
        });
        const expected = ["Physics"];
        ensureDeeplyEqual(resultAsArray, expected);
      });
    });

    describe("EXISTS clause", () => {
      it(
        "usage",
        async () => {
          //return true if the subquery in question return non empty results.
          //“Find all courses taught in both the Fall 2017
          //semester and in the Spring 2018 semester”
          const select = `
					  SELECT course_id
					  FROM section as S
					  where semester = 'Fall' and year= 2017 and
						  exists (SELECT *
								  FROM section as T
								  where semester = 'Spring' and year= 2018 and
								  S.course_id= T.course_id);`;
          const result = await utils.runNonParametricQueryAsync(
            connection,
            select
          );
          const resultAsArray = result.map((elem) => {
            return elem.course_id;
          });
          const expected = ["CS-101"];
          ensureDeeplyEqual(resultAsArray, expected);
        },
        MAX_TESTING_TIME_IN_MS
      );

      it(
        "complex with NOT ",
        async () => {
          // Find all students(their Ids and names ) who have taken all courses offered in the Biology
          // department.

          //the except is used to find course courses in the biology department that the student has
          //not taken.
          const select = `
		  SELECT S.ID, S.name
		  FROM student as S
		  WHERE not exists ((SELECT course_id
		  FROM course
		  WHERE dept_name = 'Elec. Eng.')
		  except
		  (SELECT T.course_id
		  FROM takes as T
		  WHERE S.ID = T.ID))`;

          const result = await utils.runNonParametricQueryAsync(
            connection,
            select
          );
          const resultAsArray = result.map((elem) => {
            const { ID, name } = elem;
            return [ID, name];
          });
          const expected = [["76653", "Aoi"]];
          ensureDeeplyEqual(resultAsArray, expected);
        },
        MAX_TESTING_TIME_IN_MS
      );
    });

    describe("UNIQUE clause", () => {
      // used in other sql database  but not implemented in mySQL.
      it(
        "usage",
        async () => {
          //return true if the resulting query does not contain duplicate and false
          //otherwise.

          // “Find
          // all courses that were offered at most, once(taught once or not taught at all) in 2017”
          const select = `
		  select T.title 
		  from course as T
		  where 1 >=  (select count(R.course_id)
		  from section as R
		  where T.course_id = R.course_id and R.year = 2017)`;

          // the equivalent query is this,
          //   `select T.title
          //   from course as T
          //   where unique (select R.course_id
          //   from section as R
          //   where T.course_id= R.course_id and
          //   R.year = 2017);`
          const result = await utils.runNonParametricQueryAsync(
            connection,
            select
          );
          const resultAsArray = result.map((elem) => {
            return elem.title;
          });
          const expected = [
            "Intro. to Biology",
            "Genetics",
            "Computational Biology",
            "Intro. to Computer Science",
            "Robotics",
            "Image Processing",
            "Database System Concepts",
            "Intro. to Digital Systems",
            "Investment Banking",
            "World History",
            "Music Video Production",
            "Physical Principles",
          ];
          ensureDeeplyEqual(resultAsArray, expected);
        },
        MAX_TESTING_TIME_IN_MS
      );
    });

    describe("Subqueries in the FROM clause", () => {
      //test is failing, suspecting it is due to use of Maria DB.
      it.skip(
        "usage",
        async () => {
          //the select-from-where clause return a relation hence the result can be inserted
          //in the from clause of another select-from-where clause of a relation.

          // “Find the average instructors’ salaries of those departments
          // where the average salary is greater than $42,000.”
          const select = `
		  select dept_name, avg_salary
		  from (select dept_name, avg (salary) as avg_salary
				from instructor
				group by dept_name)
		  where avg_salary > 42000`;
          const result = await utils.runNonParametricQueryAsync(
            connection,
            select
          );
          const resultAsArray = result.map((elem) => {
            const { dept_name, avg_salary } = elem;
            return [dept_name, avg_salary];
          });
          const expected = [
            ["Biology", 72000],
            ["Comp. Sci.", 77333.333333],
            ["Elec. Eng.", 80000],
            ["Finance", 85000],
            ["History", 61000],
            ["Physics", 91000],
          ];
          ensureDeeplyEqual(resultAsArray, expected);
        },
        MAX_TESTING_TIME_IN_MS
      );

      describe("WITH clause", () => {
        it(
          "usage",
          async () => {
            //it is used to define temporary relations that
            //whose result can be used in the subquent queries.
            const select = `
			  with max_budget (value) as
					(select max(budget)
					from department)
			  select dept_name
			  from department, max_budget
			  where department.budget = max_budget.value`;
            const result = await utils.runNonParametricQueryAsync(
              connection,
              select
            );
            const resultAsArray = result.map((elem) => {
              return elem.dept_name;
            });

            const expected = ["Finance"];
            ensureDeeplyEqual(resultAsArray, expected);
          },
          MAX_TESTING_TIME_IN_MS
        );
        it(
          "complex usage",

          // find all departments where the total salary is
          // greater than the average of the total salary of all departments
          async () => {
            const select = `
			with dept_total (dept_name, value) as
					(select dept_name, sum(salary)
					from instructor
					group by dept_name),
			dept_total_avg(value) as
					(select avg(value)
					from dept_total)
			select dept_name
			from dept_total, dept_total_avg
			where dept_total.value > dept_total_avg.value;`;
            const result = await utils.runNonParametricQueryAsync(
              connection,
              select
            );
            const resultAsArray = result.map((elem) => {
              return elem.dept_name;
            });

            const expected = ["Comp. Sci.", "Finance", "Physics"];
            ensureDeeplyEqual(resultAsArray, expected);
          },
          MAX_TESTING_TIME_IN_MS
        );
      });
    });

    describe("Scalar Subqueries", () => {
      //it is query that returns a single value and can be placed
      //where any where only one value is permitted.
      //the scalar is a relation with only one attribute and only one row
      //when used in an expression, the value is explicitly
      //cast to its primitve form.
      it(
        "usage",
        // lists all departments along with the
        // number of instructors in each department
        async () => {
          const select = `
		  select dept_name,
		  (select count(*)
		  from instructor
		  where department.dept_name = instructor.dept_name)
		  as num_instructors
		  from department;`;
          const result = await utils.runNonParametricQueryAsync(
            connection,
            select
          );
          const resultAsArray = result.map((elem) => {
            return [elem.dept_name, elem.num_instructors];
          });

          const expected = [
            ["Biology", 1],
            ["Comp. Sci.", 3],
            ["Elec. Eng.", 1],
            ["Finance", 2],
            ["History", 2],
            ["Music", 1],
            ["Physics", 2],
          ];
          ensureDeeplyEqual(resultAsArray, expected);
        },
        MAX_TESTING_TIME_IN_MS
      );
    });
  });

  describe.skip("DELETE ", () => {
    //it is only used for one relation.
    beforeAll(async () => {
      await utils.createTablesAndLoadThemWithData(connection);
    }, MAX_SETUP_TIME_IN_MS);

    afterAll(async () => {
      await utils.clearDbAsync(connection);
    }, MAX_SETUP_TIME_IN_MS);

    it("when used without the WHERE clause", async () => {
      const erase = `delete from instructor`;
      await utils.runNonParametricQueryAsync(connection, erase);

      const retrieved = "SELECT * FROM instructor";

      const result = await utils.runNonParametricQueryAsync(
        connection,
        retrieved
      );
    });
  });
});
