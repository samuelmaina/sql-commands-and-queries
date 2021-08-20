const utils = require("./utils");

const { ensureEqual, ensureDeeplyEqual } = require("./testUtils");

const { createConnectionAsync, createTableAsync } = utils;

const MAX_SETUP_TIME_IN_MS = 100000;
const MAX_TESTING_TIME_IN_MS = 30000;

describe("Operation on populated relations", () => {
  let connection;

  beforeAll(async () => {
    connection = await createConnectionAsync();
  }, MAX_SETUP_TIME_IN_MS);

  afterAll((done) => {
    connection.end(utils.cbWithDone(done));
  }, MAX_SETUP_TIME_IN_MS);

  describe.skip("SELECT", () => {
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

      //skipped the due to the except construct that is not supported by MySQL
      it.skip(
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

    //mySQl treats UNIQUE the same way as DISTINCT.

    describe.skip("UNIQUE clause", () => {
      // used in other sql database  but not implemented in mySQL.
      it(
        "usage",
        async () => {
          //return true if the resulting query does not contain duplicate and false
          //otherwise.

          // “Find
          // all courses that were offered at most, once(taught once or not taught at all) in 2017”
          const select = `select T.title
            from course as T
            where unique (select R.course_id
            from section as R
            where T.course_id= R.course_id and
            R.year = 2017)`;

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
      it(
        "usage",
        async () => {
          //the select-from-where clause return a relation hence the result can be inserted
          //in the from clause of another select-from-where clause of a relation.
          //For mySQL  and postgres, the relation in the from clause must be given a name even if the name is not being
          //referenced.

          // “Find the average instructors’ salaries of those departments
          // where the average salary is greater than $42,000.”
          const select = `
          select dept_name, avg_salary
          from (select dept_name, avg (salary)
                from instructor
                group by dept_name)
                as dept_avg (dept_name, avg_salary)
          where avg_salary > 42000;`;
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
      it(
        "more usage",
        async () => {
          // find the maximum across all departments
          // of the total of all instructors’ salaries in each department.
          const select = `
          select max (tot_salary) as max_total
          from (select dept_name, sum(salary)
              from instructor
              group by dept_name) as dept_total (dept_name, tot_salary);`;
          const result = await utils.runNonParametricQueryAsync(
            connection,
            select
          );
          const resultAsArray = result.map((elem) => {
            return elem.max_total;
          });
          const expected = [232000];
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
      it(
        "LITERAL clause",
        async () => {
          //it is used to access an alias name in the parent from clause or in the current query.

          // “Find the average instructors’ salaries of those departments
          // where the average salary is greater than $42,000.”
          const select = `
         select name, salary, avg_salary
         from instructor I1, lateral (select avg(salary) as avg_salary
                        from instructor I2
                        where I2.dept_name= I1.dept_name) as dept_avg(avg_salary);`;
          const result = await utils.runNonParametricQueryAsync(
            connection,
            select
          );
          const resultAsArray = result.map((elem) => {
            const { name, salary, avg_salary } = elem;
            return [name, salary, avg_salary];
          });
          const expected = [
            ["Srinivasan", 65000, 77333.333333],
            ["Wu", 90000, 85000],
            ["Mozart", 40000, 40000],
            ["Einstein", 95000, 91000],
            ["El Said", 60000, 61000],
            ["Gold", 87000, 91000],
            ["Katz", 75000, 77333.333333],
            ["Califieri", 62000, 61000],
            ["Singh", 80000, 85000],
            ["Crick", 72000, 72000],
            ["Brandt", 92000, 77333.333333],
            ["Kim", 80000, 80000],
          ];

          ensureDeeplyEqual(resultAsArray, expected);
        },
        MAX_TESTING_TIME_IN_MS
      );
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
    afterEach(async () => {
      await utils.clearDbAsync(connection);
    }, MAX_SETUP_TIME_IN_MS);

    it(
      "when used without the WHERE clause",
      async () => {
        // department must be created although we are deleting from
        // the instructor relation. instructor references dept_name.
        await utils.createTablesAndLoadThemWithData(connection, [
          "department",
          "instructor",
        ]);
        const erase = `delete from instructor`;
        await utils.runNonParametricQueryAsync(connection, erase);

        const retrieved = "SELECT * FROM instructor";

        const result = await utils.runNonParametricQueryAsync(
          connection,
          retrieved
        );
        ensureDeeplyEqual(result, []);
      },
      MAX_TESTING_TIME_IN_MS
    );

    it(
      "when combined with the WHERE  clause",
      async () => {
        //the WHERE clause can be used in the same case as the other including multiple
        //relations.

        //Performing all the tests before performing any deletion is important—if some
        // tuples are deleted before other tuples have been tested,
        //final result of the delete would depend on the order in which the tuples were
        // processed!

        await utils.createTablesAndLoadThemWithData(connection, [
          "department",
          "instructor",
        ]);

        //Delete all the instructors whose departments are  in the 'Watson' building.
        const erase = `delete from instructor
                     where dept_name in (select dept_name
                                        from department
                                        where building = 'Watson');`;
        await utils.runNonParametricQueryAsync(connection, erase);

        const retrieved = "SELECT * FROM instructor";

        const result = await utils.runNonParametricQueryAsync(
          connection,
          retrieved
        );

        const resultAsArray = result.map((elem) => {
          const { ID, name, dept_name, salary } = elem;
          return [ID, name, dept_name, salary];
        });
        //only the Physics and Biology Departments are in the Watson building, hence
        //no result should contain the two departments.
        const expected = [
          ["10101", "Srinivasan", "Comp. Sci.", 65000],
          ["12121", "Wu", "Finance", 90000],
          ["15151", "Mozart", "Music", 40000],
          ["32343", "El Said", "History", 60000],
          ["45565", "Katz", "Comp. Sci.", 75000],
          ["58583", "Califieri", "History", 62000],
          ["76543", "Singh", "Finance", 80000],
          ["83821", "Brandt", "Comp. Sci.", 92000],
          ["98345", "Kim", "Elec. Eng.", 80000],
        ];

        ensureDeeplyEqual(resultAsArray, expected);
      },
      MAX_TESTING_TIME_IN_MS
    );
  });

  describe.skip("INSERT ", () => {
    //it is only used for one relation.
    afterEach(async () => {
      await utils.clearDbAsync(connection);
    }, MAX_SETUP_TIME_IN_MS);

    it(
      "When the columns are not specified.",
      async () => {
        //course references dept_name.
        await utils.createTablesAndLoadThemWithData(connection, [
          "department",
          "course",
        ]);
        //the value to be inserted must follow the order in which the
        //columns appear in the schema.
        const insert = `insert into course
                      values ('CS-437', 'Database Systems', 'Comp. Sci.', 4);`;
        await utils.runNonParametricQueryAsync(connection, insert);

        const retrieved = "SELECT * FROM course";

        const result = await utils.runNonParametricQueryAsync(
          connection,
          retrieved
        );

        const resultAsArray = result.map((elem) => {
          const { course_id, title, dept_name, credits } = elem;
          return [course_id, title, dept_name, credits];
        });

        //com 437 is added to the result.
        const expected = [
          ["BIO-101", "Intro. to Biology", "Biology", 4],
          ["BIO-301", "Genetics", "Biology", 4],
          ["BIO-399", "Computational Biology", "Biology", 3],
          ["CS-101", "Intro. to Computer Science", "Comp. Sci.", 4],
          ["CS-190", "Game Design", "Comp. Sci.", 4],
          ["CS-315", "Robotics", "Comp. Sci.", 3],
          ["CS-319", "Image Processing", "Comp. Sci.", 3],
          ["CS-347", "Database System Concepts", "Comp. Sci.", 3],
          ["CS-437", "Database Systems", "Comp. Sci.", 4],
          ["EE-181", "Intro. to Digital Systems", "Elec. Eng.", 3],
          ["FIN-201", "Investment Banking", "Finance", 3],
          ["HIS-351", "World History", "History", 3],
          ["MU-199", "Music Video Production", "Music", 3],
          ["PHY-101", "Physical Principles", "Physics", 4],
        ];
        ensureDeeplyEqual(resultAsArray, expected);
      },
      MAX_TESTING_TIME_IN_MS
    );

    it(
      "When the colums are specified.",
      async () => {
        //course references dept_name.
        await utils.createTablesAndLoadThemWithData(connection, [
          "department",
          "course",
        ]);
        //in this case, the order of columns in the schema does not matter.
        const insert = `insert into course (title, course_id, credits, dept_name)
                              values ('Database Systems', 'CS-437', 4, 'Comp. Sci.');`;
        await utils.runNonParametricQueryAsync(connection, insert);

        const retrieved = "SELECT * FROM course";

        const result = await utils.runNonParametricQueryAsync(
          connection,
          retrieved
        );

        const resultAsArray = result.map((elem) => {
          const { course_id, title, dept_name, credits } = elem;
          return [course_id, title, dept_name, credits];
        });

        //com 437 is added to the result.
        const expected = [
          ["BIO-101", "Intro. to Biology", "Biology", 4],
          ["BIO-301", "Genetics", "Biology", 4],
          ["BIO-399", "Computational Biology", "Biology", 3],
          ["CS-101", "Intro. to Computer Science", "Comp. Sci.", 4],
          ["CS-190", "Game Design", "Comp. Sci.", 4],
          ["CS-315", "Robotics", "Comp. Sci.", 3],
          ["CS-319", "Image Processing", "Comp. Sci.", 3],
          ["CS-347", "Database System Concepts", "Comp. Sci.", 3],
          ["CS-437", "Database Systems", "Comp. Sci.", 4],
          ["EE-181", "Intro. to Digital Systems", "Elec. Eng.", 3],
          ["FIN-201", "Investment Banking", "Finance", 3],
          ["HIS-351", "World History", "History", 3],
          ["MU-199", "Music Video Production", "Music", 3],
          ["PHY-101", "Physical Principles", "Physics", 4],
        ];
        ensureDeeplyEqual(resultAsArray, expected);
      },
      MAX_TESTING_TIME_IN_MS
    );
    it.only(
      "insertion on the basis of the result of a query",
      async () => {
        //course references dept_name.
        await utils.createTablesAndLoadThemWithData(connection, [
          "department",
          "instructor",
          "student",
        ]);
        //the result of the query will be inserted into the instructor relation

        //query: make each student in the Music department who has earned
        // more than 110 credit hours an instructor in the Biology department with a salary of
        // $30,000

        //18000 is added to each tuple in the result of the query
        const insert = `insert into instructor
                          select ID, name, dept_name, 30000
                          from student
                          where dept_name = 'Biology' and tot_cred > 110;`;

        await utils.runNonParametricQueryAsync(connection, insert);

        const retrieved = "SELECT * FROM instructor";

        const result = await utils.runNonParametricQueryAsync(
          connection,
          retrieved
        );
        const resultAsArray = result.map((elem) => {
          const { ID, name, dept_name, salary } = elem;
          return [ID, name, dept_name, salary];
        });
        console.log(resultAsArray);

        //Tanaka has 120 points in the biology department, she is added to the  instructor relation
        //given a salary  of $40,000.
        const expected = [
          ["10101", "Srinivasan", "Comp. Sci.", 65000],
          ["12121", "Wu", "Finance", 90000],
          ["15151", "Mozart", "Music", 40000],
          ["22222", "Einstein", "Physics", 95000],
          ["32343", "El Said", "History", 60000],
          ["33456", "Gold", "Physics", 87000],
          ["45565", "Katz", "Comp. Sci.", 75000],
          ["58583", "Califieri", "History", 62000],
          ["76543", "Singh", "Finance", 80000],
          ["76766", "Crick", "Biology", 72000],
          ["83821", "Brandt", "Comp. Sci.", 92000],
          ["98345", "Kim", "Elec. Eng.", 80000],
          ["98988", "Tanaka", "Biology", 30000],
        ];
        ensureDeeplyEqual(resultAsArray, expected);
      },
      MAX_TESTING_TIME_IN_MS
    );
  });
  describe.skip("INSERT ", () => {
    //it is only used for one relation.
    afterEach(async () => {
      await utils.clearDbAsync(connection);
    }, MAX_SETUP_TIME_IN_MS);

    it(
      "When the columns are not specified.",
      async () => {
        //course references dept_name.
        await utils.createTablesAndLoadThemWithData(connection, [
          "department",
          "course",
        ]);
        //the value to be inserted must follow the order in which the
        //columns appear in the schema.
        const insert = `insert into course
                      values ('CS-437', 'Database Systems', 'Comp. Sci.', 4);`;
        await utils.runNonParametricQueryAsync(connection, insert);

        const retrieved = "SELECT * FROM course";

        const result = await utils.runNonParametricQueryAsync(
          connection,
          retrieved
        );

        const resultAsArray = result.map((elem) => {
          const { course_id, title, dept_name, credits } = elem;
          return [course_id, title, dept_name, credits];
        });

        //com 437 is added to the result.
        const expected = [
          ["BIO-101", "Intro. to Biology", "Biology", 4],
          ["BIO-301", "Genetics", "Biology", 4],
          ["BIO-399", "Computational Biology", "Biology", 3],
          ["CS-101", "Intro. to Computer Science", "Comp. Sci.", 4],
          ["CS-190", "Game Design", "Comp. Sci.", 4],
          ["CS-315", "Robotics", "Comp. Sci.", 3],
          ["CS-319", "Image Processing", "Comp. Sci.", 3],
          ["CS-347", "Database System Concepts", "Comp. Sci.", 3],
          ["CS-437", "Database Systems", "Comp. Sci.", 4],
          ["EE-181", "Intro. to Digital Systems", "Elec. Eng.", 3],
          ["FIN-201", "Investment Banking", "Finance", 3],
          ["HIS-351", "World History", "History", 3],
          ["MU-199", "Music Video Production", "Music", 3],
          ["PHY-101", "Physical Principles", "Physics", 4],
        ];
        ensureDeeplyEqual(resultAsArray, expected);
      },
      MAX_TESTING_TIME_IN_MS
    );

    it(
      "When the colums are specified.",
      async () => {
        //course references dept_name.
        await utils.createTablesAndLoadThemWithData(connection, [
          "department",
          "course",
        ]);
        //in this case, the order of columns in the schema does not matter.
        const insert = `insert into course (title, course_id, credits, dept_name)
                              values ('Database Systems', 'CS-437', 4, 'Comp. Sci.');`;
        await utils.runNonParametricQueryAsync(connection, insert);

        const retrieved = "SELECT * FROM course";

        const result = await utils.runNonParametricQueryAsync(
          connection,
          retrieved
        );

        const resultAsArray = result.map((elem) => {
          const { course_id, title, dept_name, credits } = elem;
          return [course_id, title, dept_name, credits];
        });

        //com 437 is added to the result.
        const expected = [
          ["BIO-101", "Intro. to Biology", "Biology", 4],
          ["BIO-301", "Genetics", "Biology", 4],
          ["BIO-399", "Computational Biology", "Biology", 3],
          ["CS-101", "Intro. to Computer Science", "Comp. Sci.", 4],
          ["CS-190", "Game Design", "Comp. Sci.", 4],
          ["CS-315", "Robotics", "Comp. Sci.", 3],
          ["CS-319", "Image Processing", "Comp. Sci.", 3],
          ["CS-347", "Database System Concepts", "Comp. Sci.", 3],
          ["CS-437", "Database Systems", "Comp. Sci.", 4],
          ["EE-181", "Intro. to Digital Systems", "Elec. Eng.", 3],
          ["FIN-201", "Investment Banking", "Finance", 3],
          ["HIS-351", "World History", "History", 3],
          ["MU-199", "Music Video Production", "Music", 3],
          ["PHY-101", "Physical Principles", "Physics", 4],
        ];
        ensureDeeplyEqual(resultAsArray, expected);
      },
      MAX_TESTING_TIME_IN_MS
    );
    it.only(
      "insertion on the basis of the result of a query",
      async () => {
        //course references dept_name.
        await utils.createTablesAndLoadThemWithData(connection, [
          "department",
          "instructor",
          "student",
        ]);
        //the result of the query will be inserted into the instructor relation

        //query: make each student in the Music department who has earned
        // more than 110 credit hours an instructor in the Biology department with a salary of
        // $30,000

        //18000 is added to each tuple in the result of the query
        const insert = `insert into instructor
                          select ID, name, dept_name, 30000
                          from student
                          where dept_name = 'Biology' and tot_cred > 110;`;

        await utils.runNonParametricQueryAsync(connection, insert);

        const retrieved = "SELECT * FROM instructor";

        const result = await utils.runNonParametricQueryAsync(
          connection,
          retrieved
        );
        const resultAsArray = result.map((elem) => {
          const { ID, name, dept_name, salary } = elem;
          return [ID, name, dept_name, salary];
        });
        console.log(resultAsArray);

        //Tanaka has 120 points in the biology department, she is added to the  instructor relation
        //given a salary  of $40,000.
        const expected = [
          ["10101", "Srinivasan", "Comp. Sci.", 65000],
          ["12121", "Wu", "Finance", 90000],
          ["15151", "Mozart", "Music", 40000],
          ["22222", "Einstein", "Physics", 95000],
          ["32343", "El Said", "History", 60000],
          ["33456", "Gold", "Physics", 87000],
          ["45565", "Katz", "Comp. Sci.", 75000],
          ["58583", "Califieri", "History", 62000],
          ["76543", "Singh", "Finance", 80000],
          ["76766", "Crick", "Biology", 72000],
          ["83821", "Brandt", "Comp. Sci.", 92000],
          ["98345", "Kim", "Elec. Eng.", 80000],
          ["98988", "Tanaka", "Biology", 30000],
        ];
        ensureDeeplyEqual(resultAsArray, expected);
      },
      MAX_TESTING_TIME_IN_MS
    );
  });
});
