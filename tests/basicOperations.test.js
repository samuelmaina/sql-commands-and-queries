const utils = require("./utils");
const payload = require("./data");
const { ensureEqual, ensureDeeplyEqual } = require("./testUtils");
const { createConnectionAsync, createTableAsync } = utils;

const MAX_SETUP_TIME_IN_MS = 30000;
const MAX_TESTING_TIME_IN_MS = 20000;
describe.skip("Basic operations of sql", () => {
  let connection;
  beforeAll(async () => {
    connection = await createConnectionAsync();
  });

  afterAll(async () => {
    await utils.clearDbAsync(connection);
    connection.end();
  }, MAX_SETUP_TIME_IN_MS);
  describe("Select", () => {
    beforeAll(async () => {
      await utils.createTablesAndLoadThemWithData(connection);
    }, MAX_SETUP_TIME_IN_MS);
    it("simple select", async () => {
      const select = `SELECT name FROM instructor`;
      const result = await utils.runNonParametricQueryAsync(connection, select);

      const resultAsArray = result.map((elem) => {
        return elem.name;
      });
      const expected = [
        "Srinivasan",
        "Wu",
        "Mozart",
        "Einstein",
        "El Said",
        "Gold",
        "Katz",
        "Califieri",
        "Singh",
        "Crick",
        "Brandt",
        "Kim",
      ];
      ensureDeeplyEqual(expected, resultAsArray);
    });
    describe("Select with or without duplicates", () => {
      it("with duplicates", async () => {
        //the "SELECT A"  works the same as "SELECT ALL A"
        const select = `SELECT ALL dept_name
									   FROM instructor`;
        const result = await utils.runNonParametricQueryAsync(
          connection,
          select
        );

        const resultAsArray = result.map((elem) => {
          return elem.dept_name;
        });

        //mySQL will sort the data in ascending order by difault for foreign
        //field when querying. This may or may not be the case for other sql dbs.
        const expected = [
          "Biology",
          "Comp. Sci.",
          "Comp. Sci.",
          "Comp. Sci.",
          "Elec. Eng.",
          "Finance",
          "Finance",
          "History",
          "History",
          "Music",
          "Physics",
          "Physics",
        ];
        ensureDeeplyEqual(expected, resultAsArray);
      });
      it("without duplicates", async () => {
        //the same query as above but duplication is removed from the results.
        const select = `SELECT DISTINCT dept_name
									   FROM instructor`;
        const result = await utils.runNonParametricQueryAsync(
          connection,
          select
        );

        const resultAsArray = result.map((elem) => {
          return elem.dept_name;
        });

        const expected = [
          "Biology",
          "Comp. Sci.",
          "Elec. Eng.",
          "Finance",
          "History",
          "Music",
          "Physics",
        ];
        ensureDeeplyEqual(expected, resultAsArray);
      });
    });

    describe("select combining tables and filtering return results", () => {
      it("the behaviour of the from clause", async () => {
        //the from clause creates a catersian product of the two relation. The
        //producted relation as the same cardinality as the product of cardinalities
        //of the individual relations.

        const select = `SELECT *
								FROM instructor, department;`;

        //can be expressed as 'FROM instructor CROSS JOIN department'
        const result = await utils.runNonParametricQueryAsync(
          connection,
          select
        );
        const { instructor, department } = payload;
        //creates a cartesian product for the tuples(records) in each relation.
        //according to math the cardinarity of a cartesian product is the product of the cardinarity of  each relation(set).
        ensureEqual(result.length, instructor.length * department.length);
      });
      it("from  together with where  for one table", async () => {
        //sql allows connectives NOT, OR and AND  and the other logical operators in the
        //WHERE clause(<,<=, >, >=, <> and =)
        //from combines two or more table that will be used for querying of data.
        const select = `SELECT name
									FROM instructor
									WHERE dept_name = 'Comp. Sci.' AND salary > 70000;`;
        const result = await utils.runNonParametricQueryAsync(
          connection,
          select
        );
        const resultAsArray = result.map((elem) => {
          return elem.name;
        });
        const expected = ["Katz", "Brandt"];
        ensureDeeplyEqual(resultAsArray, expected);
      });
      it("from  together with where  for multiple tables", async () => {
        //the from clause creates
        const select = `SELECT name, instructor.dept_name, building
									FROM instructor, department
									WHERE instructor.dept_name= department.dept_name;`;
        const result = await utils.runNonParametricQueryAsync(
          connection,
          select
        );
        const resultAsArray = result.map((elem) => {
          const { name, dept_name, building } = elem;
          return [name, dept_name, building];
        });
        //mySQL will sort the data in ascending order by difault for foreign(dept_name)
        //field when querying. This may or may not be the case for other sql dbs.
        const expected = [
          ["Crick", "Biology", "Watson"],
          ["Srinivasan", "Comp. Sci.", "Taylor"],
          ["Katz", "Comp. Sci.", "Taylor"],
          ["Brandt", "Comp. Sci.", "Taylor"],
          ["Kim", "Elec. Eng.", "Taylor"],
          ["Wu", "Finance", "Painter"],
          ["Singh", "Finance", "Painter"],
          ["El Said", "History", "Painter"],
          ["Califieri", "History", "Painter"],
          ["Mozart", "Music", "Packard"],
          ["Einstein", "Physics", "Watson"],
          ["Gold", "Physics", "Watson"],
        ];
        ensureDeeplyEqual(resultAsArray, expected);
      });
      it("use of BETWEEN in the WHERE clause", async () => {
        //BETWEEN must be used with AND
        // NOT BETWEEN can be used to return out of range.
        const select = `SELECT name
									FROM instructor
									WHERE salary BETWEEN 90000 AND 100000;`;
        const result = await utils.runNonParametricQueryAsync(
          connection,
          select
        );
        const resultAsArray = result.map((elem) => {
          return elem.name;
        });
        const expected = ["Wu", "Einstein", "Brandt"];
        ensureDeeplyEqual(resultAsArray, expected);
      });
      describe("the UNION and INTERSECT operations", () => {
        it("UNION", async () => {
          //UNION removes duplicates. ALL is used to allow for  duplication.
          //the result are not sorted by default.
          const select = `(SELECT course_id
									FROM section
									WHERE semester = 'Fall' AND year= 2017)
									UNION
									(SELECT course_id
									FROM section
									WHERE semester = 'Spring' AND year= 2018);`;
          const result = await utils.runNonParametricQueryAsync(
            connection,
            select
          );
          const resultAsArray = result.map((elem) => {
            return elem.course_id;
          });

          const expected = [
            "CS-101",
            "CS-347",
            "PHY-101",
            "CS-315",
            "CS-319",
            "FIN-201",
            "HIS-351",
            "MU-199",
          ];
          ensureDeeplyEqual(resultAsArray, expected);
        });
        it("INTERSECT", async () => {
          //removes duplicates.
          //does not sort.
          const select = `(SELECT course_id
									FROM section
									WHERE semester = 'Fall' AND year= 2017)
									INTERSECT
									(SELECT course_id
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
      });
    });

    describe("Aggregate and arithemtic operations on the select clause", () => {
      it("operations like multiplication", async () => {
        //rename the new salary, otherwise the db may give  it an awkward name.
        const incrementedSelect = `SELECT ID,name, dept_name,salary *1.1 as salary
									   	   FROM instructor`;
        const incremented = await utils.runNonParametricQueryAsync(
          connection,
          incrementedSelect
        );

        const normalSelect = `SELECT *
								FROM instructor`;
        const previous = await utils.runNonParametricQueryAsync(
          connection,
          normalSelect
        );

        previous.forEach((record, index) => {
          ensureEqual(
            Number((record.salary * 1.1).toFixed(0)),
            incremented[index].salary
          );
        });
      });
      describe("Aggregates", () => {
        it("basic operations", async () => {
          //there are others like min, max, sum, count. DISTINCT
          //can be used to removed duplication before the the aggregate
          //is called. e.g SELECT COUNT(DISTINCT ID)
          const avgSalary = `SELECT AVG(salary) AS avg_salary
									   FROM instructor
									   WHERE dept_name='Comp. Sci.'`;
          const result = await utils.runNonParametricQueryAsync(
            connection,
            avgSalary
          );
          ensureEqual(Number(result[0].avg_salary.toFixed(2)), 77333.33);
        });
        describe("with grouping", () => {
          it("grouping for one attribute", async () => {
            //there are others like min, max, sum, count. DISTINCT
            //can be used to removed duplication before the the aggregate
            //is called. e.g SELECT COUNT(DISTINCT ID).
            //the artributes to be choosen  are those that are
            //in the GROUP BY clause. The group creates groups with only the
            //the given attribute hence only those attributes in the group
            //clause can be selected in the query.
            const avgSalary = `SELECT dept_name, AVG(salary) AS avg_salary
									   	   FROM instructor
									   	   GROUP BY dept_name`;
            const result = await utils.runNonParametricQueryAsync(
              connection,
              avgSalary
            );
            const resultAsArray = result.map((elem) => {
              return Object.values(elem);
            });
            const expected = [
              ["Biology", 72000],
              ["Comp. Sci.", 77333.333333],
              ["Elec. Eng.", 80000],
              ["Finance", 85000],
              ["History", 61000],
              ["Music", 40000],
              ["Physics", 91000],
            ];
            ensureDeeplyEqual(resultAsArray, expected);
          });

          it("the HAVING clause", async () => {
            //It is used to filter result from a group . It acts as the WHERE clause
            // in the FROM clause.
            const avgSalary = `SELECT dept_name, AVG (salary) AS avg_salary
										   FROM instructor
										   GROUP BY  dept_name
										   HAVING AVG (salary) > 42000;`;
            const result = await utils.runNonParametricQueryAsync(
              connection,
              avgSalary
            );
            const resultAsArray = result.map((elem) => {
              return Object.values(elem);
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
          });

          it("grouping for with many attributes", async () => {
            // “For each course section offered in 2017, find the average total
            // credits (tot cred) of all students enrolled in the section, if the section has at least 2
            // students.”
            const avgSalary = `SELECT course_id, semester, year, sec_id, AVG (tot_cred)
										   FROM student, takes
										   WHERE student.ID= takes.ID AND year = 2017
										   GROUP BY course_id, semester, year, sec_id
										   HAVING COUNT (.ID) >= 2;`;
            const result = await utils.runNonParametricQueryAsync(
              connection,
              avgSalary
            );
            const resultAsArray = result.map((elem) => {
              return Object.values(elem);
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
          });
        });
      });
    });

    describe("renaming", () => {
      it("using AS ", async () => {
        //AS used in the from clause is used to reduce names of long relations.
        const select = `SELECT name, D.dept_name AS dept_name, building
									FROM instructor AS I , department as D
									WHERE I.dept_name= D.dept_name;`;
        const result = await utils.runNonParametricQueryAsync(
          connection,
          select
        );
        const resultAsArray = result.map((elem) => {
          const { name, dept_name, building } = elem;
          return [name, dept_name, building];
        });
        const expected = [
          ["Crick", "Biology", "Watson"],
          ["Srinivasan", "Comp. Sci.", "Taylor"],
          ["Katz", "Comp. Sci.", "Taylor"],
          ["Brandt", "Comp. Sci.", "Taylor"],
          ["Kim", "Elec. Eng.", "Taylor"],
          ["Wu", "Finance", "Painter"],
          ["Singh", "Finance", "Painter"],
          ["El Said", "History", "Painter"],
          ["Califieri", "History", "Painter"],
          ["Mozart", "Music", "Packard"],
          ["Einstein", "Physics", "Watson"],
          ["Gold", "Physics", "Watson"],
        ];
        ensureDeeplyEqual(resultAsArray, expected);
      });
    });

    it("ordering the results", async () => {
      //first order with salary in descending order
      //if there are records with the same salaries,
      //order them with the name in ASC order.
      //if DESC is left out the default is ASC,
      const select = `SELECT *
								FROM instructor
								ORDER BY salary DESC, name ASC;`;
      const result = await utils.runNonParametricQueryAsync(connection, select);
      const resultAsArray = result.map((elem) => {
        const { ID, name, dept_name, salary } = elem;
        return [ID, name, dept_name, salary];
      });
      const expected = [
        ["22222", "Einstein", "Physics", 95000],
        ["83821", "Brandt", "Comp. Sci.", 92000],
        ["12121", "Wu", "Finance", 90000],
        ["33456", "Gold", "Physics", 87000],
        ["98345", "Kim", "Elec. Eng.", 80000],
        ["76543", "Singh", "Finance", 80000],
        ["45565", "Katz", "Comp. Sci.", 75000],
        ["76766", "Crick", "Biology", 72000],
        ["10101", "Srinivasan", "Comp. Sci.", 65000],
        ["58583", "Califieri", "History", 62000],
        ["32343", "El Said", "History", 60000],
        ["15151", "Mozart", "Music", 40000],
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
});
