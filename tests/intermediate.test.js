const utils = require("./utils");

const { ensureEqual, ensureDeeplyEqual } = require("./testUtils");

const { createConnectionAsync, createTableAsync } = utils;

const MAX_SETUP_TIME_IN_MS = 100000;
const MAX_TESTING_TIME_IN_MS = 30000;

describe("Intermediate SQL queries", () => {
  let connection;

  beforeAll(async () => {
    connection = await createConnectionAsync();
  }, MAX_SETUP_TIME_IN_MS);

  afterAll((done) => {
    connection.end(utils.cbWithDone(done));
  }, MAX_SETUP_TIME_IN_MS);

  describe("Different JOINS", () => {
    beforeAll(async () => {
      await utils.createTablesAndLoadThemWithData(connection);
    }, MAX_SETUP_TIME_IN_MS);

    afterAll(async () => {
      await utils.clearDbAsync(connection);
    }, MAX_SETUP_TIME_IN_MS);

    describe("Natural Join", () => {
      //it will perform a catesian product of the two relations  and only
      //take those tuples with the same fields for all the common  columns
      //for all the relations in the join operation.
      //all the common columns are presented as one column in the resultant relation.
      //the result of natural join is a relation which can be placed and treated as
      //any other relation.

      it(
        "usage",
        async () => {
          const select = `select *
                            from student natural join takes;`;
          const result = await utils.runNonParametricQueryAsync(
            connection,
            select
          );

          const resultAsArray = result.map((tuple) => {
            const {
              ID,
              name,
              dept_name,
              tot_cred,
              course_id,
              sec_id,
              semester,
              year,
              grade,
            } = tuple;
            return [
              ID,
              name,
              dept_name,
              tot_cred,
              course_id,
              sec_id,
              semester,
              year,
              grade,
            ];
          });

          const expected = [
            [
              "00128",
              "Zhang",
              "Comp. Sci.",
              102,
              "CS-101",
              "1",
              "Fall",
              2017,
              "A",
            ],
            [
              "00128",
              "Zhang",
              "Comp. Sci.",
              102,
              "CS-347",
              "1",
              "Fall",
              2017,
              "A-",
            ],
            [
              "12345",
              "Shankar",
              "Comp. Sci.",
              32,
              "CS-101",
              "1",
              "Fall",
              2017,
              "C",
            ],
            [
              "12345",
              "Shankar",
              "Comp. Sci.",
              32,
              "CS-190",
              "2",
              "Spring",
              2017,
              "A",
            ],
            [
              "12345",
              "Shankar",
              "Comp. Sci.",
              32,
              "CS-315",
              "1",
              "Spring",
              2018,
              "A",
            ],
            [
              "12345",
              "Shankar",
              "Comp. Sci.",
              32,
              "CS-347",
              "1",
              "Fall",
              2017,
              "A",
            ],
            [
              "19991",
              "Brandt",
              "History",
              80,
              "HIS-351",
              "1",
              "Spring",
              2018,
              "B",
            ],
            [
              "23121",
              "Chavez",
              "Finance",
              110,
              "FIN-201",
              "1",
              "Spring",
              2018,
              "C+",
            ],
            [
              "44553",
              "Peltier",
              "Physics",
              56,
              "PHY-101",
              "1",
              "Fall",
              2017,
              "B-",
            ],
            ["45678", "Levy", "Physics", 46, "CS-101", "1", "Fall", 2017, "F"],
            [
              "45678",
              "Levy",
              "Physics",
              46,
              "CS-101",
              "1",
              "Spring",
              2018,
              "B+",
            ],
            [
              "45678",
              "Levy",
              "Physics",
              46,
              "CS-319",
              "1",
              "Spring",
              2018,
              "B",
            ],
            [
              "54321",
              "Williams",
              "Comp. Sci.",
              54,
              "CS-101",
              "1",
              "Fall",
              2017,
              "A-",
            ],
            [
              "54321",
              "Williams",
              "Comp. Sci.",
              54,
              "CS-190",
              "2",
              "Spring",
              2017,
              "B+",
            ],
            [
              "55739",
              "Sanchez",
              "Music",
              38,
              "MU-199",
              "1",
              "Spring",
              2018,
              "A-",
            ],
            [
              "76543",
              "Brown",
              "Comp. Sci.",
              58,
              "CS-101",
              "1",
              "Fall",
              2017,
              "A",
            ],
            [
              "76543",
              "Brown",
              "Comp. Sci.",
              58,
              "CS-319",
              "2",
              "Spring",
              2018,
              "A",
            ],
            [
              "76653",
              "Aoi",
              "Elec. Eng.",
              60,
              "EE-181",
              "1",
              "Spring",
              2017,
              "C",
            ],
            [
              "98765",
              "Bourikas",
              "Elec. Eng.",
              98,
              "CS-101",
              "1",
              "Fall",
              2017,
              "C-",
            ],
            [
              "98765",
              "Bourikas",
              "Elec. Eng.",
              98,
              "CS-315",
              "1",
              "Spring",
              2018,
              "B",
            ],
            [
              "98988",
              "Tanaka",
              "Biology",
              120,
              "BIO-101",
              "1",
              "Summer",
              2017,
              "A",
            ],
            [
              "98988",
              "Tanaka",
              "Biology",
              120,
              "BIO-301",
              "1",
              "Summer",
              2018,
              null,
            ],
          ];

          ensureDeeplyEqual(resultAsArray, expected);
        },
        MAX_TESTING_TIME_IN_MS
      );

      it(
        "more usage",
        async () => {
          //the result of the natural join are used as a relation to the
          //the cartesian operation of the from clause.
          //in this

          //query:List the names of students along with
          // the titles of courses that they have taken.
          const select = `select name, title
                            from student natural join takes, course
                            where takes.course_id = course.course_id;`;
          const result = await utils.runNonParametricQueryAsync(
            connection,
            select
          );

          const resultAsArray = result.map((tuple) => {
            const { name, title } = tuple;
            return [name, title];
          });

          const expected = [
            ["Zhang", "Intro. to Computer Science"],
            ["Zhang", "Database System Concepts"],
            ["Shankar", "Intro. to Computer Science"],
            ["Shankar", "Game Design"],
            ["Shankar", "Robotics"],
            ["Shankar", "Database System Concepts"],
            ["Brandt", "World History"],
            ["Chavez", "Investment Banking"],
            ["Peltier", "Physical Principles"],
            ["Levy", "Intro. to Computer Science"],
            ["Levy", "Intro. to Computer Science"],
            ["Levy", "Image Processing"],
            ["Williams", "Intro. to Computer Science"],
            ["Williams", "Game Design"],
            ["Sanchez", "Music Video Production"],
            ["Brown", "Intro. to Computer Science"],
            ["Brown", "Image Processing"],
            ["Aoi", "Intro. to Digital Systems"],
            ["Bourikas", "Intro. to Computer Science"],
            ["Bourikas", "Robotics"],
            ["Tanaka", "Intro. to Biology"],
            ["Tanaka", "Genetics"],
          ];

          ensureDeeplyEqual(resultAsArray, expected);
        },
        MAX_TESTING_TIME_IN_MS
      );
      it.only(
        "using the USING clause",
        async () => {
          //the result of the natural join are used as a relation to the
          //the cartesian operation of the from clause.

          //query:List the names of students along with
          // the titles of courses that they have taken.
          const select = `select name, title
                            from student natural join takes, course
                            where takes.course_id = course.course_id;`;
          const result = await utils.runNonParametricQueryAsync(
            connection,
            select
          );

          const resultAsArray = result.map((tuple) => {
            const { name, title } = tuple;
            return [name, title];
          });

          const expected = [
            ["Zhang", "Intro. to Computer Science"],
            ["Zhang", "Database System Concepts"],
            ["Shankar", "Intro. to Computer Science"],
            ["Shankar", "Game Design"],
            ["Shankar", "Robotics"],
            ["Shankar", "Database System Concepts"],
            ["Brandt", "World History"],
            ["Chavez", "Investment Banking"],
            ["Peltier", "Physical Principles"],
            ["Levy", "Intro. to Computer Science"],
            ["Levy", "Intro. to Computer Science"],
            ["Levy", "Image Processing"],
            ["Williams", "Intro. to Computer Science"],
            ["Williams", "Game Design"],
            ["Sanchez", "Music Video Production"],
            ["Brown", "Intro. to Computer Science"],
            ["Brown", "Image Processing"],
            ["Aoi", "Intro. to Digital Systems"],
            ["Bourikas", "Intro. to Computer Science"],
            ["Bourikas", "Robotics"],
            ["Tanaka", "Intro. to Biology"],
            ["Tanaka", "Genetics"],
          ];

          ensureDeeplyEqual(resultAsArray, expected);
        },
        MAX_TESTING_TIME_IN_MS
      );
    });
  });
});
