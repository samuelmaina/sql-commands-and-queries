const utils = require("./utils");

const { ensureEqual, ensureDeeplyEqual } = require("./testUtils");

const { createConnectionAsync, createTableAsync } = utils;

const MAX_SETUP_TIME_IN_MS = 100000;
const MAX_TESTING_TIME_IN_MS = 30000;
describe("Advanced sql", () => {
  let connection;

  beforeAll(async () => {
    connection = await createConnectionAsync();
  }, MAX_SETUP_TIME_IN_MS);

  afterAll((done) => {
    connection.end(utils.cbWithDone(done));
  }, MAX_SETUP_TIME_IN_MS);

  describe("advanced SQL", () => {
    beforeAll(async () => {
      await utils.createTablesAndLoadThemWithData(connection);
    }, MAX_SETUP_TIME_IN_MS);

    afterAll(async () => {
      await utils.clearDbAsync(connection);
    }, MAX_SETUP_TIME_IN_MS);
    describe("functions", () => {
      it(
        "usage",
        async () => {
          //this is type of join where a predicate like the one that is used
          //in the WHERE clause  of the From operation.
          const func = `create function instructor_of (dept_name varchar(20))
                                   returns table (
                                    ID varchar (5),
                                    name varchar (20),
                                    dept_name varchar (20),
                                    salary numeric (8,2))
                                    return table
                                    (select ID, name, dept_name, salary
                                    from instructor
                                    where instructor.dept_name = instructor_of.dept_name);`;

          await utils.runNonParametricQueryAsync(connection, func);
          const select = `select *
                            from table(instructor_of ('Finance'));`;
          const result = await utils.runNonParametricQueryAsync(
            connection,
            select
          );
          console.log(result);
          const resultAsArray = result.map((tuple) => {
            const { ID, name, dept_name } = tuple;
            return [ID, name, dept_name];
          });

          const expected = [
            ["00128", "Zhang", "Comp. Sci."],
            ["00128", "Zhang", "Comp. Sci."],
            ["12345", "Shankar", "Comp. Sci."],
            ["12345", "Shankar", "Comp. Sci."],
            ["12345", "Shankar", "Comp. Sci."],
            ["12345", "Shankar", "Comp. Sci."],
            ["19991", "Brandt", "History"],
            ["23121", "Chavez", "Finance"],
            ["44553", "Peltier", "Physics"],
            ["45678", "Levy", "Physics"],
            ["45678", "Levy", "Physics"],
            ["45678", "Levy", "Physics"],
            ["54321", "Williams", "Comp. Sci."],
            ["54321", "Williams", "Comp. Sci."],
            ["55739", "Sanchez", "Music"],
            ["76543", "Brown", "Comp. Sci."],
            ["76543", "Brown", "Comp. Sci."],
            ["76653", "Aoi", "Elec. Eng."],
            ["98765", "Bourikas", "Elec. Eng."],
            ["98765", "Bourikas", "Elec. Eng."],
            ["98988", "Tanaka", "Biology"],
            ["98988", "Tanaka", "Biology"],
          ];

          ensureDeeplyEqual(resultAsArray, expected);
        },
        MAX_TESTING_TIME_IN_MS
      );
    });
    describe.only("procedure", () => {
      it(
        "usage",
        async () => {
          const proc = `create procedure dept_name_pro(in dept_name varchar(20),
          out d_count integer)
          begin
              select count(*) into d_count
              from instructor
              where instructor.dept_name= dept_name_pro.dept_name
          end `;

          await utils.runNonParametricQueryAsync(connection, proc);
          const invoke = `declare c_count integer;
                          call dept_count_proc('Physics', c_count);`;
          const result = await utils.runNonParametricQueryAsync(
            connection,
            invoke
          );
          console.log(result);
          const resultAsArray = result.map((tuple) => {
            const { ID, name, dept_name } = tuple;
            return [ID, name, dept_name];
          });

          const expected = [
            ["00128", "Zhang", "Comp. Sci."],
            ["00128", "Zhang", "Comp. Sci."],
            ["12345", "Shankar", "Comp. Sci."],
            ["12345", "Shankar", "Comp. Sci."],
            ["12345", "Shankar", "Comp. Sci."],
            ["12345", "Shankar", "Comp. Sci."],
            ["19991", "Brandt", "History"],
            ["23121", "Chavez", "Finance"],
            ["44553", "Peltier", "Physics"],
            ["45678", "Levy", "Physics"],
            ["45678", "Levy", "Physics"],
            ["45678", "Levy", "Physics"],
            ["54321", "Williams", "Comp. Sci."],
            ["54321", "Williams", "Comp. Sci."],
            ["55739", "Sanchez", "Music"],
            ["76543", "Brown", "Comp. Sci."],
            ["76543", "Brown", "Comp. Sci."],
            ["76653", "Aoi", "Elec. Eng."],
            ["98765", "Bourikas", "Elec. Eng."],
            ["98765", "Bourikas", "Elec. Eng."],
            ["98988", "Tanaka", "Biology"],
            ["98988", "Tanaka", "Biology"],
          ];

          ensureDeeplyEqual(resultAsArray, expected);
        },
        MAX_TESTING_TIME_IN_MS
      );
    });
  });
});
