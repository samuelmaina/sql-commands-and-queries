const utils = require("./utils");

const { ensureEqual, ensureDeeplyEqual } = require("./testUtils");

const { createConnectionAsync, createTableAsync } = utils;

const MAX_SETUP_TIME_IN_MS = 100000;
const MAX_TESTING_TIME_IN_MS = 30000;

describe.skip("Intermediate SQL queries", () => {
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

      //the select for natural join takes the format
      // select A1, A2,…, An
      //    from r1 natural join r2 natural join . . . natural join rm
      //    where P;

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
      it(
        "using the USING clause",
        async () => {
          //the using clause is used to specify
          //the common column that the natural join table should have
          //values in common.

          // The operation join … using requires a list of attribute names to be specified. Both
          // relations being joined must have attributes with the specified names. Consider the operation
          // r1 join r2 using(A1, A2). The operation is similar to r1 natural join r2, except that
          // a pair of tuples t1 from r1 and t2 from r2 match if t1.A1 = t2.A1 and t1.A2 = t2.A2; even
          // if r1 and r2 both have an attribute named A3, it is not required that t1.A3 = t2.A3.
          const select = `select name, title
                          from (student natural join takes) join course using (course_id);`;
          const result = await utils.runNonParametricQueryAsync(
            connection,
            select
          );

          const resultAsArray = result.map((tuple) => {
            const { name, title } = tuple;
            return [name, title];
          });

          ensureDeeplyEqual(resultAsArray, expected);
        },
        MAX_TESTING_TIME_IN_MS
      );
    });
    describe("JOIN using ON clause", () => {
      it(
        "using the ON clause",
        async () => {
          //this is type of join where a predicate like the one that is used
          //in the WHERE clause  of the From operation.
          const select = `select student.ID,name,dept_name
                          from student join takes on student.ID = takes.ID;`;
          const result = await utils.runNonParametricQueryAsync(
            connection,
            select
          );

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

    describe("Outer joins", () => {
      //these operations allow  for those tuples that don't have
      //any matching values for common column to be preserved in the join operation.
      //the values that are not there are replaced by a null value  in the relation.
      describe("left outer join", () => {
        // The left outer join preserves tuples only in the relation named before (to the left
        //   of) the left outer join operation.
        //ensures that the values of the left relation is preserved for any non matching record for the common
        //columns being prepared. The attributes that are not in the right  are filled with null in the resultant
        //relation.
        it(
          "usage",
          async () => {
            const select = `select *
                            from student natural left outer join takes
                            where student.name= 'Snow' `;
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

            // Snow is shown though he has not taken any course.
            const expected = [
              ["70557", "Snow", "Physics", 0, null, null, null, null, null],
            ];
            ensureDeeplyEqual(resultAsArray, expected);
          },
          MAX_TESTING_TIME_IN_MS
        );

        it(
          "more complex usage",
          async () => {
            //find students who have not taken any course.
            const select = `select ID
                          from student natural left outer join takes
                          where course_id is null;`;
            const result = await utils.runNonParametricQueryAsync(
              connection,
              select
            );

            const resultAsArray = result.map((tuple) => {
              return tuple.ID;
            });

            const expected = ["70557"];

            ensureDeeplyEqual(resultAsArray, expected);
          },
          MAX_TESTING_TIME_IN_MS
        );
      });

      describe("right outer join", () => {
        // The right outer join preserves tuples only in the relation named before (to the right
        //   of) the right outer join operation.
        //the atributes in the left relation are filled with nulls in the resultant relation

        it(
          "usage",
          async () => {
            //student Snow has not taken any course hence he should be present in the right join operation
            //
            const select = `select *
                             from takes natural right outer join student as right_join
                             where right_join.name="Snow"`;
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

            //snow is displayed but the right value for the left relation attributes
            //are filled with nulls.
            const expected = [
              ["70557", "Snow", "Physics", 0, null, null, null, null, null],
            ];

            ensureDeeplyEqual(resultAsArray, expected);
          },
          MAX_TESTING_TIME_IN_MS
        );

        it(
          "more complex usage",
          async () => {
            //find students who have not taken any course.
            const select = `select ID
                          from student natural left outer join takes
                          where course_id is null;`;
            const result = await utils.runNonParametricQueryAsync(
              connection,
              select
            );

            const resultAsArray = result.map((tuple) => {
              return tuple.ID;
            });

            const expected = ["70557"];

            ensureDeeplyEqual(resultAsArray, expected);
          },
          MAX_TESTING_TIME_IN_MS
        );
      });
      describe("full outer join", () => {
        //performs both right  and left outer join operations.
        //not supported by mySQL hence the query will throw an error.

        //equivalent for mySQL
        // SELECT * FROM t1
        // LEFT JOIN t2 ON t1.id = t2.id
        // UNION ALL
        // SELECT * FROM t1
        // RIGHT JOIN t2 ON t1.id = t2.id
        // WHERE t1.id IS NULL
        it.skip(
          "usage",
          async () => {
            // “Display
            // a list of all students in the Comp. Sci. department, along with the course sections, if
            // any, that they have taken in Spring 2017; all course sections from Spring 2017 must
            //be displayed, even if no student from the Comp. Sci. department has taken the course
            // section.”
            const select = `select *
                              from (select *
                                    from student
                                    where dept_name = 'Comp. Sci.') as comp_sci
                                   natural full outer join
                                    (select *
                                    from takes
                                    where semester = 'Spring' and year = 2017) as period;`;
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

            //snow is displayed but the right value for the left relation attributes
            //are filled with nulls.
            const expected = [
              ["70557", "Snow", "Physics", 0, null, null, null, null, null],
            ];

            ensureDeeplyEqual(resultAsArray, expected);
          },
          MAX_TESTING_TIME_IN_MS
        );
      });
      describe("Use of ON and WHERE in the outer join operations.", () => {
        //It is used to do a comparison when doing the outer join.
        //It is different from the where clause since the conditions is
        //checked when the cartesian product is being done whereas as WHERE is performed
        //after the performance of the cartesian product.
        it(
          "ON usage",
          async () => {
            const select = `select *
                            from student left outer join takes on  student.name='Snow';`;
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

            //snow is displayed but the right value for the left relation attributes
            //are filled with nulls.
            const expected = [
              ["70557", "Snow", "Physics", 0, null, null, null, null, null],
            ];

            ensureDeeplyEqual(resultAsArray, expected);
          },
          MAX_TESTING_TIME_IN_MS
        );
      });
    });
    describe("Views", () => {
      //views are created using the syntax create view v as <query expression>;
      it(
        "usage",
        async () => {
          const view_creater = `create view faculty as
                          select ID, name, dept_name
                          from instructor;`;
          await utils.runNonParametricQueryAsync(connection, view_creater);

          //the view is created and it can be called at any time.
          //a view is different from a WITH clause in that the WITH
          //clause can only be accessed in the query that it has been
          //created.
          const select = `select *
                          from faculty`;
          const result = await utils.runNonParametricQueryAsync(
            connection,
            select
          );

          const resultAsArray = result.map((tuple) => {
            const { ID, name, dept_name } = tuple;
            return [ID, name, dept_name];
          });
          const expected = [
            ["10101", "Srinivasan", "Comp. Sci."],
            ["12121", "Wu", "Finance"],
            ["15151", "Mozart", "Music"],
            ["22222", "Einstein", "Physics"],
            ["32343", "El Said", "History"],
            ["33456", "Gold", "Physics"],
            ["45565", "Katz", "Comp. Sci."],
            ["58583", "Califieri", "History"],
            ["76543", "Singh", "Finance"],
            ["76766", "Crick", "Biology"],
            ["83821", "Brandt", "Comp. Sci."],
            ["98345", "Kim", "Elec. Eng."],
          ];

          ensureDeeplyEqual(resultAsArray, expected);
        },
        MAX_TESTING_TIME_IN_MS
      );
      //views can appear anywhere in a query where a  relation can appear.
      it(
        "defination of the output parameters on the view defination",
        async () => {
          //the values of the sum(salary) is not explicitly stated in the query but it can
          //be specified in the defination of the view.
          const view_creater = `create view departments_total_salary(dept_name, total_salary) as
                                  select dept_name, sum (salary)
                                  from instructor
                                  group by dept_name;`;
          await utils.runNonParametricQueryAsync(connection, view_creater);

          const select = `select *
                          from departments_total_salary`;
          const result = await utils.runNonParametricQueryAsync(
            connection,
            select
          );

          const resultAsArray = result.map((tuple) => {
            const { dept_name, total_salary } = tuple;
            return [dept_name, total_salary];
          });

          const expected = [
            ["Biology", 72000],
            ["Comp. Sci.", 232000],
            ["Elec. Eng.", 80000],
            ["Finance", 170000],
            ["History", 122000],
            ["Music", 40000],
            ["Physics", 182000],
          ];

          ensureDeeplyEqual(resultAsArray, expected);
        },
        MAX_TESTING_TIME_IN_MS
      );
    });

    //A transaction is a sequence of query and update statements.
    //A transaction must end with either a rollback(causes the current transaction to be rolled back; that is, it undoes
    // all the updates performed by the SQL statements in the transaction. Thus, the
    // database state is restored to what it was before the first statement of the transaction
    // was executed.) or a commit(the updation are made permanent to the database.)
    //transactions are atomic.
    // Either all the effects of the transaction are reflected in the database or
    // none are (after rollback).
    describe("Transactions", () => {
      it(
        "usage",
        async () => {
          const transaction = ` begin atomic
                                        select course_id
                                        from course
                                      end.  `;
          const result = await utils.runNonParametricQueryAsync(
            connection,
            transaction
          );

          console.log(result);

          const resultAsArray = result.map((tuple) => {
            const { course_id, room_number } = tuple;
            return [course_id, room_number];
          });
          const expected = [["PHY-101", "100"]];

          ensureDeeplyEqual(resultAsArray, expected);

          //views can have their results stored in a database for faster querying. This
          //types of views are called materialized views. Materialized views can be updated
          //lazily(when the view is called ) or they can be updated periodically( after sometime.).
          //the method of updation of the materialized view is specified by the database system under
          //implementation.
          //Updates can be done to the database through views but the practise is generally discouraged since view
          //are mostly defined from many relations . Also views may not display all the data from a table hence
          //insertion would fail or padding with nulls for the unpresented attributes.
          //An SQL view is said to be updatable(i.e., inserts, updates, or deletes can
          //be applied on the view)
          //if it only has
          // 1. one relation in its from clause.
          // 2. The select clause contains only attribute names of the relation and does not have
          // any expressions, aggregates, or distinct specification.
          // 3. Any attribute not listed in the select clause can be set to null; that is, it does not
          // have a not null constraint and is not part of a primary key.
          // 4. The query does not have a group by or having clause.
        },
        MAX_TESTING_TIME_IN_MS
      );
    });

    describe("Constrains", () => {
      //these are constraints that are attributes.
      //1. NOT NULL specifies that the field can't not contain a null value. null can fit into any field of
      //   an SQL attribute.
      //2. UNIQUE (a1, a2, a3,..., aN) specifies that the attributes (a1, a2, a3,..., aN) form a superkey i.e
      //   no two tuples can be equal in all of the specified attributes. the attributes can be null.
      //3. CHECKS(P) ensures that the predicated P is satisfied. e.g. check (semester in ('Fall', 'Winter', 'Spring', 'Summer'))
      // constrains can also be assigned names such as salary numeric(8,2), constraint MINSALARY check (salary > 29000), in which
      //the constain can be dropped in  by the sql query alter table instructor drop constraint minsalary;

      //ASSERTION  creates  predicate that the database must always satisfy.
      it(
        "use of ASSERTION keyword for verification of complex checks",
        async () => {
          //takes the format of create assertion <assertion-name> check <predicate>;
          const transaction = ` create assertion credits_earned_constraint check
                                      (not exists (select ID
                                      from student
                                      where tot_cred <> (select coalesce(sum(credits), 0)
                                          from takes natural join course
                                          where student.ID= takes.ID
                                          and grade is not null and grade<> ’F’ ))) `;
          const result = await utils.runNonParametricQueryAsync(
            connection,
            transaction
          );

          console.log(result);

          const resultAsArray = result.map((tuple) => {
            const { course_id, room_number } = tuple;
            return [course_id, room_number];
          });
          const expected = [["PHY-101", "100"]];

          ensureDeeplyEqual(resultAsArray, expected);

          //views can have their results stored in a database for faster querying. This
          //types of views are called materialized views. Materialized views can be updated
          //lazily(when the view is called ) or they can be updated periodically( after sometime.).
          //the method of updation of the materialized view is specified by the database system under
          //implementation.
          //Updates can be done to the database through views but the practise is generally discouraged since view
          //are mostly defined from many relations . Also views may not display all the data from a table hence
          //insertion would fail or padding with nulls for the unpresented attributes.
          //An SQL view is said to be updatable(i.e., inserts, updates, or deletes can
          //be applied on the view)
          //if it only has
          // 1. one relation in its from clause.
          // 2. The select clause contains only attribute names of the relation and does not have
          // any expressions, aggregates, or distinct specification.
          // 3. Any attribute not listed in the select clause can be set to null; that is, it does not
          // have a not null constraint and is not part of a primary key.
          // 4. The query does not have a group by or having clause.
        },
        MAX_TESTING_TIME_IN_MS
      );
    });

    //Indices are very effecient in querying of the database, enforcing integrities such as those
    //of primary keys, foreign keys and querying of data.
    it(
      "creating of index in SQL",
      async () => {
        //takes the format of create index <index-name> on <relation-name> (<attribute-list>);
        //todos: Find a way to get the present indices in the database and confirm that the below index is
        //created.
        //For now the passing of the test is that it does not throw.
        const index = `create index dept_name_index on instructor (dept_name) `;
        await utils.runNonParametricQueryAsync(connection, index);
      },
      MAX_TESTING_TIME_IN_MS
    );

    //this involved granting preveleges to perform the CRUD operations in the databaase.
    //there are also other forms of priveleges that a person can be granted such as viewing of schema,
    //adding or dropping of database schemas , and creating or dropping schemas.
    describe("Authorization", () => {
      //this is giving preveleges to the database uses to perform some or all of the CRUD operation.
      describe("grant", () => {
        it.only(
          "usage",
          async () => {
            const index = `grant select on department to Sam; `;
            await utils.runNonParametricQueryAsync(connection, index);
          },
          MAX_TESTING_TIME_IN_MS
        );
      });
    });
  });
});
