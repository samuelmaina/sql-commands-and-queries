const utils = require('./utils');

const { university } = require('./schemas');
const data = require('./data');
const { globalCb, cbWithDone, createConnectionWithDone,
    createConnectionAsync, createTableAsync, deleteTableAsync } = utils
describe('Operation on populated relations', () => {
    let connection
    beforeAll(async () => {
        connection = await createConnectionAsync();
        const schemas = Object.values(university);
        for (const schema of schemas) {
            console.log(schema);
            await createTableAsync(connection, schema);
        }

    });
    afterAll(async () => {
        const schemaInReverse = Object.values(university).reverse();
        for (const schema of schemaInReverse) {
            await deleteTableAsync(connection, schema.name);

        }
        connection.end();
    });
    it('should just pass', () => {
        expect(1).toBe(1);
    });
    function createTableWithDone(table, done) {
        utils.createTableWithDone(connection, table, done)
    }
    function deleteTableWithDone(name, done) {
        utils.deleteTableWithDone(connection, name, done)
    }

    function createTable(conf, cb) {
        utils.createTable(connection, conf, cb);
    }
    function dropTable(tableName, cb) {
        utils.dropTable(connection, tableName, cb)
    }
});