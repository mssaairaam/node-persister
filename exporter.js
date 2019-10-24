
const { performance } = require('perf_hooks');
const { Pool } = require('pg');
const copyTo = require('pg-copy-streams').to;
const fs = require('fs');
const path = require('path');
path.basename(path.resolve(process.cwd()));
const datasourcesConfigFilePath = path.join('database_configuration.json');
const datasources = JSON.parse(fs.readFileSync(datasourcesConfigFilePath, 'utf8'));

const pool = new Pool({
    user: datasources.user,
    host: datasources.host,
    database: datasources.database,
    password: datasources.password,
    port: datasources.port,
});

// create temporary table
const createTemporaryTable = () => {
    pool.connect().then(client => {
        const createQuery = "CREATE TEMPORARY TABLE temp_psv_table AS SELECT t.col_1, t.col_2, t.col_3 FROM perst_bulk_table t";
        client.query(createQuery, (err, res) => {
            if (err) {
                console.log(err.stack);
            } else {
                console.log(res);

                // do bulk export
                bulkExport(client);
            }
        });
    });
}

// bulk export
const bulkExport = (client) => {

    var startTime = performance.now();

    let done = () => {
        client.release();
        console.log("Bulk Export Task End.");

        var endTime = performance.now();

        console.log("Time taken to process is " + (endTime - startTime) + " milliseconds.");
    }
    var stream = client.query(copyTo('COPY temp_psv_table TO STDOUT DELIMITER \'|\' '));

    var fileStream = fs.createWriteStream('table_data_output.txt');
    let onError = strErr => {
        console.error('Something went wrong:', strErr);
        done();
    };
    fileStream.on('error', onError);

    stream.on('error', onError);
    stream.on('end', done);

    // do bulk export
    stream.pipe(fileStream);
}

// call to create temporary table
createTemporaryTable();