
const { performance } = require('perf_hooks');
const copyFrom = require('pg-copy-streams').from;
const { Pool } = require('pg');
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

const bulkInsert = () => {
    pool.connect().then(client => {
        var startTime = performance.now();

        let done = () => {
            client.release();
            console.log("Bulk Insert Task End.");

            var endTime = performance.now();
    
            console.log("Time taken to process is " + (endTime - startTime) + " milliseconds.");
        }
        var stream = client.query(copyFrom('COPY PERST_BULK_TABLE FROM STDIN DELIMITER \'|\' '));

        var fileStream = fs.createReadStream('input_data.txt');
        let onError = strErr => {
            console.error('Something went wrong:', strErr);
            done();
        };
        fileStream.on('error', onError);

        stream.on('error', onError);
        stream.on('end', done);
        
        // do bulk insert
        fileStream.pipe(stream);
    });
}


// main function - call to do bulk insert
bulkInsert();

