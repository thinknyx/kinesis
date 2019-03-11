'use strict';
console.log('Loading function');

exports.handler = (event, context, callback) => {

    const output = event.records.map(function (record) {
        console.log('Processing record: ' + JSON.stringify(record));
        var dataStr = new Buffer(record.data, 'base64').toString('ascii')

        return {
            recordId: record.recordId,
            result: 'Ok',
            data: new Buffer(dataStr + "\n").toString('base64')
        }
    });
    console.log('Result output: ' + JSON.stringify(output));
    console.log(`Processing completed.  Successful records ${output.length}.`);
    callback(null, { records: output });
};
