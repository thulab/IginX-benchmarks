var express = require('express');
var readline = require('readline');
var router = express.Router();
var config = require('../../config.json');
var influent = require('influent');
var async = require('async');
var pg = require('pg');

var pool = new pg.Pool({
    connectionString: config.database.connectionString,
});

var http = require('http');

router.get('/:seriesKey/:seriesNumber', function (req, res, next) {

    // Get the data definition for the specified key
    getDataDefinition(req.params.seriesKey, function (dataDefinition) {
        if (dataDefinition === null) {
            res.send({});
            return;
        }

        console.log('parameters', req.params);

        var http = require('http');

        function post(action,send,callback){
            var options = {
                hostname: '127.0.0.1',
                port: 6666,
                path: action,
                method: 'POST',
                headers: {
                    'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8'/* ,
		'Content-Length': send.length */
                }
            };
            var req = http.request(options, function (res) {
                // console.log('STATUS: ' + res.statusCode);
                // console.log('HEADERS: ' + JSON.stringify(res.headers));
                // 定义了一个post变量，用于暂存请求体的信息
                var body="";
                res.setEncoding('utf8');
                // 通过res的data事件监听函数，每当接受到请求体的数据，就累加到post变量中
                res.on('data', function (chunk) {
                    // console.log('BODY: ' + chunk);
                    body += chunk;
                });
                // 在res的end事件触发后，通过JSON.parse将post解析为真正的POST请求格式，然后调用传递过来的回调函数处理数据
                res.on('end', function(){
                    // console.log("body = "+body);
                    var json = JSON.parse(body);
                    console.log(body);
                    callback(json);
                });
            });
            req.on('error', function (e) {
                console.log('problem with request: ' + e.message);
            });
            req.write(send);
            req.end();
        }

//调用post函数在nodejs里发送post请求
        post('/api/v1/datapoints/query',"{\n" +
            "\t\"start_absolute\" : 1,\n" +
            "\t\"end_absolute\" : 2,\n" +
            "\t\"time_zone\": \"Asia/Kabul\",\n" +
            "\t\"metrics\": [\n" +
            "\t{\n" +
            "\t\t\"name\": \"qetch\",\n" +
            "\t\t\"tags\": {\n" +
            "            \"seriesKey\": [\n" +
            "              \"" +  req.params.seriesKey+ "\"\n" +
            "            ],\n" +
            "            \n" +
            "            \"seriesNumber\": [\n" +
            "              \" "+ req.params.seriesNumber+"\"\n" +
            "            ]\n" +
            "          }\n" +
            "\t}]\n" +
            "}",function(json){

/*
            var time0, dataset = {
                seriesKey: req.params.seriesKey,
                seriesNumber: req.params.seriesNumber,
                values: result.rows
            };

            if (result.rowCount > 0) {
                console.log('relative time: ', dataDefinition.relativeTime);
                if (dataDefinition.relativeTime) time0 = dataset.values[0].time.getTime();
                for (var i in dataset.values) {
                    var time = dataset.values[i].time.getTime();
                    if (dataDefinition.relativeTime) time -= time0;
                    dataset.values[i] = {
                        x: time,
                        y: dataset.values[i].value
                    };
                }
            }

            // returns the model
            res.send(dataset);
            */
            console.log(json);
        });


        pool.connect(function (err, client, done) {
            if (err) {
                res.status(500).send(err);
                return;
            }

            client.query('select time, value from Measurement where sname = $1 and snum = $2 order by time',
                [req.params.seriesKey, req.params.seriesNumber], function (err, result) {
                    if (err) {
                        res.status(500).send(err);
                        return;
                    }

                    var time0, dataset = {
                        seriesKey: req.params.seriesKey,
                        seriesNumber: req.params.seriesNumber,
                        values: result.rows
                    };
                    console.log(result);

                    if (result.rowCount > 0) {
                        console.log('relative time: ', dataDefinition.relativeTime);
                        if (dataDefinition.relativeTime) time0 = dataset.values[0].time.getTime();
                        for (var i in dataset.values) {
                            var time = dataset.values[i].time.getTime();
                            if (dataDefinition.relativeTime) time -= time0;
                            dataset.values[i] = {
                                x: time,
                                y: dataset.values[i].value
                            };
                        }
                    }

                    console.log(dataset);
                    // returns the model
                    res.send(dataset);

                    done();
                });

        });
    })

});
/*
 */
router.delete('/series/:seriesKey/:seriesNumber', function (req, res, next) {
    pool.connect(function (err, client, done) {
        client.query('delete from measurement where sname = $1 and snum = $2',
            [req.params.seriesKey, req.params.seriesNumber],
            function (err, result) {
                if (err) {
                    res.status(500).send(err);
                    done();
                    return;
                }
                client.query('delete from measurementseriesdescription where measurementkey = $1 and measurementseries = $2',
                    [req.params.seriesKey, req.params.seriesNumber],
                    function (err, result) {
                        if (err) res.status(500).send(err);
                        else res.status(200).send('ok');
                        done();
                    });
            });
    });
});

router.delete('/seriestype/:seriesKey', function (req, res, next) {
    pool.connect(function (err, client, done) {
        client.query('delete from measurementdescription where key = $1',
            [req.params.seriesKey],
            function (err, result) {
                if (err) res.status(500).send(err);
                else res.status(200).send('ok');
                done();
            });
    });
});

router.post('/series', function (req, res, next) {
    res.send('ok');
});
router.post('/series/:seriesKey/:seriesNumber', function (req, res, next) {
    var data = req.body.datasetSeriesData.split('\n');
    pool.connect(function (err, client, done) {
        async.each(data, function (dataEntry, callback) {
            var dataEntryValues = dataEntry.split(',');
            if (dataEntryValues.length != 2) {
                callback({detail: 'the data are not correctly formatted'});
                return;
            }
            console.log('inserting', [req.params.seriesKey, req.params.seriesNumber, dataEntryValues[0], dataEntryValues[1]]);
            client.query('INSERT INTO measurement (sname, snum, time, value) VALUES ($1, $2, to_timestamp($3), $4)',
                [req.params.seriesKey, req.params.seriesNumber, dataEntryValues[0], dataEntryValues[1]],
                function (err, result) {
                    callback(err);
                });
        }, function (err) {
            if (err) {
                res.status(500).send(err.detail);
                return;
            }
            client.query('INSERT INTO measurementseriesdescription (measurementkey, measurementseries, description) VALUES ($1, $2, $3)',
                [req.params.seriesKey, req.params.seriesNumber, req.body.datasetSeriesDesc],
                function (err, result) {
                    if (err) res.status(500).send(err.detail); else res.send('ok');
                    done();
                });
        });
    });
});

router.post('/seriestype', function (req, res, next) {
    res.send('ok');
});
router.post('/seriestype/:seriesKey', function (req, res, next) {
    pool.connect(function (err, client, done) {
        client.query('INSERT INTO measurementdescription (key, description, relativetime, xaxisdesc, yaxisdesc, xaxistype, yaxistype, xaxisformat) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)',
            [req.params.seriesKey,
                req.body.datasetTypeDesc,
                req.body.datasetTypeRelativeTime,
                req.body.datasetTypeXAxisDesc,
                req.body.datasetTypeYAxisDesc,
                req.body.datasetTypeXAxisType,
                req.body.datasetTypeYAxisType,
                req.body.datasetTypeXAxisFormat
            ],
            function (err, result) {
                if (err) res.status(500).send(err.detail); else res.send('ok');
                done();
            });
    });
});

router.get('/definition', function (req, res, next) {
    pool.connect(function (err, client, done) {
        if (err) {
            res.status(500).send(err);
            return;
        }

        client.query('select key, description, relativeTime, xAxisDesc, yAxisDesc, xAxisType, yAxisType, xAxisFormat from MeasurementDescription',
            [], function (err, result) {
                if (err) {
                    res.status(500).send(err);
                    return;
                }
                var idx;

                var definition = [];
                var defByKey = {};
                for (idx in result.rows) {
                    var currDef = result.rows[idx];
                    var newDef = {
                        key: currDef.key,
                        desc: currDef.description,
                        relativeTime: currDef.relativetime,
                        xAxis: {
                            desc: currDef.xaxisdesc,
                            type: currDef.xaxistype,
                            format: currDef.xaxisformat !== null ? currDef.xaxisformat : undefined,
                        },
                        yAxis: {
                            desc: currDef.yaxisdesc,
                            type: currDef.yaxistype
                        },
                        series: []
                    };
                    definition.push(newDef);
                    defByKey[currDef.key] = newDef;
                }

                client.query('select measurementkey, measurementseries, description from MeasurementSeriesDescription',
                    [], function (err, result) {
                        for (idx in result.rows) {
                            var currSerDef = result.rows[idx];
                            defByKey[currSerDef.measurementkey].series.push({
                                snum: currSerDef.measurementseries,
                                desc: currSerDef.description
                            });
                        }

                        res.send({dataDefinition: definition});
                        done();
                    });
            });

    });
});

function getDataDefinition(key, callback) {
    pool.connect(function (err, client, done) {
        if (err) {
            callback(null);
            return;
        }

        client.query('select key, description, relativeTime, xAxisDesc, yAxisDesc, xAxisType, yAxisType, xAxisFormat from MeasurementDescription where key = $1',
            [key], function (err, result) {
                if (err) {
                    callback(null);
                    done();
                    return;
                }

                var currDef = result.rows[0];
                var definition = {
                    key: currDef.key,
                    desc: currDef.description,
                    relativeTime: currDef.relativetime,
                    xAxis: {
                        desc: currDef.xaxisdesc,
                        type: currDef.xaxistype,
                        format: currDef.xaxisformat !== null ? currDef.xaxisformat : undefined,
                    },
                    yAxis: {
                        desc: currDef.yaxisdesc,
                        type: currDef.yaxistype
                    },
                    series: []
                };

                client.query('select measurementkey, measurementseries, description from MeasurementSeriesDescription where measurementkey = $1',
                    [key], function (err, result) {
                        for (var idx in result.rows) {
                            var currSerDef = result.rows[idx];
                            definition.series.push({
                                snum: currSerDef.measurementseries,
                                desc: currSerDef.description
                            });
                        }

                        done();
                        callback(definition);
                    });
            });
    });
}

module.exports = router;
