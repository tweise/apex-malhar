/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
var _ = require('underscore');
var async = require('async');
var Db = require('mongodb').Db;
var Server = require('mongodb').Server;
var Deferred = require('jquery-deferred').Deferred;
var config = require('../config');

// Create deferred for mongo connection
var mongoCxn = Deferred();

var demoEnabled = (config.fraud.mongo.host && config.fraud.mongo.port);

if (demoEnabled) {
    // Connect to the mongo database
    var db = new Db(config.fraud.mongo.dbName, new Server(config.fraud.mongo.host, config.fraud.mongo.port));
    db.open(function(err, mongoclient){
        console.log('Mongo Database connection opened');
        mongoCxn.resolve();
    });
} else {
    mongoCxn.reject();
}

/**
 * Returns a map with keys of the collection names
 * and values of their counts
*/
function getAlertCount(req, res) {
    
    // Ensure mongo connection has been made
    if ( mongoCxn.state() === "pending" ) {
        return mongoCxn.done(getAlertCount.bind(this, req, res));
    }
    
    // Alerts
    var alertNames = ['ccAlerts', 'avgAlerts', 'binAlerts'];

    // Get the counts of every alert collection
    async.map(
        
        alertNames,
        
        function(key,cb) {
            if (req.query.since) {
                console.log('since was specified in getAlertCount');
                db.collection(key).find({ time: { $gt: req.query.since*1 } }).count(cb);
            } else {
                db.collection(key).count(cb);
            }
        },

        function(err, response) {
            // Create object in the form: { ccAlerts: [NUM], avgAlerts: [NUM], binAlerts: [NUM] }
            response = _.object(alertNames, response);
            res.send(response);
        }
        
    );
}

/**
 * Retrieves a recent document from the txStats collection
*/
function getRecentStats(req, res) {
    // Ensure mongo connection has been made
    if ( mongoCxn.state() === "pending" ) {
        return mongoCxn.done(getAlertCount.bind(this, req, res));
    }
    
    // Alerts
    var colName = 'txStats';

    // Get random recent stats
    db.collection(colName).find().sort({"time":-1}).skip(2000).limit(5).toArray(function(err, items) {
        var randIndex = Math.floor(Math.random() * 5);
        res.send(items[randIndex]);
    });
    
}

exports.getAlertCount = getAlertCount;
exports.getRecentStats = getRecentStats;
