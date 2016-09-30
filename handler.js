'use strict';

const AWS = require('aws-sdk');
AWS.config.update({region: 'ap-southeast-2'});
var dynamodb = new AWS.DynamoDB({ apiVersion: '2012-08-10' });
var s3 = new AWS.S3({apiVersion: '2006-03-01'});
var bucketName = 'widgetprojection';
var tableName = 'Widgets';

module.exports.hello = (event, context, cb) => {
  var records = event.Records;
  var currentRecord = null;
  var currentKey = null;
  var currentSequencer = null;
  var tasks = [];
  for(var i = 0; i < records.length; i++){
    currentRecord = records[i];
    currentKey = currentRecord.s3.object.key;
    currentSequencer = currentRecord.s3.object.sequencer;
    var p =
      loadFile(currentKey).then(function(data){
        return insertItemIntoDb(currentKey, currentSequencer, data);
      });
    tasks.push(p);
  };

  Promise.all(tasks).then(function(){
    cb(null, { message: 'success' });
  }).catch(function(reason){
    cb({ error: reason });
  });
}

var loadFile = function(s3Key) {
  return new Promise(function(resolve,reject){
    var params = {
      Bucket: bucketName,
      Key: s3Key
    };
    s3.getObject(params, function(err,data){
        if(err !== null) return reject(err);
        resolve(data);
    });
  });
}

var insertItemIntoDb = function(key, sequencer, content) {
  var bodyUtf8 = content.Body.toString("utf8");
  return new Promise(function(resolve,reject){
    var params = {
      Item: {
        WidgetId: {
          S: key
        },
        Body: {
          S: bodyUtf8
        },
        Sequencer: {
          S: sequencer
        }
      },
        TableName: tableName,
        ConditionExpression: "Sequencer < :Sequencer OR attribute_not_exists(Sequencer)",
        ExpressionAttributeValues: {
            ":Sequencer": {
                S: sequencer
            }
        }

    };
    dynamodb.putItem(params, function(err){
        if(err !== null) {
            if (err.code = "ConditionalCheckFailedException") {
                resolve(); // condition checks failures are to be expected
            }
            else {
                reject(err);
            }
        } 
        else {
            resolve();
        }
    })
  });
}
