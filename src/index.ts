"use strict";

var kafka = require("kafka-node");
var Client = kafka.KafkaClient;
var Producer = kafka.HighLevelProducer;

exports.initialize = function (dataSource, cb) {
    var settings = dataSource.settings;
    const { debug } = settings
    if (debug) {
        console.log("KAFKAJS -- INITIALIZE CALLED")
    }

    const client = new Client({
        kafkaHost: settings?.host,
        reconnectOnIdle: true,
        autoConnect: true,
        idleConnection: 300000,
        sasl: settings?.sasl,
        sslOptions: settings?.sslOptions
    });

    var producer = new Producer(client);
    producer.on("ready", function () {
        console.log("Producer ready");
    });

    producer.on("error", function (error) {
        console.log("An error occured", error);
    });
    var connector = new Kafka(producer, { debug });
    dataSource.connector = connector;
    connector.DataAccessObject = function () { };
    for (var m in Kafka.prototype) {
        var method = Kafka.prototype[m];
        if ("function" === typeof method) {
            connector.DataAccessObject[m] = method.bind(connector);
            for (var k in method) {
                connector.DataAccessObject[m][k] = method[k];
            }
        }
    }
    cb && cb();
};

/**
 *  @constructor
 *  Constructor for KAFKA connector
 *  @param {Object} The kafka-node producer
 */
function Kafka(producer, options) {
    this.name = "kafka";
    this._models = {};
    this.producer = producer;
    this.options = options
}

Kafka.prototype.send = function (topic, messages, cb) {
    const { debug } = this.options
    if (debug) {
        console.log("KAFKAJS -- SEND CALLED")
    }
    var producer = this.producer;
    var stringify = function (json) {
        return typeof json === "string" ? json : JSON.stringify(json);
    };
    messages = Array.isArray(messages)
        ? messages.map(function (item) {
            return stringify(item);
        })
        : stringify(messages);
    producer.send(
        [
            {
                topic: topic,
                messages: messages,
            },
        ],
        cb
    );
};

function setRemoting(fn, options) {
    options = options || {};
    for (var opt in options) {
        if (options.hasOwnProperty(opt)) {
            fn[opt] = options[opt];
        }
    }
    fn.shared = true;
}

setRemoting(Kafka.prototype.send, {
    description: "Send a message to Kafka server",
    accepts: [
        {
            arg: "topic",
            type: "String",
            description: "Topic name",
            http: { source: "query" },
        },
        {
            arg: "messages",
            type: "object",
            description: "Message body",
            http: { source: "body" },
        },
    ],
    returns: { arg: "data", type: "object", root: true },
    http: { verb: "post", path: "/" },
});
