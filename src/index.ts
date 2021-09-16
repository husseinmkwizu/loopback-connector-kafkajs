import { Kafka, logLevel, ProducerRecord } from "kafkajs";

exports.initialize = function (dataSource, cb) {
  var settings = dataSource.settings;
  const { debug, host } = settings;

  //brokers
  let brokers = null;
  if (typeof host === "string") {
    brokers = host.split(",");
  }

  const kafka = new Kafka({
    clientId: settings?.clientId ?? "kafkajs-producer",
    brokers: brokers,
    sasl: settings?.sasl,
    ssl: settings?.ssl,
    retry: settings?.retry,
    logLevel: debug ? logLevel.DEBUG : logLevel.NOTHING,
  });

  const producer = kafka.producer();

  if (debug) {
    this.producer.logger().info("KAFKAJS -- Calling producer.connect");
  }

  producer.connect().then(() => {
    this.producer.logger().info("KAFKAJS -- Producer ready ");
  });

  var connector = new KafkaConnector(producer, { debug });
  dataSource.connector = connector;
  connector.DataAccessObject = function () {};
  for (var m in KafkaConnector.prototype) {
    var method = KafkaConnector.prototype[m];
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
function KafkaConnector(producer, options) {
  this.name = "kafka";
  this._models = {};
  this.producer = producer;
  this.options = options;
}

KafkaConnector.prototype.send = function (payload: ProducerRecord, cb) {
  const { debug } = this.options;
  if (debug) {
    this.producer.logger().info("KAFKAJS -- SEND CALLED");
  }

  this.producer
    .send(payload)
    .then((response) => {
      cb(null, response);
    })
    .catch((error) => {
      cb(error, null);
    });
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

setRemoting(KafkaConnector.prototype.send, {
  description: "Send a message to Kafka server",
  accepts: [
    {
      arg: "payload",
      type: "object",
      description: "Message payload",
      http: { source: "body" },
    },
  ],
  returns: { arg: "data", type: "object", root: true },
  http: { verb: "post", path: "/" },
});
