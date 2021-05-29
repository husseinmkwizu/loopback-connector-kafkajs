# loopback-connector-kafkajs

========
Kafka connector for loopback-datasource-juggler.

## Install

```sh
npm install loopback-connector-kafkajs --save
```

## Usage

```js
var DataSource = require("loopback-datasource-juggler").DataSource;
// The options for kafka-node
var options = {
  host: "127.0.0.1:9092",
};
var dataSource = new DataSource("kafkajs", options);
```

Or configure it in `datasources.json`

```js
{
    "kafkajs": {
        "connector": "kafkajs",
        "host": "127.0.0.1:9092"
    }
}
```

## LICENSE

MIT
