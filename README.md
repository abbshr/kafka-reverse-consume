Kafka Reverse Consumer
===

## Simple Usage

```coffee
RevConsumer = require "./reverse-consume"

rev = new RevConsumer
  brokers: "192.168.13.157:9092"
  topic: "robin-record"
  head: 2000
  tail: 2000

rev.on "data", (dataset, proceed) ->
  console.log dataset
  proceed()

rev.on "end", -> console.log "done"
```

## Options

```coffee
brokers: "192.168.13.157:9092" # kafka brokers list
group: "kakfa" # consumer group
topic: "robin-record"
partition: 0
head: 2000 # the earliest offset to consume
tail: 2000 # the latest offset to consume
page_size: 1000 # when fragment, this is the max page size
```

## Events

### `data`

`data` represent a series of data (dataset) collected in a inverse order compare to kafka normal consuming progress.

### `end`

`end` triggered when all messages between `head` and `tail` (include edge) has been consumed and no more data will be fetched.

## proceed callback

the `data` event's callback has a `proceed` function, which is used to notify the consumer to continue ingesting the earlier data because the current process has been finished (thus, the `data` event could be triggered again).