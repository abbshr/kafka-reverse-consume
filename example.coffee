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