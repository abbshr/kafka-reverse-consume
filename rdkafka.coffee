kafka = require "node-rdkafka"

brokers = "192.168.13.157:9092"
group = "kafka"
topic = "robin-record"
partition = 0

head = 0
tail = 1000
page_size = 100

size = tail - head + 1
pass = Math.ceil size / page_size
counter = 0
msgbuf = []

consumer = new kafka.KafkaConsumer
  "metadata.broker.list": brokers
  "group.id": group
  "enable.auto.commit": off

consume = (message) ->
  msgbuf.unshift message

  if msgbuf.length + (counter - 1) * page_size is size
    consumer.removeListener "data", consume
    proc msgbuf, end
  else if msgbuf.length is page_size
    consumer.removeListener "data", consume
    proc msgbuf, nextpass

nextpass = ->
  msgbuf = []
  len = if ++counter is pass then size % page_size else page_size
  start = Math.max tail - page_size * counter + 1, head
  consumer.assign [topic: topic, partition: partition, offset: start]
  consumer.consume len
  consumer.on "data", consume

proc = (msgs, proceed) ->
  # custom reverse logic
  console.log "message count:", msgs.length
  proceed()

end = ->
  # all message fetched
  console.log "done"

consumer.on "ready", nextpass
consumer.connect()