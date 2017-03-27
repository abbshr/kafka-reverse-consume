kafka = require './kafka'

cfg =
  kafka:
    client:
      connectionString: "192.168.13.157:2181" # zookeeper
      clientId: "kafka-node-client-consumer" # 每个 zookeeper client 应该使用不同的 clientId
    consumer:
      autoCommit: off
      groupId: "kafka-node-group" # 每个 consumer 应该使用不同的 groupId
      fromOffset: yes # 是否从特定 offset 处开始消费
      topic: "robin-record" # topic
      offset: 790000# 指定 offset, (当 fromOffset 为 true 时有效)
      fetchMaxBytes: 512
      reverse: true

topic_meta =
  topic: cfg.kafka.consumer.topic
  offset: cfg.kafka.consumer.offset
  partition: 0

oldest_offset = 787954
client = new kafka.Client cfg.kafka.client.connectionString, cfg.kafka.client.clientId
# offset = new kafka.Offset client
# offset.fetch [{topic: "robin-record", partition: 0, time: Date.now(), maxNum: 1}], (err, data) ->
#   console.log err, data

on_message = (message) -> console.log message
consumer = new kafka.Consumer client, [topic_meta], cfg.kafka.consumer
consumer.once "message", on_message
consumer.on "done", (topics) ->
  curr_offset = topics[topic_meta.topic][topic_meta.partition]
  return unless curr_offset? and curr_offset > oldest_offset
  @reverseOffsets topics
  @once "message", on_message
  setImmediate => @fetch()