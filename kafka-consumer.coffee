kafka = require './kafka'

cfg =
  client:
    connectionString: "192.168.13.157:2181" # zookeeper
    clientId: "kafka-node-client-consumer" # 每个 zookeeper client 应该使用不同的 clientId
  consumer:
    autoCommit: off
    groupId: "kaka-node-group" # 每个 consumer 应该使用不同的 groupId
    fromOffset: yes # 是否从特定 offset 处开始消费, 如果逆序消费, 需要指定
    topic: "test" # topic
    offset: 11 # 指定 offset, (当 fromOffset 为 true 时有效), 如果逆序消费, 需要指定
    fetchMaxBytes: 512
    reverse: true # 是否逆序消费
    olderOffset: 0 # 指定逆向消费的终止 offset, 只在 reverse = true 时有效

topic_meta =
  topic: cfg.consumer.topic
  offset: cfg.consumer.offset
  partition: 0

client = new kafka.Client cfg.client.connectionString, cfg.client.clientId

# time: -1 => 最新可用的(目前空闲的) offset, -2 => 最早的有效 offset
# offset = new kafka.Offset client
# offset.fetch [{topic: "robin-record", partition: 0, time: -2}], (err, data) ->
#   console.log err, data

on_message = (message) -> console.log message
consumer = new kafka.Consumer client, [topic_meta], cfg.consumer
consumer.once "message", on_message
consumer.on "done", (topics) ->
  @removeListener "message", on_message
  @once "message", on_message
  setImmediate => @fetch @reverseOffsets topics