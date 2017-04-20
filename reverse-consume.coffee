kafka = require "node-rdkafka"
{ EventEmitter } = require "events"

class RevConsumer extends EventEmitter
  constructor: (options = {}) ->
    super options
    { @brokers, @group = "kafka-reverse", @topic, @partition = 0, @head = 0, @tail, @page_size = 1000 } = options

    @size = @tail - @head + 1
    @pass = Math.ceil @size / @page_size
    @counter = 0
    @msgbuf = null

    @consumer = new kafka.KafkaConsumer "metadata.broker.list": @brokers, "group.id": @group, "enable.auto.commit": off
    @consumer.on "ready", @_nextpass
    @consumer.connect()

  _consume: (message) =>
    @msgbuf.unshift message

    if @msgbuf.length + (@counter - 1) * @page_size is @size
      @consumer.removeListener "data", @_consume
      @emit "data", @msgbuf, @_end

    else if @msgbuf.length is @page_size
      @consumer.removeListener "data", @_consume
      @emit "data", @msgbuf, @_nextpass
  
  _end: => @emit "end"
  _nextpass: =>
    @msgbuf = []
    len = if ++@counter is @pass then @size % @page_size else @page_size
    start = Math.max @tail - @page_size * @counter + 1, @head

    @consumer.assign [topic: @topic, partition: @partition, offset: start]
    @consumer.consume len
    @consumer.on "data", @_consume

module.exports = RevConsumer