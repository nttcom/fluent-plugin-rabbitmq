require_relative "../helper"

require "fluent/test/driver/output"
require "fluent/plugin/out_rabbitmq"

class RabbitMQOutputTest < Test::Unit::TestCase
  def setup
    Fluent::Test.setup
    @time = Fluent::Engine.now
    @tag = "test.test"
    
    bunny = Bunny.new
    bunny.start
    @channel = bunny.create_channel
    @queue = @channel.queue("test_out_fanout")
    fanout_exchange = Bunny::Exchange.new(@channel, "fanout", "test_out_fanout")
    @queue.bind(fanout_exchange)
  end

  def teardown
    super
    Fluent::Engine.stop
  end

  CONFIG = %[
    host localhost
    port 5672
    user guest
    pass guest
    vhost /
    exchange test_out_fanout
    exchange_type fanout
    heartbeat 10
  ]

  def create_driver(conf = CONFIG)
    Fluent::Test::Driver::Output.new(Fluent::Plugin::RabbitMQOutput).configure(conf)
  end

  def test_configure
    d = create_driver
    assert_equal "localhost", d.instance.host
    assert_equal 5672, d.instance.port
    assert_equal "guest", d.instance.user
    assert_equal "guest", d.instance.pass
    assert_equal "/", d.instance.vhost
    assert_equal "test_out_fanout", d.instance.exchange
    assert_equal "fanout", d.instance.exchange_type
    assert_equal 10, d.instance.heartbeat
  end

  def test_start_and_shutdown
    d = create_driver

    d.instance.start
    d.instance.shutdown
  end

  def test_emit
    d = create_driver

    record = {"test_emit" => 1}
    d.run(default_tag: "test.test") do
      d.feed(@time, record)
    end

    _, _, body = @queue.pop
    assert_equal(record, JSON.parse(body))
  end

  def test_topic
    d = create_driver(%[
      exchange test_out_topic
      exchange_type topic
      routing_key test_out_topic
    ])

    queue = @channel.queue("test_out_topic")
    topic_exchange = Bunny::Exchange.new(@channel, "topic", "test_out_topic")
    queue.bind(topic_exchange, routing_key: "test_out_topic")

    record = {"test_topic" => 1}
    d.run(default_tag: "test.test") do
      d.feed(@time, record)
    end

    _, _, body = queue.pop
    assert_equal(record, JSON.parse(body))
  end

  def test_timestamp
    d = create_driver(%[
      exchange test_out_fanout
      exchange_type fanout
      timestamp true
    ])

    record = {"test_timestamp" => true}
    d.run(default_tag: "test.test") do
      d.feed(@time, record)
    end

    _, properties, _ = @queue.pop
    assert_equal(@time, properties[:timestamp].to_i)
  end

  def test_id_key
    d = create_driver(%[
      exchange test_out_fanout
      exchange_type fanout
      id_key test_id
    ])

    message_id = "abc123"
    record = {"test_id" => message_id, "foo" => "bar"}
    d.run(default_tag: "test.test") do
      d.feed(@time, record)
    end

    _, properties, _ = @queue.pop
    assert_equal(message_id, properties[:message_id])
  end

  def test_server_interval
    d = create_driver(%[
      exchange test
      exchange_type fanout
      heartbeat server
    ])
    assert_equal(:server, d.instance.heartbeat)
  end

  def test_server_interval_invalid_string
    assert_raise ArgumentError do
      create_driver(%[
        exchange test
        exchange_type fanout
        heartbeat invalid
      ])
    end
  end

  def test_emit_ltsv
    d = create_driver(%[
      exchange test_out_fanout
      exchange_type fanout
      format ltsv
    ])

    record = {test_emit_ltsv: "2"}
    d.run(default_tag: "test.test") do
      d.feed(@time, record)
    end

    _, _, body = @queue.pop
    assert_equal(record, LTSV.parse(body).first)
  end

  def test_emit_msgpack
    d = create_driver(%[
      exchange test_out_fanout
      exchange_type fanout
      format msgpack
    ])

    record = {"test_emit_msgpack" => true}
    d.run(default_tag: "test.test") do
      d.feed(@time, record)
    end

    _, _, body = @queue.pop
    assert_equal(record, MessagePack.unpack(body))
  end

  def test_emit_single_value
    d = create_driver(%[
      exchange test_out_fanout
      exchange_type fanout
      format single_value
    ])

    string = "test_emit_single_value"
    record = {"message" => string}
    d.run(default_tag: "test.test") do
      d.feed(@time, record)
    end

    _, _, body = @queue.pop
    body.force_encoding("utf-8")
    assert_equal(string, body.chomp)
  end

  def test_buffered_emit
    d = create_driver(%[
      exchange test_out_fanout
      exchange_type fanout
      format json
      timestamp true
      <buffer>
      </buffer>
    ])

    record = {"test_emit" => 1}
    d.run(default_tag: "test.test") do
      d.feed(@time, record)
    end

    _, properties, body = @queue.pop
    assert_equal(record, JSON.parse(body))
    assert_equal(@time, properties[:timestamp].to_i)
  end
end
