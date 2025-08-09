require_relative "../helper"

require "fluent/test/driver/input"
require "fluent/plugin/in_rabbitmq"

class RabbitMQInputTest < Test::Unit::TestCase
  def setup
    Fluent::Test.setup
    @time = Fluent::Engine.now

    bunny = Bunny.new
    bunny.start
    @channel = bunny.create_channel
    queue = @channel.queue("test_in_fanout")
    @fanout_exchange = Bunny::Exchange.new(@channel, :fanout, "test_in_fanout")
    @fanout_exchange_bind = Bunny::Exchange.new(@channel, :fanout, "test_in_bind")
    @topic_exchange = Bunny::Exchange.new(@channel, :topic, "test_in_topic")
    queue.bind(@fanout_exchange)
  end

  def teardown
    super
    Fluent::Engine.stop
  end

  CONFIG = %[
    tag test.test
    host localhost
    port 5672
    user guest
    pass guest
    vhost /
    heartbeat 10
    queue test_in_fanout
  ]

  def create_driver(conf = CONFIG)
    Fluent::Test::Driver::Input.new(Fluent::Plugin::RabbitMQInput).configure(conf)
  end

  def test_configure
    d = create_driver
    assert_equal "localhost", d.instance.host
    assert_equal 5672, d.instance.port
    assert_equal "guest", d.instance.user
    assert_equal "guest", d.instance.pass
    assert_equal "/", d.instance.vhost
    assert_equal 10, d.instance.heartbeat
  end

  def test_start_and_shutdown
    d = create_driver

    d.instance.start
    d.instance.shutdown
  end

  def test_emit
    d = create_driver

    expect_hash = {"foo" => "bar"}
    d.run(expect_emits: 1) do
      @fanout_exchange.publish(expect_hash.to_json)
    end

    d.events.each do |event|
      assert_equal expect_hash, event[2]
    end
  end

  def test_emit_ltsv
    conf = CONFIG.clone
    conf << "\nformat ltsv\n"
    d = create_driver(conf)

    expect_hash = {"foo" => "bar", "bar" => "baz"}
    d.run(expect_emits: 1) do
      @fanout_exchange.publish(LTSV.dump(expect_hash))
    end

    d.events.each do |event|
      assert_equal expect_hash, event[2]
    end
  end

  def test_emit_msgpack
    conf = CONFIG.clone
    conf << "\nformat msgpack\n"
    d = create_driver(conf)

    expect_hash = {"foo" => "bar", "bar" => "baz"}
    d.run(expect_emits: 1) do
      @fanout_exchange.publish(expect_hash.to_msgpack)
    end

    d.events.each do |event|
      assert_equal expect_hash, event[2]
    end
  end

  def test_emit_single_value
    conf = CONFIG.clone
    conf << "\nformat none\n"
    d = create_driver(conf)

    expect_string = "foo"
    d.run(expect_emits: 1) do
      @fanout_exchange.publish(expect_string)
    end

    d.events.each do |event|
      assert_equal({"message" => expect_string}, event[2])
    end
  end

  def test_emit_with_timestamp
    d = create_driver

    expect_time = Fluent::EventTime.parse("2018-08-15 13:14:15 UTC")
    expect_hash = {"foo" => "bar"}
    d.run(expect_emits: 1) do
      @fanout_exchange.publish(expect_hash.to_json, timestamp: expect_time.to_i)
    end

    d.events.each do |event|
      assert_equal expect_time, event[1]
      assert_equal expect_hash, event[2]
    end
  end

  def test_emit_direct
    conf = CONFIG.clone.gsub(/queue\stest_in_fanout/, "queue test_in_direct")
    d = create_driver(conf)

    queue = @channel.queue("test_in_direct")
    direct_exchange = Bunny::Exchange.new(@channel, "direct", "test_in_direct")
    queue.bind(direct_exchange)

    expect_hash = {"foo" => "bar"}
    d.run(expect_emits: 1) do
      direct_exchange.publish(expect_hash.to_json)
    end

    d.events.each do |event|
      assert_equal expect_hash, event[2]
    end
  end

  def test_emit_topic
    conf = CONFIG.clone.gsub(/queue\stest_in_fanout/, "queue test_in_topic")
    d = create_driver(conf)

    queue = @channel.queue("test_in_topic")
    topic_exchange = Bunny::Exchange.new(@channel, "topic", "test_in_topic")
    queue.bind(topic_exchange, routing_key: "test_in_topic")

    expect_hash = {"foo" => "bar"}
    d.run(expect_emits: 1) do
      topic_exchange.publish(expect_hash.to_json, routing_key: "test_in_topic")
    end

    d.events.each do |event|
      assert_equal expect_hash, event[2]
    end
  end

  def test_durable
    conf = CONFIG.clone.gsub(/queue\stest_in_fanout/, "queue test_in_durable")
    conf << "\ndurable true\n"
    d = create_driver(conf)
    d.run {}
    assert_nothing_raised do
      @channel.queue("test_in_durable", durable: true)
    end
  end

  def test_auto_delete
    conf = CONFIG.clone.gsub(/queue\stest_in_fanout/, "queue test_in_auto_delete")
    conf << "\nauto_delete true\n"
    d = create_driver(conf)
    d.run {}
    assert_nothing_raised do
      @channel.queue("test_in_auto_delete", auto_delete: true)
    end
  end

  def test_exclusive
    conf = CONFIG.clone.gsub(/queue\stest_in_fanout/, "queue test_in_exclusive")
    conf << "\nexclusive true\n"
    d = create_driver(conf)
    d.run {}
    assert_nothing_raised do
      @channel.queue("test_in_exclusive", exclusive: true)
    end
  end

  def test_hosts
    conf = CONFIG.clone.gsub(/host\slocalhost/, "hosts [\"localhost\"]")
    d = create_driver(conf)
    d.run {}
  end

  def test_prefetch_count
    conf = CONFIG.clone
    conf << "\nprefetch_count 16^n"
    d = create_driver(conf)
    d.run {}
  end

  def test_bind
    conf = CONFIG.clone
    conf.gsub!(/queue\stest_in_fanout/, "queue test_in_bind")
    conf << "\nexchange test_in_bind"
    d = create_driver(conf)

    expect_hash = {"foo" => "bar", "bar" => "baz"}
    d.run(expect_emits: 1) do
      @fanout_exchange_bind.publish(expect_hash.to_json)
    end

    d.events.each do |event|
      assert_equal expect_hash, event[2]
    end
  end

  def test_bind_routing_by_tag
    conf = CONFIG.clone
    conf.gsub!(/queue\stest_in_topic/, "queue test_in_bind_routing")
    conf << "\nexchange test_in_topic"
    d = create_driver(conf)

    expect_hash = {"foo" => "bar", "bar" => "baz"}
    d.run(expect_emits: 1) do
      @topic_exchange.publish(expect_hash.to_json, routing_key: "test.test")
    end
    
    d.events.each do |event|
      assert_equal expect_hash, event[2]
    end
  end

  def test_bind_routing_by_key
    conf = CONFIG.clone
    conf.gsub!(/queue\stest_in_topic/, "queue test_in_bind_routing")
    conf << "\nexchange test_in_topic\nrouting_key test.bind.routing"
    d = create_driver(conf)

    expect_hash = {"foo" => "bar", "bar" => "baz"}
    d.run(expect_emits: 1) do
      @topic_exchange.publish(expect_hash.to_json, routing_key: "test.bind.routing")
    end

    d.events.each do |event|
      assert_equal expect_hash, event[2]
    end
  end

  def test_include_headers
    conf = CONFIG.clone
    conf << "\ninclude_headers true\n"
    d = create_driver(conf)

    hash = {"foo" => "bar"}
    headers = {"hoge" => "fuga"}
    expect_hash = hash.dup
    expect_hash["headers"] = headers

    d.run(expect_emits: 1) do
      @fanout_exchange.publish(hash.to_json, headers: headers)
    end

    d.events.each do |event|
      assert_equal expect_hash, event[2]
    end
  end

  def test_include_headers_without_payload
    conf = CONFIG.clone
    conf << "\ninclude_headers true\n"
    d = create_driver(conf)

    headers = {"hoge" => "fuga"}
    expect_hash = {"headers" => headers}

    d.run(expect_emits: 1) do
      @fanout_exchange.publish("", headers: headers)
    end

    d.events.each do |event|
      assert_equal expect_hash, event[2]
    end
  end

  def test_headers_key
    conf = CONFIG.clone
    conf << "\ninclude_headers true\nheaders_key test"
    d = create_driver(conf)

    hash = {"foo" => "bar"}
    headers = {"hoge" => "fuga"}
    expect_hash = hash.dup
    expect_hash["test"] = headers

    d.run(expect_emits: 1) do
      @fanout_exchange.publish(hash.to_json, headers: {"hoge": "fuga"})
    end

    d.events.each do |event|
      assert_equal expect_hash, event[2]
    end
  end

  def test_include_delivery_info
    conf = CONFIG.clone.gsub(/queue\stest_in_fanout/, "queue test_in_topic")
    conf << "\ninclude_delivery_info true\n"
    d = create_driver(conf)

    queue = @channel.queue("test_in_topic")
    topic_exchange = Bunny::Exchange.new(@channel, "topic", "test_in_topic")
    queue.bind(topic_exchange, routing_key: "test_in_topic")

    delivery_info = { "exchange" => "test_in_topic", "routing_key" => "test_in_topic" }
    expect_hash = {"foo" => "bar"}
    expect_hash["delivery_info"] = delivery_info
    d.run(expect_emits: 1) do
      @topic_exchange.publish(expect_hash.to_json, routing_key: "test_in_topic")
    end

    d.events.each do |event|
      # assert_equal expect_hash, event[2]
      assert_equal expect_hash["foo"], event[2]["foo"]
      assert_equal expect_hash["delivery_info"][":exchange"], event[2]["delivery_info"][":exchange"]
      assert_equal expect_hash["delivery_info"][":routing_key"], event[2]["delivery_info"][":routing_key"]
    end
  end

  def test_manual_ack
    conf = CONFIG.clone
    conf << "\nmanual_ack true"
    d = create_driver(conf)
    expect_hash = {"foo" => "bar"}
    d.run(expect_emits: 1) do
      @fanout_exchange.publish(expect_hash.to_json)
    end

    d.events.each do |event|
      assert_equal expect_hash, event[2]
    end
  end
end
