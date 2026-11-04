require_relative "../helper"

require "fluent/test/driver/output"
require "fluent/plugin/out_rabbitmq"

# Covers how the TLS configuration parameters are translated into Bunny options.
# Configuring the plugin is enough here: no connection is opened, so these tests
# need no TLS-enabled broker.
class RabbitMQOutputTLSTest < Test::Unit::TestCase
  def setup
    Fluent::Test.setup
  end

  BASE_CONFIG = %[
    host localhost
    port 5671
    exchange test_tls
    exchange_type fanout
    tls true
  ]

  def create_driver(conf)
    Fluent::Test::Driver::Output.new(Fluent::Plugin::RabbitMQOutput).configure(conf)
  end

  def bunny_options_of(driver)
    driver.instance.instance_variable_get(:@bunny).instance_variable_get(:@opts)
  end

  def test_tls_protocol_is_not_set_by_default
    d = create_driver(BASE_CONFIG)

    assert_nil d.instance.tls_protocol
    assert_nil bunny_options_of(d)[:tls_protocol]
  end

  data("TLS 1.2" => ["TLSv1_2", :TLSv1_2],
       "TLS 1.3" => ["TLSv1_3", :TLSv1_3],
       "short form" => ["1.2", :"1.2"])
  def test_tls_protocol_is_passed_as_symbol(data)
    configured, expected = data

    d = create_driver(BASE_CONFIG + %[
      tls_protocol #{configured}
    ])

    assert_equal expected, bunny_options_of(d)[:tls_protocol]
  end

  def test_tls_silence_warnings_is_not_set_by_default
    d = create_driver(BASE_CONFIG)

    assert_nil d.instance.tls_silence_warnings
    assert_false bunny_options_of(d).key?(:tls_silence_warnings)
  end

  data("enabled" => true, "disabled" => false)
  def test_tls_silence_warnings_is_passed_through(configured)
    d = create_driver(BASE_CONFIG + %[
      tls_silence_warnings #{configured}
    ])

    assert_equal configured, bunny_options_of(d)[:tls_silence_warnings]
  end
end
