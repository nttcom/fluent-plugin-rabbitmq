require "test-unit"
require "serverengine"
require "fluent/test"

require "bunny"
require "json"
require "ltsv"
require "msgpack"

module ServerEngine
  def windows? #XXX: workaround
    false
  end
end

def config_element(name = 'test', argument = '', params = {}, elements = [])
  Fluent::Config::Element.new(name, argument, params, elements)
end
