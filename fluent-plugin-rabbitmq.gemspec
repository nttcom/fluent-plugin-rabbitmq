lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)

Gem::Specification.new do |spec|
  spec.name          = "fluent-plugin-rabbitmq"
  spec.version       = "0.0.6"
  spec.authors       = ["NTT Communications"]
  spec.email         = ["masaki.matsushita@ntt.com"]

  spec.summary       = %q{fluent plugin for rabbitmq (AMQP)}
  spec.description   = %q{fluent plugin for rabbitmq (AMQP)}
  spec.homepage      = "https://github.com/nttcom/fluent-plugin-rabbitmq"
  spec.license       = "Apache-2.0"

  spec.files         = `git ls-files -z`.split("\x0").reject { |f| f.match(%r{^(test|spec|features)/}) }
  spec.require_paths = ["lib"]

  spec.add_development_dependency "bundler"
  spec.add_development_dependency "rake"
  spec.add_development_dependency "test-unit"
  spec.add_development_dependency "ltsv"
  spec.add_development_dependency "msgpack"

  spec.add_runtime_dependency "fluentd", ">= 0.14.0"

  spec.add_dependency "bunny", "~> 2.13.0"
end
