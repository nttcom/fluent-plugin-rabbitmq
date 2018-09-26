#
# fluent-plugin-rabbitmq
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
#
#
require "fluent/plugin/input"

module Fluent::Plugin
  class RabbitMQInput < Input
    Fluent::Plugin.register_input("rabbitmq", self)

    helpers :parser, :compat_parameters

    config_param :tag, :string

    config_section :parse do
	    config_set_default :@type, "json"
	  end

    config_param :host, :string, default: nil
    config_param :hosts, :array, default: nil
    config_param :port, :integer, default: nil
    config_param :user, :string, default: nil
    config_param :pass, :string, default: nil, secret: true
    config_param :vhost, :string, default: nil
    config_param :exchange, :string, default: nil
    config_param :routing_key, :string, default: nil
    config_param :connection_timeout, :time, default: nil
    config_param :continuation_timeout, :integer, default: nil
    config_param :automatically_recover, :bool, default: nil
    config_param :network_recovery_interval, :time, default: nil
    config_param :recovery_attempts, :integer, default: nil
    config_param :auth_mechanism, :string, default: nil
    config_param :heartbeat, default: nil do |param|
      param == "server" ? :server : Integer(param)
    end
    config_param :consumer_pool_size, :integer, default: nil

    config_param :tls, :bool, default: false
    config_param :tls_cert, :string, default: nil
    config_param :tls_key, :string, default: nil
    config_param :tls_ca_certificates, :array, default: nil
    config_param :verify_peer, :bool, default: true

    config_param :queue, :string
    config_param :durable, :bool, default: false
    config_param :exclusive, :bool, default: false
    config_param :auto_delete, :bool, default: false

    config_param :prefetch_count, :integer, default: nil

    def initialize
      super
      require "bunny"
    end
    
    def configure(conf)
      compat_parameters_convert(conf, :parser)

      super

      bunny_options = {}
      bunny_options[:host] = @host if @host
      bunny_options[:hosts] = @hosts if @hosts
      bunny_options[:port] = @port if @port
      bunny_options[:user] = @user if @user
      bunny_options[:pass] = @pass if @pass
      bunny_options[:vhost] = @vhost if @vhost
      bunny_options[:connection_timeout] = @connection_timeout if @connection_timeout
      bunny_options[:continuation_timeout] = @continuation_timeout if @continuation_timeout
      bunny_options[:automatically_recover] = @automatically_recover if @automatically_recover
      bunny_options[:network_recovery_interval] = @network_recovery_interval if @network_recovery_interval
      bunny_options[:recovery_attempts] = @recovery_attempts
      bunny_options[:auth_mechanism] = @auth_mechanism if @auth_mechanism
      bunny_options[:heartbeat] = @heartbeat if @heartbeat
      bunny_options[:logger] = log

      bunny_options[:tls] = @tls
      bunny_options[:tls_cert] = @tls_cert if @tls_cert
      bunny_options[:tls_key] = @tls_key if @tls_key
      bunny_options[:tls_ca_certificates] = @tls_ca_certificates if @tls_ca_certificates
      bunny_options[:verify_peer] = @verify_peer

      @parser = parser_create

      @routing_key ||= @tag
      @bunny = Bunny.new(bunny_options)
    end
    
    def start
      super
      @bunny.start
      channel = @bunny.create_channel(nil, @consumer_pool_size)
      channel.prefetch(@prefetch_count) if @prefetch_count
      queue = channel.queue(
        @queue,
        durable: @durable,
        exclusive: @exclusive,
        auto_delete: @auto_delete
      )
      if @exchange
        queue.bind(@exchange, routing_key: @routing_key)
      end
      queue.subscribe do |delivery_info, properties, payload|
        @parser.parse(payload) do |time, record|
          time = if properties[:timestamp]
                    Fluent::EventTime.from_time(properties[:timestamp])
                 else
                    time
                 end
          router.emit(@tag, time, record)
        end
      end
    end
    
    def multi_workers_ready?
      true
    end

    def shutdown
      @bunny.close
      super
    end
  end
end
