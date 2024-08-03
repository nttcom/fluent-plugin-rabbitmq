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
require "fluent/plugin/output"

module Fluent::Plugin
  class RabbitMQOutput < Output
    Fluent::Plugin.register_output("rabbitmq", self)

    helpers :formatter, :inject, :compat_parameters

    config_section :format do
      config_set_default :@type, "json"
    end

    config_param :host, :string, default: nil
    config_param :hosts, :array, default: nil
    config_param :port, :integer, default: nil
    config_param :user, :string, default: nil
    config_param :pass, :string, default: nil, secret: true
    config_param :vhost, :string, default: nil
    config_param :connection_timeout, :time, default: nil
    config_param :continuation_timeout, :integer, default: nil
    config_param :automatically_recover, :bool, default: nil
    config_param :network_recovery_interval, :time, default: nil
    config_param :recovery_attempts, :integer, default: nil
    config_param :auth_mechanism, :string, default: nil
    config_param :heartbeat, default: nil do |param|
      param == "server" ? :server : Integer(param)
    end
    config_param :frame_max, :integer, default: nil

    config_param :tls, :bool, default: false
    config_param :tls_cert, :string, default: nil
    config_param :tls_key, :string, default: nil
    config_param :tls_ca_certificates, :array, default: nil
    config_param :verify_peer, :bool, default: true

    config_param :exchange, :string
    config_param :exchange_type, :string
    config_param :exchange_durable, :bool, default: false

    config_param :persistent, :bool, default: false
    config_param :routing_key, :string, default: nil    
    config_param :id_key, :string, default: nil
    config_param :timestamp, :bool, default: false
    config_param :content_type, :string, default: nil
    config_param :content_encoding, :string, default: nil
    config_param :expiration, :integer, default: nil
    config_param :message_type, :string, default: nil
    config_param :priority, :integer, default: nil
    config_param :app_id, :string, default: nil

    def initialize
      super
      require "bunny"
    end

    def configure(conf)
      compat_parameters_convert(conf, :inject, :formatter, default_chunk_key: "time")

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
      bunny_options[:frame_max] = @frame_max if @frame_max

      bunny_options[:tls] = @tls
      bunny_options[:tls_cert] = @tls_cert if @tls_cert
      bunny_options[:tls_key] = @tls_key if @tls_key
      bunny_options[:tls_ca_certificates] = @tls_ca_certificates if @tls_ca_certificates
      bunny_options[:verify_peer] = @verify_peer

      @bunny = Bunny.new(bunny_options)

      @publish_options = {}
      @publish_options[:content_type] = @content_type if @content_type
      @publish_options[:content_encoding] = @content_encoding if @content_encoding
      @publish_options[:persistent] = @persistent if @persistent
      @publish_options[:mandatory] = @mandatory if @mandatory
      @publish_options[:expiration] = @expiration if @expiration
      @publish_options[:type] = @message_type if @message_type
      @publish_options[:priority] = @priority if @priority
      @publish_options[:app_id] = @app_id if @app_id

      @formatter = formatter_create(default_type: @type)
    end

    def multi_workers_ready?
      true
    end

    def prefer_buffered_processing
      false
    end

    def start
      super
      @bunny.start
      @channel = @bunny.create_channel
      exchange_options = {
        durable: @exchange_durable,
        auto_delete: @exchange_auto_delete
      }
      @bunny_exchange = Bunny::Exchange.new(@channel, @exchange_type, @exchange, exchange_options)
    end

    def shutdown
      @bunny.close
      super
    end

    def set_publish_options(tag, time, record)
      @publish_options[:timestamp] = time.to_i if @timestamp

      if @exchange_type != "fanout"
        @publish_options[:routing_key] = @routing_key || tag
      end

      if @id_key
        id = record[@id_key]
        @publish_options[:message_id] = id if id
      end
    end

    def process(tag, es)
      es.each do |time, record|
        set_publish_options(tag, time, record)
        record = inject_values_to_record(tag, time, record)
        buf = @formatter.format(tag, time, record)
        @bunny_exchange.publish(buf, @publish_options)
      end
    end

    def write(chunk)
      tag = chunk.metadata.tag

      chunk.each do |time, record|
        set_publish_options(tag, time, record)
        record = inject_values_to_record(tag, time, record)
        buf = @formatter.format(tag, time, record)
        @bunny_exchange.publish(buf, @publish_options)
      end
    end
  end
end
