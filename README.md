# fluent-plugin-rabbitmq

## Overview

This repository includes input/output plugins for RabbitMQ.

## Requirements

fluentd >= 0.14.0

## Installation

    $ fluent-gem install fluent-plugin-rabbitmq

## Testing

    $ rabbitmq-server
    $ rake test

## Configuration

### Input

```
<source>
  @type rabbitmq
  tag foo
  host 127.0.0.1
  # or hosts ["192.168.1.1", "192.168.1.2"]
  user guest
  pass guest
  vhost /
  exchange foo # not required. if specified, the queue will be bound to the exchange
  queue bar
  routing_key hoge # if not specified, the tag is used
  heartbeat 10 # integer as seconds or :server (interval specified by server)
  <parse>
    @type json # or msgpack, ltsv, none
  </parse>
</source>
```

#### Other Configurations for Input

|key|example|default value|description|
|:--|---|---|---|
|durable|true|false|set durable flag of the queue|
|exclusive|true|false|set exclusive flag of the queue|
|auto_delete|true|false|set auto_delete flag of the queue|
|ttl|60000|nil|queue ttl in ms|
|prefetch_count|10|nil||
|consumer_pool_size|5|nil||
|include_headers|true|false|include headers in events|
|headers_key|string|header|key name of headers|
|create_exchange|true|false|create exchange or not|
|exchange_to_bind|string|nil|exchange to bind created exchange|
|exchange_type|direct|topic|type of created exchange|
|exchange_routing_key|hoge|nil|created exchange routing key|
|exchange_durable|true|false|durability of create exchange|
|manual_ack|true|false|manual ACK|
|queue_mode|"lazy"|nil|queue mode|

### Output

```
<match pattern>
  @type rabbitmq
  host 127.0.0.1
  # or hosts ["192.168.1.1", "192.168.1.2"]
  user guest
  pass guest
  vhost /
  format json # or msgpack, ltsv, none
  exchange foo # required: name of exchange
  exchange_type fanout # required: type of exchange e.g. topic, direct
  exchange_durable false
  routing_key hoge # if not specified, the tag is used
  heartbeat 10 # integer as seconds or :server (interval specified by server)
  <format>
    @type json # or msgpack, ltsv, none
  </format>
  <buffer> # to use in buffered mode
  </buffer>
</match>
```

#### Other Configurations for Output

|key|example|default value|description|
|:--|---|---|---|
|persistent|true|false|messages is persistent to disk|
|timestamp|true|false|if true, time of record is used as timestamp in AMQP message|
|content_type|application/json|nil|message content type|
|frame_max|131072|nil|maximum permissible size of a frame|
|mandatory||true|nil||
|expiration|3600|nil|message time-to-live|
|message_type||nil||
|priority||nil||
|app_id||nil||
|id_key|message_id|nil|id to specify message_id|

### TLS related configurations

```
tls false # enable TLS or not
tls_cert /path/to/cert
tls_key /path/to/key
tls_ca_certificates ["/path/to/ca_certificate"]
verify_peer true
```

### Other Configurations for Input/Output

|key|example|default value|description|
|:--|---|---|---|
|automatically_recover|true|nil|automatic network failure recovery|
|network_recovery_interval|30|nil|interval between reconnection attempts|
|recovery_attempts|3|nil|limits the number of connection recovery|
|connection_timeout|30|nil||
|continuation_timeout|600|nil||

## License

The gem is available as open source under the terms of the [Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0.txt).

