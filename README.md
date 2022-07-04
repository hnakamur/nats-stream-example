nats-stream-example
===================

An example for [JetStream Walkthrough - NATS Docs](https://docs.nats.io/nats-concepts/jetstream/js_walkthrough).

Based on a lot of code copied from [nats-io/natscli: The NATS Command Line Interface](https://github.com/nats-io/natscli).

## My experiment result

```
$ for s in nats-server natscli nats.go jsm.go; do echo $s $(cd $s; git rev-parse HEAD); done
nats-server 7cf814de8e096d2d156b2d9293e25c80c77fbb37
natscli 84c91a5d58776ad102a5d46851d8db07e237533c
nats.go d29a40a9c70386e2373337a5a3192eacc4f4f4e9
jsm.go 24c9fcc2ceb9d44c1eb03535ff5e2e2c5517fd0e
```

1. Run the server.

```
$ ./nats-server -js
```


2. Create a stream.

```
$ ./nats-stream-example stream-add --json --stream my_stream2 --subject foo2
streamAdd subcommand called: severs=  , tlscert= 
connected
{
  "config": {
    "name": "my_stream2",
    "subjects": [
      "foo2"
    ],
    "retention": "limits",
    "max_consumers": -1,
    "max_msgs_per_subject": -1,
    "max_msgs": -1,
    "max_bytes": -1,
    "max_age": 0,
    "max_msg_size": -1,
    "storage": "file",
    "discard": "old",
    "num_replicas": 1,
    "duplicate_window": 120000000000,
    "sealed": false,
    "deny_delete": false,
    "deny_purge": false,
    "allow_rollup_hdrs": false
  },
  "created": "2022-07-04T03:10:20.787006041Z",
  "state": {
    "messages": 0,
    "bytes": 0,
    "first_seq": 0,
    "first_ts": "0001-01-01T00:00:00Z",
    "last_seq": 0,
    "last_ts": "0001-01-01T00:00:00Z",
    "consumer_count": 0
  }
}
```

3. Create a consumer

```
$ ./nats-stream-example consumer-add --json --consumer pull_consumer2 --stream my_stream2
consumerAdd subcommand called: severs=  , tlscert= 
connected
{
  "stream_name": "my_stream2",
  "name": "pull_consumer2",
  "config": {
    "ack_policy": "explicit",
    "ack_wait": 30000000000,
    "deliver_policy": "all",
    "durable_name": "pull_consumer2",
    "max_ack_pending": 1000,
    "max_deliver": -1,
    "max_waiting": 512,
    "replay_policy": "instant",
    "num_replicas": 0
  },
  "created": "2022-07-04T03:13:05.428592737Z",
  "delivered": {
    "consumer_seq": 0,
    "stream_seq": 0
  },
  "ack_floor": {
    "consumer_seq": 0,
    "stream_seq": 0
  },
  "num_ack_pending": 0,
  "num_redelivered": 0,
  "num_waiting": 0,
  "num_pending": 0,
  "cluster": {
    "leader": "NBWM3EFUV6RAGIY7UIH2DQPK6JWQREVYSW6YP7EDPEOKY25IKTHRIDB6"
  }
}
```

4. Publish some messages into the stream.

```
$ ./nats-stream-example ^Cblish --subject foo2 --count 100
```

It prints the following output:

```
publish subcommand called: severs=  , tlscert= 
connected
2022/07/04 12:45:49 i=0, published msg.Data=hello 0 at 2022-07-04T12:45:49.539079765+09:00
2022/07/04 12:45:50 i=1, published msg.Data=hello 1 at 2022-07-04T12:45:50.53993924+09:00
2022/07/04 12:45:51 i=2, published msg.Data=hello 2 at 2022-07-04T12:45:51.540850747+09:00
2022/07/04 12:45:52 i=3, published msg.Data=hello 3 at 2022-07-04T12:45:52.541800947+09:00
2022/07/04 12:45:53 i=4, published msg.Data=hello 4 at 2022-07-04T12:45:53.542669292+09:00
2022/07/04 12:45:54 i=5, published msg.Data=hello 5 at 2022-07-04T12:45:54.543537917+09:00
...
```

While the above command is running, open another terminal and do the following step.

5. Receive messages

```
$ ./nats-stream-example consumer-next --stream my_stream2 --consumer pull_consumer2 --count 100
```

It prints the following output:

```
consumerNext subcommand called: severs=  , tlscert= 
connected
2022/07/04 12:45:51 i=0
[12:45:51] subj: foo2 / tries: 1 / cons seq: 1 / str seq: 1 / pending: 1

hello 0 at 2022-07-04T12:45:49.539079765+09:00
2022/07/04 12:45:51 i=1
[12:45:51] subj: foo2 / tries: 1 / cons seq: 2 / str seq: 2 / pending: 0

hello 1 at 2022-07-04T12:45:50.53993924+09:00
2022/07/04 12:45:51 i=2
[12:45:51] subj: foo2 / tries: 1 / cons seq: 3 / str seq: 3 / pending: 0

hello 2 at 2022-07-04T12:45:51.540850747+09:00
2022/07/04 12:45:51 i=3
[12:45:52] subj: foo2 / tries: 1 / cons seq: 4 / str seq: 4 / pending: 0

hello 3 at 2022-07-04T12:45:52.541800947+09:00
2022/07/04 12:45:52 i=4
[12:45:53] subj: foo2 / tries: 1 / cons seq: 5 / str seq: 5 / pending: 0

hello 4 at 2022-07-04T12:45:53.542669292+09:00
2022/07/04 12:45:53 i=5
[12:45:54] subj: foo2 / tries: 1 / cons seq: 6 / str seq: 6 / pending: 0

hello 5 at 2022-07-04T12:45:54.543537917+09:00
```
