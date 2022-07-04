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
$ ./nats-stream-example stream-add --stream my_stream2 --subject foo2
```

3. Create a consumer

```
$ ./nats-stream-example consumer-add --consumer pull_consumer2 --stream my_stream2
```

4. Publish some messages into the stream.

```
$ ./nats-stream-example publish --subject foo2 --count 100
```

It prints the following output:

```
2022/07/04 14:03:14 i=0, published msg.Data=hello 0 at 2022-07-04T14:03:14.034590226+09:00
2022/07/04 14:03:15 i=1, published msg.Data=hello 1 at 2022-07-04T14:03:15.035388189+09:00
2022/07/04 14:03:16 i=2, published msg.Data=hello 2 at 2022-07-04T14:03:16.036276313+09:00
2022/07/04 14:03:17 i=3, published msg.Data=hello 3 at 2022-07-04T14:03:17.037197884+09:00
2022/07/04 14:03:18 i=4, published msg.Data=hello 4 at 2022-07-04T14:03:18.038115187+09:00
2022/07/04 14:03:19 i=5, published msg.Data=hello 5 at 2022-07-04T14:03:19.039166581+09:00
...
```

While the above command is running, open another terminal and do the following step.

5. Receive messages

```
$ ./nats-stream-example consumer-next --stream my_stream2 --consumer pull_consumer2 --count 100
```

It prints the following output:

```
2022/07/04 14:03:15 i=0
[14:03:15] subj: foo2 / tries: 1 / cons seq: 1 / str seq: 1 / pending: 1
hello 0 at 2022-07-04T14:03:14.034590226+09:00

2022/07/04 14:03:15 i=1
[14:03:15] subj: foo2 / tries: 1 / cons seq: 2 / str seq: 2 / pending: 0
hello 1 at 2022-07-04T14:03:15.035388189+09:00

2022/07/04 14:03:15 i=2
[14:03:16] subj: foo2 / tries: 1 / cons seq: 3 / str seq: 3 / pending: 0
hello 2 at 2022-07-04T14:03:16.036276313+09:00

2022/07/04 14:03:16 i=3
[14:03:17] subj: foo2 / tries: 1 / cons seq: 4 / str seq: 4 / pending: 0
hello 3 at 2022-07-04T14:03:17.037197884+09:00

2022/07/04 14:03:17 i=4
[14:03:18] subj: foo2 / tries: 1 / cons seq: 5 / str seq: 5 / pending: 0
hello 4 at 2022-07-04T14:03:18.038115187+09:00

2022/07/04 14:03:18 i=5
[14:03:19] subj: foo2 / tries: 1 / cons seq: 6 / str seq: 6 / pending: 0
hello 5 at 2022-07-04T14:03:19.039166581+09:00

...
```
