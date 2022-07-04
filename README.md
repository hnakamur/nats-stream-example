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
```

3. Create a consumer

```
$ ./nats-stream-example consumer-add --json --consumer pull_consumer2 --stream my_stream2
```

4. Publish some messages into the stream.

```
$ ./nats-stream-example ^Cblish --subject foo2 --count 100
```

It prints the following output:

```
2022/07/04 13:08:06 i=0, published msg.Data=hello 0 at 2022-07-04T13:08:06.558232222+09:00
2022/07/04 13:08:07 i=1, published msg.Data=hello 1 at 2022-07-04T13:08:07.55895319+09:00
2022/07/04 13:08:08 i=2, published msg.Data=hello 2 at 2022-07-04T13:08:08.55968631+09:00
2022/07/04 13:08:09 i=3, published msg.Data=hello 3 at 2022-07-04T13:08:09.560974119+09:00
2022/07/04 13:08:10 i=4, published msg.Data=hello 4 at 2022-07-04T13:08:10.561926754+09:00
2022/07/04 13:08:11 i=5, published msg.Data=hello 5 at 2022-07-04T13:08:11.562620345+09:00
...
```

While the above command is running, open another terminal and do the following step.

5. Receive messages

```
$ ./nats-stream-example consumer-next --stream my_stream2 --consumer pull_consumer2 --count 100
```

It prints the following output:

```
2022/07/04 13:08:08 i=0
[13:08:08] subj: foo2 / tries: 1 / cons seq: 1 / str seq: 1 / pending: 2
hello 0 at 2022-07-04T13:08:06.558232222+09:00

2022/07/04 13:08:08 i=1
[13:08:08] subj: foo2 / tries: 1 / cons seq: 2 / str seq: 2 / pending: 1
hello 1 at 2022-07-04T13:08:07.55895319+09:00

2022/07/04 13:08:08 i=2
[13:08:08] subj: foo2 / tries: 1 / cons seq: 3 / str seq: 3 / pending: 0
hello 2 at 2022-07-04T13:08:08.55968631+09:00

2022/07/04 13:08:08 i=3
[13:08:09] subj: foo2 / tries: 1 / cons seq: 4 / str seq: 4 / pending: 0
hello 3 at 2022-07-04T13:08:09.560974119+09:00

2022/07/04 13:08:09 i=4
[13:08:10] subj: foo2 / tries: 1 / cons seq: 5 / str seq: 5 / pending: 0
hello 4 at 2022-07-04T13:08:10.561926754+09:00

2022/07/04 13:08:10 i=5
[13:08:11] subj: foo2 / tries: 1 / cons seq: 6 / str seq: 6 / pending: 0
hello 5 at 2022-07-04T13:08:11.562620345+09:00
...
```
