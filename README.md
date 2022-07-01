nats-stream-example
===================

An example for NATS Jetstream.

Based on https://stackoverflow.com/a/72815187/1391518

## My experiment result

```
hnakamur@thinkcentre2:~/ghq/github.com/nats-io/nats-server$ git log -1
commit 8a94e14fe71703cb8a7ef5d7c2cec00fb68040db (HEAD -> main, origin/main, origin/HEAD)
Author: Derek Collison <derek@nats.io>
Date:   Thu Jun 30 07:42:11 2022

    Bump to 2.9.0-beta.1

    Signed-off-by: Derek Collison <derek@nats.io>
```

1. Run the server.

```
$ ./nats-server -js
```


2. Publish a message.

```
$ ./nats-stream-example publish
publish subcommand called: severs= 127.0.0.1:4222 , tlscert=
connected
published msg.Data=hello at 2022-07-01T13:21:01.371962063+09:00, msg.ID=2022-07-01T13:21:01.371962063+09:00, ack=&{Stream:test Sequence:1 Duplicate:false Domain:}
```

3. Receive a message with adding a consumer.

```
$ ./nats-stream-example subscribe -consumer
subscribe subcommand called: severs= 127.0.0.1:4222 , tlscert=
connected
2022/07/01 13:21:28 1 messages
sent ack for msg.Data=hello at 2022-07-01T13:21:01.371962063+09:00, id=2022-07-01T13:21:01.371962063+09:00
```

4. Receive again the same message above.

```
$ ./nats-stream-example subscribe -consumer
subscribe subcommand called: severs= 127.0.0.1:4222 , tlscert=
connected
2022/07/01 13:21:30 1 messages
sent ack for msg.Data=hello at 2022-07-01T13:21:01.371962063+09:00, id=2022-07-01T13:21:01.371962063+09:00
```

5. Receive a message without adding a consumer.

```
$ ./nats-stream-example subscribe
subscribe subcommand called: severs= 127.0.0.1:4222 , tlscert=
connected
2022/07/01 13:27:40 1 messages
sent ack for msg.Data=hello at 2022-07-01T13:21:01.371962063+09:00, id=2022-07-01T13:21:01.371962063+09:00
```

6. This time, no message are received on the second run.

```
$ ./nats-stream-example subscribe
subscribe subcommand called: severs= 127.0.0.1:4222 , tlscert=
connected
2022/07/01 13:27:48 nats: timeout
```

Use nats CLI to remove the test stream:

```
$ nats stream rm --force test
```
