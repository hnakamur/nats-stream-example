package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/urfave/cli/v2"
)

func main() {
	cli.VersionFlag = &cli.BoolFlag{
		Name:    "version",
		Aliases: []string{"V"},
		Usage:   "print only the version",
	}

	serverFlag := &cli.StringFlag{
		Name:    "servers",
		Aliases: []string{"s"},
		Value:   "127.0.0.1:4222",
		Usage:   "comma separated values of server URLs",
		EnvVars: []string{"SERVERS"},
	}
	tlscaFlag := &cli.StringFlag{
		Name:    "tlsca",
		Aliases: []string{"a"},
		Usage:   "TLS CA certificate path",
		EnvVars: []string{"TLSCA"},
	}

	app := &cli.App{
		Name:    "nats-stream-example",
		Version: "0.0.1",
		Usage:   "A stream example for NATS",
		Commands: []*cli.Command{
			{
				Name:    "publish",
				Aliases: []string{"p"},
				Usage:   "Publish items",
				Action: func(cCtx *cli.Context) error {
					return publish(cCtx.String("servers"), cCtx.String("tlsca"), cCtx.Int("count"))
				},
				Flags: []cli.Flag{
					serverFlag,
					tlscaFlag,
					&cli.StringFlag{
						Name:    "count",
						Aliases: []string{"c"},
						Usage:   "count of messages to publish",
					},
				},
			},
			{
				Name:    "subscribe",
				Aliases: []string{"s"},
				Usage:   "complete a task on the list",
				Action: func(cCtx *cli.Context) error {
					return subscribe(cCtx.String("servers"), cCtx.String("tlsca"))
				},
				Flags: []cli.Flag{
					serverFlag,
					tlscaFlag,
				},
			},
		},
	}

	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}

func publish(servers, tlsca string, count int) error {
	fmt.Println("publish subcommand called: severs=", servers, ", tlscert=", tlsca)
	nc, err := connect(servers, tlsca)
	if err != nil {
		return err
	}
	defer nc.Close()

	fmt.Println("connected")

	js, err := nc.JetStream(nats.PublishAsyncMaxPending(256), nats.MaxWait(5*time.Second))
	if err != nil {
		return err
	}

	stream, err := js.AddStream(&nats.StreamConfig{
		Name:       "test",
		Storage:    nats.MemoryStorage,
		Subjects:   []string{"test.>"},
		Duplicates: time.Minute,
	})
	if err != nil {
		return err
	}
	fmt.Printf("streamInfo=%+v\n", stream)

	// Publish some messages with duplicates.
	if ack, err := js.Publish("test.1", []byte("hello"), nats.MsgId("1")); err != nil {
		return err
	} else {
		fmt.Printf("ack=%+v\n", ack)
	}
	if ack, err := js.Publish("test.2", []byte("hello"), nats.MsgId("2")); err != nil {
		return err
	} else {
		fmt.Printf("ack=%+v\n", ack)
	}
	if ack, err := js.Publish("test.1", []byte("hello"), nats.MsgId("1")); err != nil {
		return err
	} else {
		fmt.Printf("ack=%+v\n", ack)
	}
	if ack, err := js.Publish("test.2", []byte("hello"), nats.MsgId("2")); err != nil {
		return err
	} else {
		fmt.Printf("ack=%+v\n", ack)
	}
	if ack, err := js.Publish("test.2", []byte("hello"), nats.MsgId("2")); err != nil {
		return err
	} else {
		fmt.Printf("ack=%+v\n", ack)
	}

	return nil
}

func subscribe(servers, tlsca string) error {
	fmt.Println("subscribe subcommand called: severs=", servers, ", tlscert=", tlsca)

	nc, err := connect(servers, tlsca)
	if err != nil {
		return err
	}
	defer nc.Close()

	fmt.Println("connected")

	js, err := nc.JetStream(nats.PublishAsyncMaxPending(256), nats.MaxWait(5*time.Second))
	if err != nil {
		return err
	}

	sub, err := js.PullSubscribe("", "test", nats.BindStream("test"))
	if err != nil {
		return err
	}
	// AckSync both to ensure the server received the ack.
	msgs, err := sub.Fetch(10)
	if err != nil {
		return err
	}
	log.Printf("%d messages", len(msgs))
	for _, msg := range msgs {
		if err := msg.AckSync(); err != nil {
			return fmt.Errorf("send ack: %e", err)
		} else {
			fmt.Printf("sent ack for msg=%+v\n", msg)
		}
	}

	return nil
}

func connect(servers, tlsca string) (*nats.Conn, error) {
	var opts []nats.Option
	// if tlsca != "" {
	// 	opts = append(opts, natscontext.WithCA(tlsca))
	// }
	return nats.Connect(servers, opts...)
}
