package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
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
					&cli.IntFlag{
						Name:    "count",
						Aliases: []string{"c"},
						Value:   1,
						Usage:   "count of messages to publish",
					},
				},
			},
			{
				Name:    "subscribe",
				Aliases: []string{"s"},
				Usage:   "complete a task on the list",
				Action: func(cCtx *cli.Context) error {
					return subscribe(cCtx.String("servers"), cCtx.String("tlsca"), cCtx.Bool("consumer"))
				},
				Flags: []cli.Flag{
					serverFlag,
					tlscaFlag,
					&cli.BoolFlag{
						Name:  "consumer",
						Usage: "create a consumer",
					},
				},
			},
			{
				Name:  "request",
				Usage: "send requests",
				Action: func(cCtx *cli.Context) error {
					return request(cCtx.String("servers"), cCtx.String("tlsca"), cCtx.String("subject"), cCtx.Int("count"))
				},
				Flags: []cli.Flag{
					serverFlag,
					tlscaFlag,
					&cli.StringFlag{
						Name:     "subject",
						Required: true,
						Usage:    "subject of queue",
					},
					&cli.IntFlag{
						Name:    "count",
						Aliases: []string{"c"},
						Value:   1,
						Usage:   "count of messages to send",
					},
				},
			},
			{
				Name:  "reply",
				Usage: "send replies",
				Action: func(cCtx *cli.Context) error {
					return reply(cCtx.String("servers"), cCtx.String("tlsca"), cCtx.String("subject"), cCtx.String("queue"))
				},
				Flags: []cli.Flag{
					serverFlag,
					tlscaFlag,
					&cli.StringFlag{
						Name:     "subject",
						Required: true,
						Usage:    "subject of queue",
					},
					&cli.StringFlag{
						Name:    "queue",
						Aliases: []string{"q"},
						Usage:   "queue group name",
					},
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

	_, err = js.AddStream(&nats.StreamConfig{
		Name:       "test",
		Storage:    nats.MemoryStorage,
		Subjects:   []string{"test.>"},
		Duplicates: time.Minute,
	})
	if err != nil {
		return err
	}

	// Publish some messages with duplicates.
	for i := 0; i < count; i++ {
		now := time.Now()
		nowStr := now.Format(time.RFC3339Nano)
		msgData := "hello at " + nowStr
		if ack, err := js.Publish("test.1", []byte(msgData), nats.MsgId(nowStr)); err != nil {
			return err
		} else {
			fmt.Printf("published msg.Data=%s, msg.ID=%s, ack=%+v\n", msgData, nowStr, ack)
		}
	}

	return nil
}

func subscribe(servers, tlsca string, createsConsumer bool) error {
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

	if createsConsumer {
		_, err = js.AddConsumer("test", &nats.ConsumerConfig{
			Durable:       "test",
			AckPolicy:     nats.AckExplicitPolicy,
			DeliverPolicy: nats.DeliverAllPolicy,
		})
		if err != nil {
			return err
		}
		defer js.DeleteConsumer("test", "test")
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
			fmt.Printf("sent ack for msg.Data=%s, id=%s\n", string(msg.Data), msg.Header.Get("Nats-Msg-Id"))
		}
	}

	return nil
}

func request(servers, tlsca, subject string, count int) error {
	fmt.Println("request subcommand called: severs=", servers, ", tlscert=", tlsca)
	nc, err := connect(servers, tlsca)
	if err != nil {
		return err
	}
	defer nc.Close()

	fmt.Println("connected")

	for i := 0; i < count; i++ {
		now := time.Now()
		nowStr := now.Format(time.RFC3339Nano)
		payload := "help me at " + nowStr
		msg, err := nc.Request(subject, []byte(payload), 2*time.Second)
		if err != nil {
			if nc.LastError() != nil {
				log.Fatalf("%v for request", nc.LastError())
			}
			log.Fatalf("%v for request", err)
		}
		log.Printf("Published [%s] : '%s'", subject, payload)
		log.Printf("Received  [%v] : '%s'", msg.Subject, string(msg.Data))
	}

	return nil
}

func reply(servers, tlsca, subject, queueName string) error {
	fmt.Println("reply subcommand called: severs=", servers, ", tlscert=", tlsca)
	nc, err := connect(servers, tlsca)
	if err != nil {
		return err
	}
	defer nc.Close()

	fmt.Println("connected")

	i := 0
	_, err = nc.QueueSubscribe(subject, queueName, func(msg *nats.Msg) {
		i++
		msgData := string(msg.Data)
		log.Printf("[#%d] Received on [%s]: '%s'\n", i, msg.Subject, msgData)
		now := time.Now()
		nowStr := now.Format(time.RFC3339Nano)
		reply := "reply at " + nowStr + " for " + msgData
		msg.Respond([]byte(reply))
	})
	if err != nil {
		return fmt.Errorf("subscribe queue: %s", err)
	}
	if err = nc.Flush(); err != nil {
		return fmt.Errorf("flush: %s", err)
	}

	log.Printf("Listening on [%s]", subject)

	// Setup the interrupt handler to drain so we don't miss
	// requests when scaling down.
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	<-c
	log.Printf("Draining...")
	if err = nc.Drain(); err != nil {
		log.Printf("drain: %s", err)
	}
	log.Printf("Exiting")
	return nil
}

func connect(servers, tlsca string) (*nats.Conn, error) {
	var opts []nats.Option
	if tlsca != "" {
		log.Printf("tlsca options is not implemented yet")
	}
	return nats.Connect(servers, opts...)
}
