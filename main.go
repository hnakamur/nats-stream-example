package main

import (
	"errors"
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
		Value:   "",
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
				Name:  "stream-add",
				Usage: "Add a stream",
				Action: func(cCtx *cli.Context) error {
					return streamAdd(cCtx.String("servers"), cCtx.String("tlsca"), cCtx.String("stream"), cCtx.String("subject"))
				},
				Flags: []cli.Flag{
					serverFlag,
					tlscaFlag,
					&cli.StringFlag{
						Name:     "stream",
						Required: true,
						Usage:    "stream name to create",
					},
					&cli.StringFlag{
						Name:     "subject",
						Required: true,
						Usage:    "subject to consume",
					},
				},
			},
			{
				Name:    "publish",
				Aliases: []string{"p"},
				Usage:   "Publish items",
				Action: func(cCtx *cli.Context) error {
					return publish(cCtx.String("servers"), cCtx.String("tlsca"), cCtx.String("subject"), cCtx.Int("count"))
				},
				Flags: []cli.Flag{
					serverFlag,
					tlscaFlag,
					&cli.StringFlag{
						Name:     "subject",
						Required: true,
						Usage:    "subject to publish to",
					},
					&cli.IntFlag{
						Name:    "count",
						Aliases: []string{"c"},
						Value:   1,
						Usage:   "count of messages to publish",
					},
				},
			},
			{
				Name:  "consumer-add",
				Usage: "Add a consumer",
				Action: func(cCtx *cli.Context) error {
					return consumerAdd(cCtx.String("servers"), cCtx.String("tlsca"), cCtx.String("consumer"), cCtx.String("stream"))
				},
				Flags: []cli.Flag{
					serverFlag,
					tlscaFlag,
					&cli.StringFlag{
						Name:     "stream",
						Required: true,
						Usage:    "stream name",
					},
					&cli.StringFlag{
						Name:     "consumer",
						Required: true,
						Usage:    "consumer name",
					},
				},
			},
			{
				Name:    "consumer-next",
				Aliases: []string{"c"},
				Usage:   "subscribe from consumer",
				Action: func(cCtx *cli.Context) error {
					return consumerNext(cCtx.String("servers"), cCtx.String("tlsca"), cCtx.String("stream"), cCtx.String("consumer"), cCtx.Int("count"))
				},
				Flags: []cli.Flag{
					serverFlag,
					tlscaFlag,
					&cli.StringFlag{
						Name:     "stream",
						Required: true,
						Usage:    "stream name",
					},
					&cli.StringFlag{
						Name:     "consumer",
						Required: true,
						Usage:    "consumer name",
					},
					&cli.IntFlag{
						Name:    "count",
						Aliases: []string{"c"},
						Value:   1,
						Usage:   "count of messages to consume",
					},
				},
			},
		},
	}

	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}

func streamAdd(servers, tlsca, streamName, subject string) error {
	nc, err := connect(servers, tlsca)
	if err != nil {
		return err
	}
	defer nc.Close()

	js, err := nc.JetStream()
	if err != nil {
		return err
	}

	streamCfg := &nats.StreamConfig{
		Name:       streamName,
		Subjects:   []string{subject},
		Storage:    nats.FileStorage,
		Retention:  nats.LimitsPolicy,
		Discard:    nats.DiscardOld,
		Duplicates: 2 * time.Minute,
		Replicas:   1,
	}
	_, err = js.AddStream(streamCfg)
	if err != nil {
		return err
	}
	return nil
}

func publish(servers, tlsca, subject string, count int) error {
	nc, err := connect(servers, tlsca)
	if err != nil {
		return err
	}
	defer nc.Close()

	for i := 0; i < count; i++ {
		if i > 0 {
			time.Sleep(time.Second)
		}

		msg := nats.NewMsg(subject)
		now := time.Now()
		msg.Data = []byte(fmt.Sprintf("hello %d at %s", i, now.Format(time.RFC3339Nano)))
		if err := nc.PublishMsg(msg); err != nil {
			return err
		}
		if err := nc.Flush(); err != nil {
			return err
		}
		if err := nc.LastError(); err != nil {
			return err
		}
		log.Printf("i=%d, published msg.Data=%s", i, string(msg.Data))
	}

	return nil
}

func consumerAdd(servers, tlsca, consumerName, streamName string) error {
	nc, err := connect(servers, tlsca)
	if err != nil {
		return err
	}
	defer nc.Close()

	js, err := nc.JetStream()
	if err != nil {
		return err
	}

	consumerCfg := &nats.ConsumerConfig{
		Durable:       consumerName,
		DeliverPolicy: nats.DeliverAllPolicy,
		ReplayPolicy:  nats.ReplayInstantPolicy,
		AckPolicy:     nats.AckExplicitPolicy,
	}
	_, err = js.AddConsumer(streamName, consumerCfg)
	if err != nil {
		return err
	}
	return nil
}

func consumerNext(servers, tlsca, streamName, consumerName string, count int) error {
	nc, err := connect(servers, tlsca)
	if err != nil {
		return err
	}
	defer nc.Close()

	js, err := nc.JetStream()
	if err != nil {
		return err
	}

	for i := 0; i < count; i++ {
		log.Printf("i=%d", i)
		if err := getNextMsgDirect(nc, js, streamName, consumerName); err != nil {
			return err
		}
	}

	return nil
}

func getNextMsgDirect(nc *nats.Conn, js nats.JetStreamContext, stream, consumer string) error {
	sub, err := js.PullSubscribe("", consumer, nats.BindStream(stream))
	if err != nil {
		return err
	}
	sub.AutoUnsubscribe(1)

	batch := 1
	timeout := 5 * time.Second
	msgs, err := sub.Fetch(batch, nats.MaxWait(timeout))
	if err != nil {
		return err
	}
	if len(msgs) != 1 {
		return fmt.Errorf("unexpected count of messaged fetched: %d", len(msgs))
	}

	msg := msgs[0]

	if msg.Header != nil && msg.Header.Get("Status") == "503" {
		return errors.New("got 503 Status in msg header")
	}

	metadata, err := msg.Metadata()
	if err != nil {
		if msg.Reply == "" {
			fmt.Printf("--- subject: %s\n", msg.Subject)
		} else {
			fmt.Printf("--- subject: %s reply: %s\n", msg.Subject, msg.Reply)
		}
		return err
	}
	fmt.Printf("[%s] subj: %s / tries: %d / cons seq: %d / str seq: %d / pending: %d\n", time.Now().Format("15:04:05"), msg.Subject, metadata.NumDelivered, metadata.Sequence.Consumer, metadata.Sequence.Stream, metadata.NumPending)
	if len(msg.Header) > 0 {
		fmt.Println("Headers:")
		for h, vals := range msg.Header {
			for _, val := range vals {
				fmt.Printf("  %s: %s\n", h, val)
			}
		}

		fmt.Println()
		fmt.Println("Data:")
	}

	fmt.Println(string(msg.Data))
	fmt.Println()

	if err := msg.Respond(nil); err != nil {
		return err
	}
	if err := nc.Flush(); err != nil {
		return err
	}
	return nil
}

func connect(servers, tlsca string, opts ...nats.Option) (*nats.Conn, error) {
	if tlsca != "" {
		log.Printf("tlsca options is not implemented yet")
	}
	return nats.Connect(servers, opts...)
}
