package main

import (
	"fmt"
	"log"
	"math"
	"os"
	"os/signal"
	"sort"
	"strings"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/nats-io/jsm.go"
	"github.com/nats-io/jsm.go/api"
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

	streamCfg := &nats.StreamConfig{
		Name:       "my_stream",
		Subjects:   []string{"foo"},
		Storage:    nats.FileStorage,
		Retention:  nats.LimitsPolicy,
		Discard:    nats.DiscardOld,
		Duplicates: 2 * time.Minute,
		Replicas:   1,
	}
	streamInfo, err := js.AddStream(streamCfg)
	if err != nil {
		return err
	}
	showStreamInfo(streamInfo)

	// Publish some messages with duplicates.
	for i := 0; i < count; i++ {
		msg := nats.NewMsg(streamCfg.Name)
		now := time.Now()
		nowStr := now.Format(time.RFC3339Nano)
		msg.Data = []byte("hello at " + nowStr)
		log.Printf("before publish, i=%d", i)
		if err := nc.PublishMsg(msg); err != nil {
			return err
		}
		log.Printf("after publish, i=%d", i)
		if err := nc.Flush(); err != nil {
			return err
		}
		log.Printf("after flush, i=%d", i)
		// if ack, err := js.Publish("foo", []byte(msgData), nats.MsgId(nowStr)); err != nil {
		// 	return err
		// } else {
		// 	fmt.Printf("published msg.Data=%s, msg.ID=%s, ack=%+v\n", msgData, nowStr, ack)
		// }
	}

	return nil
}

func showStreamInfo(info *nats.StreamInfo) {
	fmt.Printf("Information for Stream %s created %s\n", info.Config.Name, info.Created.Local().Format("2006-01-02 15:04:05"))
	fmt.Println()
	showStreamConfig(info.Config)
	fmt.Println()

	if info.Cluster != nil && info.Cluster.Name != "" {
		fmt.Println("Cluster Information:")
		fmt.Println()
		fmt.Printf("                 Name: %s\n", info.Cluster.Name)
		fmt.Printf("               Leader: %s\n", info.Cluster.Leader)
		for _, r := range info.Cluster.Replicas {
			state := []string{r.Name}

			if r.Current {
				state = append(state, "current")
			} else {
				state = append(state, "outdated")
			}

			if r.Offline {
				state = append(state, "OFFLINE")
			}

			if r.Active > 0 && r.Active < math.MaxInt64 {
				state = append(state, fmt.Sprintf("seen %s ago", humanizeDuration(r.Active)))
			} else {
				state = append(state, "not seen")
			}

			switch {
			case r.Lag > 1:
				state = append(state, fmt.Sprintf("%s operations behind", humanize.Comma(int64(r.Lag))))
			case r.Lag == 1:
				state = append(state, fmt.Sprintf("%d operation behind", r.Lag))
			}

			fmt.Printf("              Replica: %s\n", strings.Join(state, ", "))

		}
		fmt.Println()
	}

	showSource := func(s *nats.StreamSourceInfo) {
		fmt.Printf("          Stream Name: %s\n", s.Name)
		fmt.Printf("                  Lag: %s\n", humanize.Comma(int64(s.Lag)))
		if s.Active > 0 && s.Active < math.MaxInt64 {
			fmt.Printf("            Last Seen: %v\n", humanizeDuration(s.Active))
		} else {
			fmt.Printf("            Last Seen: never\n")
		}
	}

	if len(info.Sources) > 0 {
		fmt.Println("Source Information:")
		fmt.Println()
		for _, s := range info.Sources {
			showSource(s)
			fmt.Println()
		}
	}

	fmt.Println("State:")
	fmt.Println()
	fmt.Printf("             Messages: %s\n", humanize.Comma(int64(info.State.Msgs)))
	fmt.Printf("                Bytes: %s\n", humanize.IBytes(info.State.Bytes))

	if info.State.FirstTime.Equal(time.Unix(0, 0)) || info.State.LastTime.IsZero() {
		fmt.Printf("             FirstSeq: %s\n", humanize.Comma(int64(info.State.FirstSeq)))
	} else {
		fmt.Printf("             FirstSeq: %s @ %s UTC\n", humanize.Comma(int64(info.State.FirstSeq)), info.State.FirstTime.Format("2006-01-02T15:04:05"))
	}

	if info.State.LastTime.Equal(time.Unix(0, 0)) || info.State.LastTime.IsZero() {
		fmt.Printf("              LastSeq: %s\n", humanize.Comma(int64(info.State.LastSeq)))
	} else {
		fmt.Printf("              LastSeq: %s @ %s UTC\n", humanize.Comma(int64(info.State.LastSeq)), info.State.LastTime.Format("2006-01-02T15:04:05"))
	}

	fmt.Printf("     Active Consumers: %d\n", info.State.Consumers)
}

func showStreamConfig(cfg nats.StreamConfig) {
	fmt.Println("Configuration:")
	fmt.Println()
	if cfg.Description != "" {
		fmt.Printf("          Description: %s\n", cfg.Description)
	}
	if len(cfg.Subjects) > 0 {
		fmt.Printf("             Subjects: %s\n", strings.Join(cfg.Subjects, ", "))
	}
	if cfg.Sealed {
		fmt.Printf("               Sealed: true\n")
	}
	fmt.Printf("     Acknowledgements: %v\n", !cfg.NoAck)
	fmt.Printf("            Retention: %s - %s\n", cfg.Storage.String(), cfg.Retention.String())
	fmt.Printf("             Replicas: %d\n", cfg.Replicas)
	fmt.Printf("       Discard Policy: %s\n", cfg.Discard.String())
	fmt.Printf("     Duplicate Window: %v\n", cfg.Duplicates)
	fmt.Printf("    Allows Msg Delete: %v\n", !cfg.DenyDelete)
	fmt.Printf("         Allows Purge: %v\n", !cfg.DenyPurge)

	if cfg.MaxMsgs == -1 {
		fmt.Println("     Maximum Messages: unlimited")
	} else {
		fmt.Printf("     Maximum Messages: %s\n", humanize.Comma(cfg.MaxMsgs))
	}
	if cfg.MaxBytes == -1 {
		fmt.Println("        Maximum Bytes: unlimited")
	} else {
		fmt.Printf("        Maximum Bytes: %s\n", humanize.IBytes(uint64(cfg.MaxBytes)))
	}
	if cfg.MaxAge <= 0 {
		fmt.Println("          Maximum Age: unlimited")
	} else {
		fmt.Printf("          Maximum Age: %s\n", humanizeDuration(cfg.MaxAge))
	}
	if cfg.MaxMsgSize == -1 {
		fmt.Println(" Maximum Message Size: unlimited")
	} else {
		fmt.Printf(" Maximum Message Size: %s\n", humanize.IBytes(uint64(cfg.MaxMsgSize)))
	}
	if cfg.MaxConsumers == -1 {
		fmt.Println("    Maximum Consumers: unlimited")
	} else {
		fmt.Printf("    Maximum Consumers: %d\n", cfg.MaxConsumers)
	}
	if cfg.Template != "" {
		fmt.Printf("  Managed by Template: %s\n", cfg.Template)
	}
	if cfg.Placement != nil {
		if cfg.Placement.Cluster != "" {
			fmt.Printf("    Placement Cluster: %s\n", cfg.Placement.Cluster)
		}
		if len(cfg.Placement.Tags) > 0 {
			fmt.Printf("       Placement Tags: %s\n", strings.Join(cfg.Placement.Tags, ", "))
		}
	}
	if len(cfg.Sources) > 0 {
		fmt.Printf("              Sources: ")
		sort.Slice(cfg.Sources, func(i, j int) bool {
			return cfg.Sources[i].Name < cfg.Sources[j].Name
		})
	}

	fmt.Println()
}

func humanizeDuration(d time.Duration) string {
	if d == math.MaxInt64 {
		return "never"
	}

	tsecs := d / time.Second
	tmins := tsecs / 60
	thrs := tmins / 60
	tdays := thrs / 24
	tyrs := tdays / 365

	if tyrs > 0 {
		return fmt.Sprintf("%dy%dd%dh%dm%ds", tyrs, tdays%365, thrs%24, tmins%60, tsecs%60)
	}

	if tdays > 0 {
		return fmt.Sprintf("%dd%dh%dm%ds", tdays, thrs%24, tmins%60, tsecs%60)
	}

	if thrs > 0 {
		return fmt.Sprintf("%dh%dm%ds", thrs, tmins%60, tsecs%60)
	}

	if tmins > 0 {
		return fmt.Sprintf("%dm%ds", tmins, tsecs%60)
	}

	return fmt.Sprintf("%.2fs", d.Seconds())
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

	streamName := "my_stream"
	consumerName := "pull_consumer"
	consumerCfg := &nats.ConsumerConfig{
		Durable:       consumerName,
		DeliverPolicy: nats.DeliverAllPolicy,
		ReplayPolicy:  nats.ReplayInstantPolicy,
		AckPolicy:     nats.AckExplicitPolicy,
	}
	consumerInfo, err := js.AddConsumer(streamName, consumerCfg)
	if err != nil {
		return err
	}
	defer js.DeleteConsumer(streamName, consumerName)
	showConsumerInfo(consumerInfo)

	sub, err := nc.SubscribeSync(nc.NewRespInbox())
	if err != nil {
		return err
	}
	sub.AutoUnsubscribe(1)

	mgr, err := jsm.New(nc)
	if err != nil {
		return err
	}

	timeout := 5 * time.Second
	req := &api.JSApiConsumerGetNextRequest{Batch: 1, Expires: timeout}
	if err := mgr.NextMsgRequest(streamName, consumerName, sub.Subject, req); err != nil {
		return err
	}

	msg, err := sub.NextMsg(timeout)
	if err != nil {
		return err
	}

	fatalIfNotPull := func() {
		cons, err := mgr.LoadConsumer(streamName, consumerName)
		if err != nil {
			log.Fatalf("could not load consumer %q, err=%v", consumerName, err)
		}

		if !cons.IsPullMode() {
			log.Fatalf("consumer %q is not a Pull consumer", consumerName)
		}
	}

	if msg.Header != nil && msg.Header.Get("Status") == "503" {
		fatalIfNotPull()
	}

	metadata, err := jsm.ParseJSMsgMetadata(msg)
	if err != nil {
		if msg.Reply == "" {
			fmt.Printf("--- subject: %s\n", msg.Subject)
		} else {
			fmt.Printf("--- subject: %s reply: %s\n", msg.Subject, msg.Reply)
		}
		return err
	}
	fmt.Printf("[%s] subj: %s / tries: %d / cons seq: %d / str seq: %d / pending: %d\n", time.Now().Format("15:04:05"), msg.Subject, metadata.Delivered(), metadata.ConsumerSequence(), metadata.StreamSequence(), metadata.Pending())
	if len(msg.Header) > 0 {
		fmt.Println()
		fmt.Println("Headers:")
		fmt.Println()
		for h, vals := range msg.Header {
			for _, val := range vals {
				fmt.Printf("  %s: %s\n", h, val)
			}
		}

		fmt.Println()
		fmt.Println("Data:")
		fmt.Println()
	}

	fmt.Println()
	fmt.Println(string(msg.Data))

	if err := msg.Respond(nil); err != nil {
		return err
	}
	log.Printf("responded")
	if err := nc.Flush(); err != nil {
		return err
	}
	log.Printf("flushed response")

	return nil
}

func showConsumerInfo(state *nats.ConsumerInfo) {
	config := state.Config

	fmt.Printf("Information for Consumer %s > %s created %s\n", state.Stream, state.Name, state.Created.Local().Format(time.RFC3339))
	fmt.Println()
	fmt.Println("Configuration:")
	fmt.Println()
	if config.Durable != "" {
		fmt.Printf("        Durable Name: %s\n", config.Durable)
	}
	if config.Description != "" {
		fmt.Printf("         Description: %s\n", config.Description)
	}
	if config.DeliverSubject != "" {
		fmt.Printf("    Delivery Subject: %s\n", config.DeliverSubject)
	} else {
		fmt.Printf("           Pull Mode: true\n")
	}
	if config.FilterSubject != "" {
		fmt.Printf("      Filter Subject: %s\n", config.FilterSubject)
	}
	fmt.Printf("      Deliver Policy: %+v\n", config.DeliverPolicy)
	if config.DeliverGroup != "" && config.DeliverSubject != "" {
		fmt.Printf(" Deliver Queue Group: %s\n", config.DeliverGroup)
	}
	fmt.Printf("          Ack Policy: %s\n", config.AckPolicy.String())
	if config.MaxDeliver != -1 {
		fmt.Printf("  Maximum Deliveries: %d\n", config.MaxDeliver)
	}
	if config.SampleFrequency != "" {
		fmt.Printf("       Sampling Rate: %s\n", config.SampleFrequency)
	}
	if config.RateLimit > 0 {
		fmt.Printf("          Rate Limit: %s / second\n", humanize.IBytes(config.RateLimit/8))
	}
	if config.MaxAckPending > 0 {
		fmt.Printf("     Max Ack Pending: %s\n", humanize.Comma(int64(config.MaxAckPending)))
	}
	if config.MaxWaiting > 0 {
		fmt.Printf("   Max Waiting Pulls: %s\n", humanize.Comma(int64(config.MaxWaiting)))
	}
	if config.Heartbeat > 0 {
		fmt.Printf("      Idle Heartbeat: %s\n", humanizeDuration(config.Heartbeat))
	}
	if config.DeliverSubject != "" {
		fmt.Printf("        Flow Control: %v\n", config.FlowControl)
	}
	if config.HeadersOnly {
		fmt.Printf("        Headers Only: true\n")
	}
	if config.InactiveThreshold > 0 && config.DeliverSubject == "" {
		fmt.Printf("  Inactive Threshold: %s\n", humanizeDuration(config.InactiveThreshold))
	}
	if config.MaxRequestExpires > 0 {
		fmt.Printf("     Max Pull Expire: %s\n", humanizeDuration(config.MaxRequestExpires))
	}
	if config.MaxRequestBatch > 0 {
		fmt.Printf("      Max Pull Batch: %s\n", humanize.Comma(int64(config.MaxRequestBatch)))
	}
	if len(config.BackOff) > 0 {
		fmt.Printf("             Backoff: %+v\n", config.BackOff)
	}
	if config.Replicas > 0 {
		fmt.Printf("            Replicas: %d\n", config.Replicas)
	}
	if config.MemoryStorage {
		fmt.Printf("      Memory Storage: yes\n")
	}
	fmt.Println()

	if state.Cluster != nil && state.Cluster.Name != "" {
		fmt.Println("Cluster Information:")
		fmt.Println()
		fmt.Printf("                Name: %s\n", state.Cluster.Name)
		fmt.Printf("              Leader: %s\n", state.Cluster.Leader)
		for _, r := range state.Cluster.Replicas {
			since := fmt.Sprintf("seen %s ago", humanizeDuration(r.Active))
			if r.Active == 0 || r.Active == math.MaxInt64 {
				since = "not seen"
			}

			if r.Current {
				fmt.Printf("             Replica: %s, current, %s\n", r.Name, since)
			} else {
				fmt.Printf("             Replica: %s, outdated, %s\n", r.Name, since)
			}
		}
		fmt.Println()
	}

	fmt.Println("State:")
	fmt.Println()
	if state.Delivered.Last == nil {
		fmt.Printf("   Last Delivered Message: Consumer sequence: %s Stream sequence: %s\n", humanize.Comma(int64(state.Delivered.Consumer)), humanize.Comma(int64(state.Delivered.Stream)))
	} else {
		fmt.Printf("   Last Delivered Message: Consumer sequence: %s Stream sequence: %s Last delivery: %s ago\n", humanize.Comma(int64(state.Delivered.Consumer)), humanize.Comma(int64(state.Delivered.Stream)), humanizeDuration(time.Since(*state.Delivered.Last)))
	}

	fmt.Printf("     Unprocessed Messages: %s\n", humanize.Comma(int64(state.NumPending)))
	if config.DeliverSubject == "" {
		if config.MaxWaiting > 0 {
			fmt.Printf("            Waiting Pulls: %s of maximum %s\n", humanize.Comma(int64(state.NumWaiting)), humanize.Comma(int64(config.MaxWaiting)))
		} else {
			fmt.Printf("            Waiting Pulls: %s of unlimited\n", humanize.Comma(int64(state.NumWaiting)))
		}
	} else {
		if state.PushBound {
			if config.DeliverGroup != "" {
				fmt.Printf("          Active Interest: Active using Queue Group %s", config.DeliverGroup)
			} else {
				fmt.Printf("          Active Interest: Active")
			}
		} else {
			fmt.Printf("          Active Interest: No interest")
		}
	}

	fmt.Println()
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
